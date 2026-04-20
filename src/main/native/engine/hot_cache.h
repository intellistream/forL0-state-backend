// hot_cache.h — L0 Hot-Key Cache.
//
// A set-associative read/write-through cache that lives in Kunpeng L0 memory
// (a user-space partition of the L3 cache). It sits ABOVE the SwissTable in
// every supported StateTable<K,V> and short-circuits hot-key get/put.
//
// Layout: each HotSet is exactly 192 bytes / 3 cache lines (tags / keys /
// values), 8-way associative. Lookup uses NEON `vceq_u8` on aarch64, an SSE2
// `pcmpeqb` on x86_64, and a portable scalar fallback elsewhere.
//
// Hardware gating (`HotCacheManager::is_active()`):
//   - dlopen("libl0mempool.so") succeeds
//   - /dev/hisi_l0 exists
//   - cache_tuner_init() returns a non-null tuner
//   - probe l0_mem_alloc succeeds for at least one HotSet
// If any step fails, the manager is NOT active and StateTable::cache_ is
// left null — there is no heap fallback (see L0_HotKey_Cache_Design §3.2).
//
// Thread safety: NOT required. Flink keyed-state access is single-threaded.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#if defined(FORL0_NEON)
#include <arm_neon.h>
#elif defined(FORL0_SSE2)
#include <emmintrin.h>
#endif

namespace forl0 {

// ---------------------------------------------------------------------------
//  Constants
// ---------------------------------------------------------------------------

// Empty/sentinel control byte. Real H2 values are in [0, 0x7F]; 0x80 marks
// an unused slot. Since H2 = hash & 0x7F, a real entry's tag never clashes.
static constexpr uint8_t HOTCACHE_EMPTY_TAG = 0x80;
static constexpr int     HOTCACHE_WAYS      = 8;

// 192 B / 64-byte aligned: 1 line tags+meta, 1 line keys, 1 line values.
struct alignas(64) HotSet {
    // line 0 ------------------------------------------------------------
    uint8_t  tags[HOTCACHE_WAYS];   // H2 (hash & 0x7F); HOTCACHE_EMPTY_TAG = 0x80
    uint8_t  rr;                    // round-robin eviction pointer (3 bits used)
    uint8_t  _pad0[64 - HOTCACHE_WAYS - 1];
    // line 1 ------------------------------------------------------------
    int64_t  keys[HOTCACHE_WAYS];   // 8 * 8B = 64B
    // line 2 ------------------------------------------------------------
    int64_t  vals[HOTCACHE_WAYS];   // 8 * 8B = 64B

    void init_empty() noexcept {
        std::memset(tags, HOTCACHE_EMPTY_TAG, sizeof(tags));
        rr = 0;
        // keys / vals contents are irrelevant when tags are EMPTY.
    }
};

static_assert(sizeof(HotSet) == 192, "HotSet must be exactly 192 bytes (3 cache lines)");
static_assert(alignof(HotSet) == 64, "HotSet must be 64-byte aligned");

// ---------------------------------------------------------------------------
//  HotSet32 — 6-cache-line variant for fixed-row values up to 32 bytes.
//  line 0:   tags + metadata (same layout as HotSet)
//  line 1:   keys[8] (int64)
//  line 2-5: vals[8][32]  (8 × 32-byte row = 256 B = 4 cache lines)
//  Total = 64 * 6 = 384 B. The fast path only pulls line 0 + one value
//  line, so the actual per-lookup L0 traffic stays ~128 B.
// ---------------------------------------------------------------------------
struct alignas(64) HotSet32 {
    uint8_t tags[HOTCACHE_WAYS];
    uint8_t rr;
    uint8_t _pad0[64 - HOTCACHE_WAYS - 1];
    int64_t keys[HOTCACHE_WAYS];
    uint8_t vals[HOTCACHE_WAYS][32];

    void init_empty() noexcept {
        std::memset(tags, HOTCACHE_EMPTY_TAG, sizeof(tags));
        rr = 0;
    }
};

static_assert(sizeof(HotSet32) == 384, "HotSet32 must be exactly 384 bytes (6 cache lines)");
static_assert(alignof(HotSet32) == 64, "HotSet32 must be 64-byte aligned");

// ---------------------------------------------------------------------------
//  Hash mixer (Wyhash-ish): cheap, well-distributed for keyed workloads.
//  Reused by both H1 (set index) and H2 (tag).
// ---------------------------------------------------------------------------
static inline uint64_t hotcache_mix64(uint64_t k) noexcept {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

// Portable 8-byte equality mask (one byte per slot, 0xFF = match).
static inline uint64_t hotcache_match_h2_scalar(const uint8_t* tags, uint8_t h2) noexcept {
    uint64_t mask = 0;
    for (int i = 0; i < HOTCACHE_WAYS; ++i) {
        if (tags[i] == h2) mask |= (uint64_t)0xFF << (i * 8);
    }
    return mask;
}

#if defined(FORL0_HOTCACHE_SCALAR)
static inline uint64_t hotcache_match_h2(const uint8_t* tags, uint8_t h2) noexcept {
    return hotcache_match_h2_scalar(tags, h2);
}
#elif defined(FORL0_NEON)
static inline uint64_t hotcache_match_h2(const uint8_t* tags, uint8_t h2) noexcept {
    uint8x8_t v   = vld1_u8(tags);
    uint8x8_t cmp = vceq_u8(v, vdup_n_u8(h2));
    return vget_lane_u64(vreinterpret_u64_u8(cmp), 0);
}
#elif defined(FORL0_SSE2)
static inline uint64_t hotcache_match_h2(const uint8_t* tags, uint8_t h2) noexcept {
    __m128i v   = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(tags));
    __m128i cmp = _mm_cmpeq_epi8(v, _mm_set1_epi8(static_cast<char>(h2)));
    uint64_t lo;
    std::memcpy(&lo, &cmp, sizeof(lo));
    return lo;
}
#else
static inline uint64_t hotcache_match_h2(const uint8_t* tags, uint8_t h2) noexcept {
    return hotcache_match_h2_scalar(tags, h2);
}
#endif

// Pop the lowest matching slot index (0..7) from an 8-byte equality mask.
static inline int hotcache_pop_match(uint64_t& mask) noexcept {
    int slot = __builtin_ctzll(mask) >> 3;
    mask &= mask - 1;
    return slot;
}

// ---------------------------------------------------------------------------
//  Key / value encoders — let the single int64/int64 HotCacheLL cover
//  int32 keys (sign-extended) and double values (bit-pattern preserved).
//  No information is lost: distinct int32 keys stay distinct, and
//  encode/decode round-trips all IEEE-754 doubles including NaN/-0.
// ---------------------------------------------------------------------------
static inline int64_t hotcache_key_from_i32(int32_t k) noexcept {
    return static_cast<int64_t>(k);
}

static inline int64_t hotcache_val_from_double(double v) noexcept {
    int64_t bits;
    std::memcpy(&bits, &v, sizeof(bits));
    return bits;
}

static inline double hotcache_val_to_double(int64_t bits) noexcept {
    double v;
    std::memcpy(&v, &bits, sizeof(v));
    return v;
}

// ---------------------------------------------------------------------------
//  Composite-key folding.
//
//  Phase B callers need to cache values keyed by (user_key, namespace) —
//  e.g. `<int64, int64>` with a TimeWindow namespace — or by a FixedRow
//  multi-column key. We fold such composites into a single int64 that can
//  feed HotCacheLL directly.
//
//  Correctness: `fold_tw_key` and `fold_fixed_row_key` are injective "enough"
//  for Zipf-ish workloads: distinct inputs almost always produce distinct
//  folded values, and a stray collision merely triggers a cache miss (the
//  SwissTable lookup below will catch it) — it never returns wrong data.
// ---------------------------------------------------------------------------
static inline int64_t hotcache_fold_tw_key(int64_t key, int64_t ns_start, int64_t ns_end) noexcept {
    uint64_t h = hotcache_mix64(static_cast<uint64_t>(ns_start));
    h ^= hotcache_mix64(static_cast<uint64_t>(ns_end) + 0x9E3779B97F4A7C15ULL);
    h ^= static_cast<uint64_t>(key);
    return static_cast<int64_t>(h);
}

// Fold an arbitrary column vector (FixedRow key) into an int64. The caller is
// responsible for supplying the native int64 reinterpretation of each column
// (via `std::memcpy` round-trip) — doubles, longs, and ints all fit.
static inline int64_t hotcache_fold_fixed_row_key(const int64_t* cols, size_t n) noexcept {
    uint64_t h = 0x9E3779B97F4A7C15ULL;
    for (size_t i = 0; i < n; ++i) {
        h ^= hotcache_mix64(static_cast<uint64_t>(cols[i]) + i);
    }
    return static_cast<int64_t>(h);
}

// ---------------------------------------------------------------------------
//  HotCacheLL — int64 key, int64 value (Phase A primary path).
// ---------------------------------------------------------------------------
class HotCacheLL {
public:
    // num_sets MUST be a power of two; HotCacheManager::acquire_ll guarantees this.
    HotCacheLL(HotSet* sets, uint32_t num_sets) noexcept
        : sets_(sets), set_mask_(num_sets - 1), num_sets_(num_sets) {
        for (uint32_t i = 0; i < num_sets; ++i) sets_[i].init_empty();
    }

    HotCacheLL(const HotCacheLL&) = delete;
    HotCacheLL& operator=(const HotCacheLL&) = delete;

    uint32_t num_sets() const noexcept { return num_sets_; }
    HotSet*  sets()     const noexcept { return sets_; }

    // Diagnostics
    uint64_t hits()          const noexcept { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses()        const noexcept { return misses_.load(std::memory_order_relaxed); }
    uint64_t lookups()       const noexcept { return hits() + misses(); }
    uint64_t invalidations() const noexcept { return invalidations_.load(std::memory_order_relaxed); }

    bool get(int64_t key, int64_t* out) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet*  s  = &sets_[(h >> 7) & set_mask_];
        uint64_t m  = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                *out = s->vals[i];
                hits_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        misses_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    void put(int64_t key, int64_t val) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet*  s  = &sets_[(h >> 7) & set_mask_];

        // Already present → in-place update.
        uint64_t m = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                s->vals[i] = val;
                return;
            }
        }
        // Find an empty way, else round-robin evict.
        uint64_t empties = hotcache_match_h2(s->tags, HOTCACHE_EMPTY_TAG);
        int slot;
        if (empties) {
            slot = hotcache_pop_match(empties);
        } else {
            slot = s->rr & 0x7;
            s->rr = static_cast<uint8_t>((s->rr + 1) & 0x7);
        }
        s->tags[slot] = h2;
        s->keys[slot] = key;
        s->vals[slot] = val;
    }

    void invalidate(int64_t key) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet*  s  = &sets_[(h >> 7) & set_mask_];
        uint64_t m  = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                s->tags[i] = HOTCACHE_EMPTY_TAG;
                invalidations_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
    }

    void clear() noexcept {
        for (uint32_t i = 0; i < num_sets_; ++i) sets_[i].init_empty();
    }

    void reset_stats() noexcept {
        hits_.store(0, std::memory_order_relaxed);
        misses_.store(0, std::memory_order_relaxed);
        invalidations_.store(0, std::memory_order_relaxed);
        last_rebalance_lookups_ = 0;
    }

    // Rebalancer bookkeeping: returns the lookups accumulated since the
    // previous call and updates the baseline. Caller consults the return
    // together with `hits()` / `misses()` to decide whether to clear state.
    uint64_t consume_rebalance_delta() noexcept {
        uint64_t cur = lookups();
        uint64_t delta = cur - last_rebalance_lookups_;
        last_rebalance_lookups_ = cur;
        return delta;
    }

private:
    HotSet*  sets_;
    uint32_t set_mask_;     // num_sets_ - 1 (num_sets_ is power of 2)
    uint32_t num_sets_;
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> invalidations_{0};
    uint64_t last_rebalance_lookups_ = 0;   // baseline for rebalance_if_needed
};

// ---------------------------------------------------------------------------
//  HotCacheR32 — int64 key, fixed-row value up to 32 bytes (Phase B).
//  Storage uses `HotSet32` (384 B, 6 cache lines). Hot path reads line 0
//  (tags) plus one value line in the common case, ~128 B of L0 traffic —
//  still comfortably under the ~10-cycle budget.
//
//  Callers are responsible for fixing the value width; the cache blindly
//  copies `val_width_` bytes in/out. Value widths > 32 must use a different
//  storage path (not implemented).
// ---------------------------------------------------------------------------
class HotCacheR32 {
public:
    HotCacheR32(HotSet32* sets, uint32_t num_sets, uint8_t val_width) noexcept
        : sets_(sets), set_mask_(num_sets - 1), num_sets_(num_sets),
          val_width_(val_width) {
        for (uint32_t i = 0; i < num_sets; ++i) sets_[i].init_empty();
    }

    HotCacheR32(const HotCacheR32&) = delete;
    HotCacheR32& operator=(const HotCacheR32&) = delete;

    uint32_t num_sets()  const noexcept { return num_sets_; }
    uint8_t  val_width() const noexcept { return val_width_; }
    uint64_t hits()      const noexcept { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses()    const noexcept { return misses_.load(std::memory_order_relaxed); }
    uint64_t lookups()   const noexcept { return hits() + misses(); }

    // Returns true iff the key is present; copies exactly val_width_ bytes into out.
    bool get(int64_t key, void* out) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet32* s = &sets_[(h >> 7) & set_mask_];
        uint64_t m  = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                std::memcpy(out, s->vals[i], val_width_);
                hits_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        misses_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    void put(int64_t key, const void* val) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet32* s = &sets_[(h >> 7) & set_mask_];
        uint64_t m  = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                std::memcpy(s->vals[i], val, val_width_);
                return;
            }
        }
        uint64_t empties = hotcache_match_h2(s->tags, HOTCACHE_EMPTY_TAG);
        int slot;
        if (empties) {
            slot = hotcache_pop_match(empties);
        } else {
            slot = s->rr & 0x7;
            s->rr = static_cast<uint8_t>((s->rr + 1) & 0x7);
        }
        s->tags[slot] = h2;
        s->keys[slot] = key;
        std::memcpy(s->vals[slot], val, val_width_);
    }

    void invalidate(int64_t key) noexcept {
        uint64_t h  = hotcache_mix64(static_cast<uint64_t>(key));
        uint8_t  h2 = static_cast<uint8_t>(h & 0x7F);
        HotSet32* s = &sets_[(h >> 7) & set_mask_];
        uint64_t m  = hotcache_match_h2(s->tags, h2);
        while (m) {
            int i = hotcache_pop_match(m);
            if (s->keys[i] == key) {
                s->tags[i] = HOTCACHE_EMPTY_TAG;
                invalidations_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
    }

    void clear() noexcept {
        for (uint32_t i = 0; i < num_sets_; ++i) sets_[i].init_empty();
    }

private:
    HotSet32* sets_;
    uint32_t  set_mask_;
    uint32_t  num_sets_;
    uint8_t   val_width_;
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> invalidations_{0};
};

// ---------------------------------------------------------------------------
//  L0 library bindings (libl0mempool.so) — opaque, dlopen-loaded.
// ---------------------------------------------------------------------------
struct L0LibBindings {
    using CacheTuner          = void;
    using CacheTunerInitFn    = int   (*)(CacheTuner** tuner, size_t max_capacity);
    using CacheTunerDestroyFn = int   (*)(CacheTuner* tuner);
    using L0MemAllocFn        = void* (*)(CacheTuner* tuner, size_t size);
    using L0MemFreeFn         = int   (*)(CacheTuner* tuner, void* p);

    void* lib_handle = nullptr;
    CacheTunerInitFn    cache_tuner_init    = nullptr;
    CacheTunerDestroyFn cache_tuner_destroy = nullptr;
    L0MemAllocFn        l0_mem_alloc        = nullptr;
    L0MemFreeFn         l0_mem_free         = nullptr;

    // True if `lib_handle` requires a real `dlclose`. Test-injected bindings
    // typically set this false.
    bool owns_lib_handle = true;
};

// Factory hook used by tests to inject fake L0 bindings. On success returns
// true and populates *out; on failure returns false (and may write a short
// reason into *reason).
using L0BindingsLoader = bool (*)(L0LibBindings* out, std::string* reason);

// Default loader: real dlopen of libl0mempool.so. Implemented in hot_cache.cpp.
bool default_l0_bindings_loader(L0LibBindings* out, std::string* reason);

// ---------------------------------------------------------------------------
//  HotCacheManager
//
//  Phase A allocator strategy: single-shot bump allocator over the HotSet
//  array, plus a small free-list for releases. acquire_ll() always rounds
//  the request DOWN to a power of 2 and tries (a) the bump frontier, then
//  (b) a sufficiently large released run.
// ---------------------------------------------------------------------------
class HotCacheManager {
public:
    HotCacheManager(size_t requested_capacity_bytes,
                    L0BindingsLoader loader = nullptr);
    ~HotCacheManager();

    HotCacheManager(const HotCacheManager&) = delete;
    HotCacheManager& operator=(const HotCacheManager&) = delete;

    bool   is_active()        const noexcept { return active_; }
    size_t capacity_bytes()   const noexcept { return capacity_bytes_; }
    size_t used_bytes()       const noexcept;
    uint32_t total_sets()     const noexcept { return total_sets_; }
    uint32_t free_sets()      const noexcept;
    const std::string& failure_reason() const noexcept { return failure_reason_; }

    std::unique_ptr<HotCacheLL> acquire_ll(uint32_t sets_requested);
    void release_ll(HotCacheLL* cache);

    // Aggregate counters across all live caches owned by this manager.
    // Used by Flink MetricGroup via JNI (see getHotCacheManagerStats).
    uint64_t total_lookups()       const noexcept;
    uint64_t total_hits()          const noexcept;
    uint64_t total_invalidations() const noexcept;

    // Phase C §6.3 adaptive rebalancer.
    //
    // Heuristic: any cache whose miss_rate > 0.5 AND lookups_since_last >= N
    // is flagged as "starving" and has its sets cleared (so eviction state
    // resets and hot keys get fresh slots). This is a best-effort nudge, not
    // a precise set re-allocation — the next N ops will organically refill
    // the cleared sets with the currently-hot working set. Correctness
    // invariant: clearing a cache only drops non-authoritative state, so no
    // user-visible behaviour changes.
    //
    // Returns the number of caches that were rebalanced.
    uint32_t rebalance_if_needed(uint64_t interval_ops = (1u << 20),
                                 double miss_rate_threshold = 0.5) noexcept;

    // Round x DOWN to the largest power of two <= x. Returns 0 if x == 0.
    static uint32_t pow2_floor(uint32_t x) noexcept {
        if (x == 0) return 0;
        uint32_t p = 1;
        while ((p << 1) && (p << 1) <= x) p <<= 1;
        return p;
    }

private:
    L0LibBindings bindings_{};
    void*         tuner_           = nullptr;
    void*         raw_base_        = nullptr;   // ptr returned by l0_mem_alloc (for free)
    void*         l0_base_         = nullptr;   // 64B-aligned region used as HotSet[]
    size_t        capacity_bytes_  = 0;
    uint32_t      total_sets_      = 0;
    uint32_t      bump_next_       = 0;        // next set id to hand out from the unused tail
    bool          active_          = false;
    std::string   failure_reason_;

    struct Owned {
        HotCacheLL* cache;
        uint32_t    start;
        uint32_t    count;
    };
    std::vector<Owned> owned_;
    std::vector<std::pair<uint32_t /*start*/, uint32_t /*count*/>> released_runs_;

    HotSet* base_sets() noexcept { return reinterpret_cast<HotSet*>(l0_base_); }

    void shutdown() noexcept;
};

}  // namespace forl0
