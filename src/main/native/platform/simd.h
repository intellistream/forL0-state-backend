// SIMD abstraction for SwissTable group probing.
// Aligned with abseil's GroupSse2Impl / GroupAArch64Impl / GroupPortableImpl.
// Each Group operates on kWidth (16) control bytes simultaneously.

#pragma once

#include <cstdint>
#include <cstring>

#if defined(FORL0_SSE2)
#include <emmintrin.h>  // SSE2
#endif

#if defined(FORL0_NEON)
#include <arm_neon.h>
#endif

namespace forl0 {

// Control byte constants (abseil-compatible)
enum Ctrl : int8_t {
    kEmpty    = -128,  // 0x80
    kDeleted  = -2,    // 0xFE
    kSentinel = -1,    // 0xFF — marks end of ctrl array
};

static_assert(kEmpty < kSentinel && kDeleted < kSentinel,
              "kEmpty and kDeleted must be less than kSentinel");

// A group width of 16 bytes — matches SSE2 register / NEON register width.
static constexpr size_t kGroupWidth = 16;

// ----------------------------------------------------------------------------
// BitMask — abseil-aligned template.
//
// Each backend chooses its own (storage_type, shift) combination:
//   - SSE2 / portable: uint32_t mask, Shift=0  (one bit per slot: bits 0..15)
//   - NEON           : uint64_t mask, Shift=2  (one bit per slot at bit slot*4,
//                      derived from the vshrn_n_u16 nibble-compression trick —
//                      this is the same idea abseil's GroupAArch64Impl uses)
//
// The invariant every backend maintains: within the storage, each logical slot
// is represented by exactly ONE set bit, located at bit index (slot << Shift).
// That lets `mask &= mask - 1` step to the next slot without any scalar loop,
// and `__builtin_ctz*(mask) >> Shift` recover the slot index in O(1).
// ----------------------------------------------------------------------------
template <typename T, int Shift>
struct BitMaskImpl {
    T mask;

    explicit BitMaskImpl(T m) : mask(m) {}

    explicit operator bool() const { return mask != 0; }

    // Returns the slot index of the lowest set bit.
    uint32_t lowest_bit_set() const {
        if constexpr (sizeof(T) <= 4) {
            return static_cast<uint32_t>(__builtin_ctz(static_cast<uint32_t>(mask))) >> Shift;
        } else {
            return static_cast<uint32_t>(__builtin_ctzll(static_cast<uint64_t>(mask))) >> Shift;
        }
    }

    // Removes the lowest set bit and returns its slot index.
    uint32_t next() {
        uint32_t idx = lowest_bit_set();
        mask &= mask - 1;
        return idx;
    }

    bool has_next() const { return mask != 0; }
};

#if defined(FORL0_NEON)
using BitMask = BitMaskImpl<uint64_t, 2>;
#else
using BitMask = BitMaskImpl<uint32_t, 0>;
#endif

// ============================================================================
//  SSE2 Group implementation (x86-64)
// ============================================================================
#if defined(FORL0_SSE2)

struct Group {
    __m128i ctrl;

    explicit Group(const int8_t* pos) {
        ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
    }

    // Match slots whose ctrl byte equals h2.
    BitMask match(int8_t h2) const {
        auto match_vec = _mm_set1_epi8(h2);
        return BitMask(
            static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(ctrl, match_vec))));
    }

    // Match empty slots (ctrl == kEmpty).
    BitMask match_empty() const {
        return match(kEmpty);
    }

    // Match empty or deleted slots.
    BitMask match_empty_or_deleted() const {
        // kEmpty = 0x80 and kDeleted = 0xFE are both < 0 in signed compare.
        // kSentinel = 0xFF is also < 0 but we want to skip it.
        // abseil uses: special bit pattern test. Simplification:
        // A slot is empty-or-deleted if its ctrl byte has the high bit set
        // AND it's not kSentinel. However, for probing we can just check < kSentinel.
        // Actually abseil simply checks the top bit: ctrl < kSentinel means
        // empty or deleted in practice. We use _mm_cmpgt to find bytes > kSentinel..
        // NO — abseil's approach: empty/deleted both have top bit set.
        // match_empty_or_deleted = mask of bytes where top bit is set.
        auto special = _mm_set1_epi8(static_cast<int8_t>(kSentinel));
        return BitMask(
            static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(special, ctrl))));
    }

    // Count the number of leading empty or deleted entries (for growth budget).
    uint32_t count_leading_empty_or_deleted() const {
        auto special = _mm_set1_epi8(static_cast<int8_t>(kSentinel));
        uint32_t m =
            static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(special, ctrl)));
        // invert: we want leading (from LSB); ctz of ~m gives trailing zeros of inverted = leading matches
        return (m == 0xFFFF) ? kGroupWidth : __builtin_ctz(~m);
    }
};

// ============================================================================
//  NEON Group implementation (AArch64 / Kunpeng)
//
//  Design aligned with abseil's GroupAArch64Impl:
//   - vceqq_u8 / vcltq_s8 produces a vector where each matched byte is 0xFF
//     and each non-match byte is 0x00.
//   - vshrn_n_u16(cmp, 4) packs the 128-bit byte-mask into 64 bits, with each
//     slot taking 4 bits: slot i's nibble is located at bit position i*4, and
//     the nibble is 0xF on match / 0x0 on miss.
//   - Masking the result with 0x1111...11 leaves exactly one set bit per
//     matched slot, at bit (i << 2). The generic BitMaskImpl<uint64_t, Shift=2>
//     then iterates with `ctzll >> 2` and `mask &= mask - 1` — no scalar loop.
//
//  This is the critical fix for AArch64 (Kunpeng) performance: the previous
//  implementation recompressed the nibble-mask into a 16-bit bitmask via a
//  16-iteration scalar loop on every match(), completely erasing the SIMD
//  advantage in SwissTable probes.
// ============================================================================
#elif defined(FORL0_NEON)

struct Group {
    uint8x16_t ctrl;

    explicit Group(const int8_t* pos) {
        ctrl = vld1q_u8(reinterpret_cast<const uint8_t*>(pos));
    }

    // Compress a per-byte compare result (each byte 0x00 or 0xFF) into the
    // sparse 64-bit "one bit per slot, at bit slot*4" form consumed by
    // BitMaskImpl<uint64_t, 2>.
    static inline uint64_t nibble_mask_sparse(uint8x16_t cmp) {
        uint8x8_t narrowed = vshrn_n_u16(vreinterpretq_u16_u8(cmp), 4);
        uint64_t bits = vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
        return bits & 0x1111111111111111ULL;
    }

    // Dense nibble mask: each slot occupies 4 bits, full 0xF on match / 0x0 on miss.
    // Used by count_leading_empty_or_deleted where we need to find the first
    // non-matching slot in positional order.
    static inline uint64_t nibble_mask_dense(uint8x16_t cmp) {
        uint8x8_t narrowed = vshrn_n_u16(vreinterpretq_u16_u8(cmp), 4);
        return vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
    }

    BitMask match(int8_t h2) const {
        uint8x16_t dup = vdupq_n_u8(static_cast<uint8_t>(h2));
        uint8x16_t cmp = vceqq_u8(ctrl, dup);
        return BitMask(nibble_mask_sparse(cmp));
    }

    BitMask match_empty() const {
        return match(kEmpty);
    }

    BitMask match_empty_or_deleted() const {
        // Empty (0x80) and Deleted (0xFE) are strictly less than kSentinel (0xFF)
        // in signed byte comparison; FULL (0x00..0x7F) is non-negative, i.e. > -1.
        int8x16_t sentinel = vdupq_n_s8(kSentinel);
        uint8x16_t cmp = vcltq_s8(vreinterpretq_s8_u8(ctrl), sentinel);
        return BitMask(nibble_mask_sparse(cmp));
    }

    uint32_t count_leading_empty_or_deleted() const {
        int8x16_t sentinel = vdupq_n_s8(kSentinel);
        uint8x16_t cmp = vcltq_s8(vreinterpretq_s8_u8(ctrl), sentinel);
        uint64_t dense = nibble_mask_dense(cmp);
        // Leading empty/deleted count = number of leading 0xF nibbles in `dense`.
        // Equivalent: index (in nibbles) of the first 0x0 nibble. If all 16 slots
        // are empty/deleted, dense is all-ones.
        if (dense == 0xFFFFFFFFFFFFFFFFULL) return kGroupWidth;
        return static_cast<uint32_t>(__builtin_ctzll(~dense)) >> 2;
    }
};

// ============================================================================
//  Portable SWAR Group implementation (fallback)
// ============================================================================
#else

struct Group {
    uint64_t lo;  // first 8 ctrl bytes
    uint64_t hi;  // next 8 ctrl bytes

    explicit Group(const int8_t* pos) {
        std::memcpy(&lo, pos, 8);
        std::memcpy(&hi, pos + 8, 8);
    }

    BitMask match(int8_t h2) const {
        // Broadcast h2 to all 8 bytes of a uint64
        uint64_t pattern = 0x0101010101010101ULL * static_cast<uint8_t>(h2);
        return BitMask(swar_match_word(lo, pattern) | (swar_match_word(hi, pattern) << 8));
    }

    BitMask match_empty() const {
        return match(kEmpty);
    }

    BitMask match_empty_or_deleted() const {
        // Top bit set AND not 0xFF (kSentinel).
        // For portable: check top bit set, then exclude sentinel.
        constexpr uint64_t msbs = 0x8080808080808080ULL;
        uint64_t lo_special = lo & msbs;
        uint64_t hi_special = hi & msbs;
        // Sentinel (0xFF) also has top bit set; exclude by checking if byte == 0xFF.
        // sentinel_pattern = 0xFFFFFFFFFFFFFFFF all ones
        // byte == 0xFF means (byte ^ 0xFF) == 0
        constexpr uint64_t all_ff = 0xFFFFFFFFFFFFFFFF;
        uint64_t lo_not_sentinel = (lo ^ all_ff);  // zero where sentinel
        uint64_t hi_not_sentinel = (hi ^ all_ff);
        // A byte is empty-or-deleted if top bit set AND not sentinel.
        // "not sentinel" = at least one bit differs from 0xFF.
        // Approximate: if top bit is set, check that byte != 0xFF.
        // For each byte: if byte == 0xFF then (byte ^ 0xFF) == 0 → the byte in lo_not_sentinel is 0.
        // We need each byte in lo_not_sentinel to be nonzero. Use: (x - 0x01) & ~x & 0x80 to detect zero bytes.
        // Invert: byte is nonzero if NOT detected as zero.
        // This is getting complex. Simpler approach for portable:
        uint32_t mask = 0;
        const auto* p = reinterpret_cast<const uint8_t*>(&lo);
        const auto* q = reinterpret_cast<const uint8_t*>(&hi);
        for (int i = 0; i < 8; ++i) {
            if (p[i] == static_cast<uint8_t>(kEmpty) || p[i] == static_cast<uint8_t>(kDeleted))
                mask |= (1u << i);
            if (q[i] == static_cast<uint8_t>(kEmpty) || q[i] == static_cast<uint8_t>(kDeleted))
                mask |= (1u << (i + 8));
        }
        return BitMask(mask);
    }

    uint32_t count_leading_empty_or_deleted() const {
        auto m = match_empty_or_deleted();
        return (m.mask == 0xFFFF) ? kGroupWidth : __builtin_ctz(~m.mask);
    }

private:
    // SWAR: returns a bitmask (one bit per byte) of matching bytes in 'word'.
    static uint32_t swar_match_word(uint64_t word, uint64_t pattern) {
        constexpr uint64_t lsb = 0x0101010101010101ULL;
        constexpr uint64_t msb = 0x8080808080808080ULL;
        uint64_t x = word ^ pattern;
        // Zero bytes in x → set high bit in result
        uint64_t result = (x - lsb) & ~x & msb;
        // Extract high bit of each byte into a bit position
        // Bit 7 of byte 0 → bit 0, bit 15 of byte 1 → bit 1, etc.
        uint32_t mask = 0;
        mask |= ((result >>  7) & 1) << 0;
        mask |= ((result >> 15) & 1) << 1;
        mask |= ((result >> 23) & 1) << 2;
        mask |= ((result >> 31) & 1) << 3;
        mask |= ((result >> 39) & 1) << 4;
        mask |= ((result >> 47) & 1) << 5;
        mask |= ((result >> 55) & 1) << 6;
        mask |= ((result >> 63) & 1) << 7;
        return mask;
    }
};

#endif  // SIMD backend selection

// ============================================================================
//  Probe sequence — triangular probing (abseil compatible)
//  probe(i) = (H1 + i*(i+1)/2) mod num_groups
// ============================================================================
struct ProbeSeq {
    size_t offset;   // current group index * kGroupWidth
    size_t stride;   // increments by 1 each step (triangular)
    size_t mask;     // capacity - 1 (capacity is power of 2)

    ProbeSeq(size_t hash, size_t mask) : offset(hash & mask), stride(0), mask(mask) {}

    size_t pos() const { return offset; }

    void next() {
        stride += kGroupWidth;
        offset = (offset + stride) & mask;
    }

    // For safety: the sequence visits all groups when capacity is power of 2.
    size_t index() const { return stride / kGroupWidth; }
};

// ============================================================================
//  Hash splitting (abseil compatible)
// ============================================================================

// H1: used for probe sequence starting position
inline size_t H1(size_t hash) { return hash >> 7; }

// H2: stored in ctrl byte (low 7 bits, guaranteed 0x00..0x7F)
inline int8_t H2(size_t hash) { return static_cast<int8_t>(hash & 0x7F); }

// ============================================================================
//  Utility: set ctrl byte and its clone
// ============================================================================
inline void set_ctrl(int8_t* ctrl, size_t capacity, size_t i, int8_t h) {
    ctrl[i] = h;
    // Mirror first (kGroupWidth - 1) bytes at the end for wrap-around probing.
    // The ctrl array has capacity + kGroupWidth bytes total.
    // Bytes [capacity .. capacity + kGroupWidth - 1] mirror bytes [0 .. kGroupWidth - 2] + sentinel.
    constexpr size_t kClone = kGroupWidth - 1;
    if (i < kClone) {
        ctrl[capacity + i] = h;
    }
}

// Initialize ctrl array: all kEmpty, with sentinel at ctrl[capacity].
inline void init_ctrl(int8_t* ctrl, size_t capacity) {
    std::memset(ctrl, kEmpty, capacity + kGroupWidth);
    ctrl[capacity] = kSentinel;
}

}  // namespace forl0
