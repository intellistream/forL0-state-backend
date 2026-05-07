// hot_cache_phase_b_test.cpp — tests for Phase B encoders, HotCacheR32,
// composite-key folders, and HotCacheManager::rebalance_if_needed.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <unordered_set>

#include "hot_cache.h"

using namespace forl0;

// ---------------------------------------------------------------------------
//  Encoder helpers
// ---------------------------------------------------------------------------

TEST(HotCacheEncoders, Int32KeySignExtendsDistinctly) {
    // Distinct int32 keys must map to distinct int64s (so HotCacheLL cannot
    // false-hit on sign-extended collisions).
    std::unordered_set<int64_t> seen;
    for (int32_t k = -1000; k <= 1000; ++k) {
        auto e = hotcache_key_from_i32(k);
        ASSERT_TRUE(seen.insert(e).second);
    }
    // Min/max corners.
    ASSERT_EQ(hotcache_key_from_i32(std::numeric_limits<int32_t>::min()),
              static_cast<int64_t>(std::numeric_limits<int32_t>::min()));
    ASSERT_EQ(hotcache_key_from_i32(std::numeric_limits<int32_t>::max()),
              static_cast<int64_t>(std::numeric_limits<int32_t>::max()));
}

TEST(HotCacheEncoders, DoubleRoundtripPreservesBits) {
    const double cases[] = {
        0.0, -0.0, 1.0, -1.0, 3.14159, 2.718281828,
        std::numeric_limits<double>::min(),
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::denorm_min(),
    };
    for (double d : cases) {
        int64_t bits = hotcache_val_from_double(d);
        double back = hotcache_val_to_double(bits);
        // Compare bit-for-bit (NaN != NaN in IEEE but bits must match).
        int64_t db, bb;
        std::memcpy(&db, &d, sizeof(db));
        std::memcpy(&bb, &back, sizeof(bb));
        ASSERT_EQ(db, bb);
    }
}

TEST(HotCacheFolders, TwKeyFoldCollidesRarely) {
    std::unordered_set<int64_t> seen;
    const int N = 4096;
    int collisions = 0;
    for (int i = 0; i < N; ++i) {
        int64_t key = i * 17;
        int64_t start = i * 1000;
        int64_t end = start + 250;
        int64_t f = hotcache_fold_tw_key(key, start, end);
        if (!seen.insert(f).second) ++collisions;
    }
    // Expect near-zero collisions on a smooth-varying input.
    ASSERT_LT(collisions, 4);
}

TEST(HotCacheFolders, FixedRowFoldIsInputDependent) {
    int64_t a[] = {1, 2, 3};
    int64_t b[] = {1, 2, 4};
    int64_t c[] = {3, 2, 1}; // reordered → must differ
    auto fa = hotcache_fold_fixed_row_key(a, 3);
    auto fb = hotcache_fold_fixed_row_key(b, 3);
    auto fc = hotcache_fold_fixed_row_key(c, 3);
    ASSERT_NE(fa, fb);
    ASSERT_NE(fa, fc);
    ASSERT_NE(fb, fc);
    // Zero-arity rows collapse to the seed; that is expected and acceptable
    // because a real cache attach path always has at least one column.
    ASSERT_EQ(hotcache_fold_fixed_row_key(nullptr, 0),
              hotcache_fold_fixed_row_key(nullptr, 0));
}

// ---------------------------------------------------------------------------
//  HotCacheR32 — fixed-row value cache
// ---------------------------------------------------------------------------

TEST(HotSet32Layout, SizeAndAlignment) {
    ASSERT_EQ(sizeof(HotSet32), 384u);
    ASSERT_EQ(alignof(HotSet32), 64u);
}

TEST(HotCacheR32, RoundtripPreserves24ByteRow) {
    alignas(64) HotSet32 sets[4];
    HotCacheR32 c(sets, 4, /*val_width=*/24);

    uint8_t v1[32] = {0};
    for (int i = 0; i < 24; ++i) v1[i] = static_cast<uint8_t>(i + 1);
    c.put(42, v1);

    uint8_t out[32] = {0};
    ASSERT_TRUE(c.get(42, out));
    for (int i = 0; i < 24; ++i) ASSERT_EQ(out[i], v1[i]);
    // Bytes beyond the declared width are untouched / irrelevant.
}

TEST(HotCacheR32, MissThenPutThenHit) {
    alignas(64) HotSet32 sets[8];
    HotCacheR32 c(sets, 8, /*val_width=*/16);
    uint8_t out[32] = {0};
    ASSERT_FALSE(c.get(99, out));
    uint8_t v[16];
    std::memset(v, 0xAB, 16);
    c.put(99, v);
    ASSERT_TRUE(c.get(99, out));
    for (int i = 0; i < 16; ++i) ASSERT_EQ(out[i], 0xAB);
}

TEST(HotCacheR32, InvalidateMakesKeyMiss) {
    alignas(64) HotSet32 sets[2];
    HotCacheR32 c(sets, 2, /*val_width=*/8);
    uint8_t v[8] = {1,2,3,4,5,6,7,8};
    c.put(7, v);
    uint8_t out[8];
    ASSERT_TRUE(c.get(7, out));
    c.invalidate(7);
    ASSERT_FALSE(c.get(7, out));
}

// ---------------------------------------------------------------------------
//  HotCacheManager::rebalance_if_needed
// ---------------------------------------------------------------------------

namespace {
// Reusable fake loader that successfully allocates `N` HotSets worth of
// memory on the heap so we can drive HotCacheManager without real L0.
static uint8_t* g_backing = nullptr;
static size_t  g_backing_size = 0;

static int fake_tuner_init(void** t, size_t)  { *t = reinterpret_cast<void*>(0x1); return 0; }
static int fake_tuner_destroy(void*)          { return 0; }
static void* fake_mem_alloc(void*, size_t sz) {
    delete[] g_backing;
    g_backing = new uint8_t[sz];
    g_backing_size = sz;
    std::memset(g_backing, 0, sz);
    return g_backing;
}
static int fake_mem_free(void*, void*)        { delete[] g_backing; g_backing = nullptr; g_backing_size = 0; return 0; }

static bool fake_loader(L0LibBindings* out, std::string*) {
    out->lib_handle = reinterpret_cast<void*>(0x1);
    out->owns_lib_handle = false;
    out->cache_tuner_init    = reinterpret_cast<L0LibBindings::CacheTunerInitFn>(fake_tuner_init);
    out->cache_tuner_destroy = reinterpret_cast<L0LibBindings::CacheTunerDestroyFn>(fake_tuner_destroy);
    out->l0_mem_alloc        = reinterpret_cast<L0LibBindings::L0MemAllocFn>(fake_mem_alloc);
    out->l0_mem_free         = reinterpret_cast<L0LibBindings::L0MemFreeFn>(fake_mem_free);
    return true;
}
}  // namespace

TEST(HotCacheManagerRebalance, DoesNothingBelowInterval) {
    HotCacheManager mgr(/*capacity=*/64 * sizeof(HotSet), fake_loader);
    // The fake_loader bypasses the access("/dev/hisi_l0") probe path in the
    // default loader by providing its own bindings — so on any OS we get an
    // active manager here.
    ASSERT_TRUE(mgr.is_active());
    auto c = mgr.acquire_ll(8);
    ASSERT_NE(c.get(), nullptr);

    // A few lookups: must NOT trigger rebalance with default interval (1<<20).
    int64_t dummy;
    for (int i = 0; i < 100; ++i) c->get(i, &dummy);
    ASSERT_EQ(mgr.rebalance_if_needed(), 0u);
    mgr.release_ll(c.release());
}

TEST(HotCacheManagerRebalance, ClearsCacheWhenMissRateHigh) {
    HotCacheManager mgr(/*capacity=*/64 * sizeof(HotSet), fake_loader);
    ASSERT_TRUE(mgr.is_active());
    auto c = mgr.acquire_ll(8);

    // Generate 100 misses, 0 hits — miss_rate = 1.0.
    int64_t dummy;
    for (int i = 0; i < 100; ++i) c->get(i, &dummy);
    ASSERT_EQ(c->hits(), 0u);
    ASSERT_EQ(c->misses(), 100u);

    // Use a very small interval so the heuristic fires in this test.
    uint32_t n = mgr.rebalance_if_needed(/*interval_ops=*/50,
                                         /*miss_rate_threshold=*/0.5);
    ASSERT_EQ(n, 1u);
    // After clear, stats must be reset.
    ASSERT_EQ(c->hits(), 0u);
    ASSERT_EQ(c->misses(), 0u);
    mgr.release_ll(c.release());
}

TEST(HotCacheManagerAggregateStats, SumsPerCacheCounters) {
    HotCacheManager mgr(/*capacity=*/64 * sizeof(HotSet), fake_loader);
    ASSERT_TRUE(mgr.is_active());
    auto a = mgr.acquire_ll(8);
    auto b = mgr.acquire_ll(8);

    int64_t out;
    a->put(1, 111);
    a->get(1, &out);   // hit
    a->get(2, &out);   // miss
    b->put(10, 222);
    b->get(10, &out);  // hit
    b->get(11, &out);  // miss
    b->invalidate(10); // invalidation

    ASSERT_EQ(mgr.total_lookups(), 4u);
    ASSERT_EQ(mgr.total_hits(),    2u);
    ASSERT_EQ(mgr.total_invalidations(), 1u);
    mgr.release_ll(a.release());
    mgr.release_ll(b.release());
}
