// hot_cache_test.cpp — unit tests for HotSet layout and HotCacheLL operations.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif

#include <cstddef>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "hot_cache.h"

using namespace forl0;

// Helper: a 64B-aligned backing buffer for `n` HotSets.
template <uint32_t N>
struct AlignedBacking {
    alignas(64) HotSet sets[N];
    static_assert((N & (N - 1)) == 0, "N must be a power of two");
};

// ----------------------------------------------------------------------
//  Layout invariants
// ----------------------------------------------------------------------

TEST(HotSetLayout, SizeAndAlignment) {
    ASSERT_EQ(sizeof(HotSet), 192u);
    ASSERT_EQ(alignof(HotSet), 64u);
    ASSERT_EQ(offsetof(HotSet, tags), 0u);
    // Keys live on the 2nd cache line, vals on the 3rd.
    ASSERT_EQ(offsetof(HotSet, keys), 64u);
    ASSERT_EQ(offsetof(HotSet, vals), 128u);
}

TEST(HotSetLayout, InitEmptyMarksAllSlotsEmpty) {
    HotSet s{};
    s.init_empty();
    for (int i = 0; i < HOTCACHE_WAYS; ++i) {
        ASSERT_EQ(s.tags[i], HOTCACHE_EMPTY_TAG);
    }
    ASSERT_EQ(s.rr, 0);
}

// ----------------------------------------------------------------------
//  HotCacheLL: basic put/get
// ----------------------------------------------------------------------

TEST(HotCacheLL, PutThenGet) {
    AlignedBacking<8> backing{};
    HotCacheLL c(backing.sets, 8);

    int64_t out = -1;
    ASSERT_FALSE(c.get(42, &out));
    ASSERT_EQ(c.misses(), 1u);

    c.put(42, 12345);
    ASSERT_TRUE(c.get(42, &out));
    ASSERT_EQ(out, 12345);
    ASSERT_EQ(c.hits(), 1u);
}

TEST(HotCacheLL, MissOnUnknownKey) {
    AlignedBacking<8> backing{};
    HotCacheLL c(backing.sets, 8);

    int64_t out = 0;
    ASSERT_FALSE(c.get(7, &out));
    ASSERT_FALSE(c.get(8, &out));
    ASSERT_EQ(c.misses(), 2u);
}

TEST(HotCacheLL, UpdateInPlace) {
    AlignedBacking<8> backing{};
    HotCacheLL c(backing.sets, 8);

    c.put(99, 1);
    c.put(99, 2);
    c.put(99, 3);

    int64_t out = 0;
    ASSERT_TRUE(c.get(99, &out));
    ASSERT_EQ(out, 3);

    // Same key must occupy exactly one slot in its set.
    int occupied = 0;
    for (uint32_t i = 0; i < c.num_sets(); ++i) {
        for (int j = 0; j < HOTCACHE_WAYS; ++j) {
            if (c.sets()[i].tags[j] != HOTCACHE_EMPTY_TAG && c.sets()[i].keys[j] == 99) {
                ++occupied;
            }
        }
    }
    ASSERT_EQ(occupied, 1);
}

TEST(HotCacheLL, Invalidate) {
    AlignedBacking<8> backing{};
    HotCacheLL c(backing.sets, 8);

    c.put(5, 500);
    int64_t out = 0;
    ASSERT_TRUE(c.get(5, &out));
    ASSERT_EQ(out, 500);

    c.invalidate(5);
    ASSERT_FALSE(c.get(5, &out));
    ASSERT_EQ(c.invalidations(), 1u);

    // Invalidating a key that isn't present is a no-op.
    c.invalidate(123456);
    ASSERT_EQ(c.invalidations(), 1u);
}

TEST(HotCacheLL, Clear) {
    AlignedBacking<8> backing{};
    HotCacheLL c(backing.sets, 8);

    for (int i = 0; i < 50; ++i) c.put(i, i * 10);
    c.clear();
    int64_t out = 0;
    for (int i = 0; i < 50; ++i) {
        ASSERT_FALSE(c.get(i, &out));
    }
}

// ----------------------------------------------------------------------
//  Round-robin eviction (single-set cache → all keys collide)
// ----------------------------------------------------------------------

TEST(HotCacheLL, RoundRobinEviction) {
    AlignedBacking<1> backing{};
    HotCacheLL c(backing.sets, 1);

    // Insert 8 distinct keys → all 8 slots filled.
    // Keys are chosen so that h2 values differ enough to avoid in-place update.
    // (We rely on real distinct H2s from the mixer; collisions are tolerated as
    //  long as the keys themselves differ — both are checked.)
    for (int64_t k = 1; k <= 8; ++k) c.put(k, k * 100);
    int64_t out = 0;
    for (int64_t k = 1; k <= 8; ++k) {
        ASSERT_TRUE(c.get(k, &out));
        ASSERT_EQ(out, k * 100);
    }

    // The 9th insertion must evict an existing slot (round-robin starts at 0).
    c.put(9, 900);
    ASSERT_TRUE(c.get(9, &out));
    ASSERT_EQ(out, 900);

    // Exactly one of the original 8 keys must now miss.
    int still_present = 0;
    for (int64_t k = 1; k <= 8; ++k) {
        if (c.get(k, &out)) ++still_present;
    }
    ASSERT_EQ(still_present, 7);
}

// ----------------------------------------------------------------------
//  EMPTY tag (0x80) is distinguishable from real H2 (which is 7 bits, [0..0x7F])
// ----------------------------------------------------------------------

TEST(HotCacheLL, EmptyTagDoesNotMatchRealKey) {
    AlignedBacking<2> backing{};
    HotCacheLL c(backing.sets, 2);

    // Without inserting anything, no key may "match" — the EMPTY (0x80) byte
    // must be excluded from the H2 match space because real H2 = hash & 0x7F.
    int64_t out = 0;
    for (int64_t k = -100; k <= 100; ++k) {
        ASSERT_FALSE(c.get(k, &out));
    }
}

// ----------------------------------------------------------------------
//  Bulk random workload — no false hits, behaviour matches a reference map
// ----------------------------------------------------------------------

TEST(HotCacheLL, RandomFillNoFalseHits) {
    constexpr uint32_t kSets = 64;       // 64 sets * 8 ways = 512 capacity
    AlignedBacking<kSets> backing{};
    HotCacheLL c(backing.sets, kSets);

    std::mt19937_64 rng(0xc0ffee);
    std::unordered_map<int64_t, int64_t> reference;

    for (int op = 0; op < 4000; ++op) {
        int64_t k = static_cast<int64_t>(rng() & 0x3FFF);  // 16 384 distinct
        int64_t v = static_cast<int64_t>(rng());
        c.put(k, v);
        reference[k] = v;
    }

    // Every successful lookup must report the latest reference value;
    // NO key absent from the reference is ever allowed to hit.
    int hits = 0;
    int64_t out = 0;
    for (int64_t k = 0; k < 16384; ++k) {
        bool got = c.get(k, &out);
        auto it = reference.find(k);
        if (it == reference.end()) {
            ASSERT_FALSE(got);
        } else if (got) {
            ASSERT_EQ(out, it->second);
            ++hits;
        }
    }
    // Sanity: at least some keys must be cached (RR keeps the 8 most recent
    // per-set inserts). With 4 000 ops over 16 K keys and 512 capacity, the
    // observable hit count is comfortably > 100.
    ASSERT_GT(hits, 100);
}
