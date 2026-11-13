#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "serialized_hot_cache.h"
#include <random>
#include <unordered_map>

using namespace forl0;
static uint64_t collide(std::string_view) { return 7; }

TEST(SerializedHotCache, ExactIdentityDespiteHashCollisionsAndBinaryKeys) {
    alignas(64) HotSet memory[16];
    HotCacheLL backing(memory, 16);
    SerializedHotCache cache(&backing, collide);
    std::string a("ns\0key1", 7), b("ns\0key2", 7), out;
    cache.put(a, "first"); cache.put(b, "second");
    ASSERT_TRUE(cache.get(a, &out)); ASSERT_EQ(out, "first");
    ASSERT_TRUE(cache.get(b, &out)); ASSERT_EQ(out, "second");
    cache.invalidate(a); ASSERT_FALSE(cache.get(a, &out));
    ASSERT_TRUE(cache.get(b, &out)); ASSERT_EQ(out, "second");
    // Rebalancing scalar tags would corrupt these payloads.
    ASSERT_FALSE(backing.rebalance_if_needed(0, -1));
    ASSERT_TRUE(cache.get(b, &out)); ASSERT_EQ(out, "second");
}

TEST(SerializedHotCache, VariableRowsWrapEvictionAndOversizedUpdates) {
    alignas(64) HotSet memory[16];
    HotCacheLL backing(memory, 16);
    SerializedHotCache cache(&backing);
    std::mt19937 rng(42);
    std::unordered_map<std::string, std::string> oracle;
    std::string out;
    for (int i = 0; i < 10000; ++i) {
        auto key = std::to_string(rng() % 30);
        auto value = std::string(rng() % 1500, char('a' + i % 26));
        cache.put(key, value); oracle[key] = value;
        for (const auto& entry : oracle) {
            if (cache.get(entry.first, &out)) ASSERT_EQ(out, entry.second);
        }
    }
    cache.put("x", "old");
    cache.put("x", std::string(sizeof(memory) + 1, 'z'));
    ASSERT_FALSE(cache.get("x", &out));
    ASSERT_TRUE(backing.bypass_events() > 0);
}

TEST(SerializedHotCache, PrefixInvalidationDoesNotCrossNamespaces) {
    alignas(64) HotSet memory[16];
    HotCacheLL backing(memory, 16);
    SerializedHotCache cache(&backing);
    cache.put("window1/key/a", "a", 8); cache.put("window1/key/b", "b", 8);
    cache.put("window2/key/a", "other", 8);
    cache.invalidate_prefix("window1/");
    std::string out;
    ASSERT_FALSE(cache.get("window1/key/a", &out));
    ASSERT_FALSE(cache.get("window1/key/b", &out));
    ASSERT_TRUE(cache.get("window2/key/a", &out)); ASSERT_EQ(out, "other");
    cache.clear(); ASSERT_FALSE(cache.get("window2/key/a", &out));
}

TEST(SerializedHotCache, MixedPrefixMutationsSurviveRingWrapAndDirectoryCollisions) {
    alignas(64) HotSet memory[4];
    HotCacheLL backing(memory, 4);
    SerializedHotCache cache(&backing, collide);
    std::mt19937 rng(123);
    std::unordered_map<std::string, std::string> oracle;
    std::string out;
    for (int i = 0; i < 15000; ++i) {
        std::string prefix = std::to_string(rng() % 5) + "/";
        std::string key = prefix + std::to_string(rng() % 20);
        switch (rng() % 10) {
            case 0:
                cache.clear(); oracle.clear(); break;
            case 1:
                cache.invalidate_prefix(prefix);
                for (auto it = oracle.begin(); it != oracle.end();) {
                    if (it->first.compare(0, prefix.size(), prefix) == 0) it = oracle.erase(it);
                    else ++it;
                }
                break;
            case 2:
                cache.invalidate(key); oracle.erase(key); break;
            default: {
                std::string value(rng() % 900, char(rng() % 256));
                cache.put(key, value, prefix.size()); oracle[key] = value;
            }
        }
        for (int group = 0; group < 5; ++group) {
            for (int user = 0; user < 20; ++user) {
                std::string probe = std::to_string(group) + "/" + std::to_string(user);
                if (cache.get(probe, &out)) {
                    ASSERT_TRUE(oracle.find(probe) != oracle.end());
                    ASSERT_EQ(out, oracle.at(probe));
                }
            }
        }
    }
}
