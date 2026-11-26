// TypedInnerMap tests — verifies MapState type specializations.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "state_engine.h"
#include "type_layout.h"
#include "checkpoint_reader.h"
#include "checkpoint_writer.h"
#include "window_map_storage.h"
#include <map>

using namespace forl0;

TEST(TypedInnerMapTest, RowKeysStayBesideTheirCountAndOwnLongBytesAcrossTierCopies) {
    alignas(64) std::byte first[32768], second[32768];
    std::pmr::monotonic_buffer_resource one(first, sizeof(first)), two(second, sizeof(second));
    InnerMapRowFour map{InnerMapRowFour::allocator_type(&one)};
    auto input = [](size_t n, unsigned char id) {
        NamespaceBlob bytes(n, 'q');
        bytes[n - 1] = static_cast<char>(id);
        return bytes;
    };
    const auto expected = NamespaceBlob("\0\1\2\3", 4);
    for (const size_t n : {36u, 44u, 45u, 375u}) {
        auto key = input(n, 0);
        auto probe = CompactMapKey::probe(key);
        map.try_emplace(probe).first->second = expected;
        key.back() = 'X';
        ASSERT_TRUE(map.find_value(key) == nullptr);
        ASSERT_EQ(map.find_value(input(n, 0))->size(), 4u);
    }
    auto inside = [](const void* p, const void* begin, size_t bytes) {
        auto at = reinterpret_cast<uintptr_t>(p), lo = reinterpret_cast<uintptr_t>(begin);
        return at >= lo && at < lo + bytes;
    };
    for (const auto& entry : map) {
        ASSERT_EQ(inside(entry.first.data(), &entry.first, sizeof(entry.first)), entry.first.size() <= 44);
        ASSERT_TRUE(inside(entry.first.data(), first, sizeof(first)));
    }
    for (unsigned char i = 1; i <= 36; ++i) map.try_emplace(input(100, i)).first->second = expected;
    ASSERT_FALSE(map.supports_compact_lookup());
    InnerMapRowFour moved(map, InnerMapRowFour::allocator_type(&two));
    for (const auto& entry : moved) {
        ASSERT_TRUE(inside(entry.first.data(), second, sizeof(second)));
        ASSERT_EQ(std::string_view(entry.second), std::string_view(expected));
        const auto* other = map.find_value(entry.first);
        ASSERT_TRUE(other != nullptr && other != &entry.second);
    }
    for (unsigned char i = 1; i <= 36; ++i) ASSERT_EQ(moved.erase(input(100, i)), 1u);
    ASSERT_EQ(map.size(), 40u);
}

TEST(TypedInnerMapTest, WideSchemaKeepsSmallHeadersAndCopiesEverySerializedKey) {
    using Entry = std::pair<WideMapKey, FourByteMapValue>;
    static_assert(sizeof(Entry) == 32 && sizeof(InnerMapWideRowFour) < sizeof(InnerMapRowFour));
    std::pmr::unsynchronized_pool_resource source, destination;
    InnerMapWideRowFour map{InnerMapWideRowFour::allocator_type(&source)};
    std::vector<NamespaceBlob> keys;
    for (size_t len : {0u, 20u, 21u, 44u, 80u, 600u}) keys.emplace_back(len, 'a');
    for (size_t i = 0; i < keys.size(); ++i) {
        map.try_emplace(WideMapKey::probe(keys[i])).first->second = NamespaceBlob("abcd");
    }
    for (int i = 0; i < 72; ++i) map.try_emplace(NamespaceBlob(100, char(i))).first->second = NamespaceBlob("wxyz");
    InnerMapWideRowFour copy(map, InnerMapWideRowFour::allocator_type(&destination));
    for (auto& key : keys) {
        const auto* v = copy.find_value(key);
        ASSERT_TRUE(v != nullptr);
        ASSERT_EQ(std::string_view(*v), std::string_view("abcd"));
        ASSERT_EQ(map.erase(key), 1u);
        ASSERT_TRUE(copy.find_value(key) != nullptr);
    }
}

TEST(TypedInnerMapTest, CompactMapStorageFollowsTheNamespaceAllocatorAtEverySize) {
    std::pmr::unsynchronized_pool_resource first, second;
    using Map = InnerMapTimeWindow;
    static_assert(sizeof(Map) < sizeof(std::pmr::unordered_map<TimeWindow, TimeWindow, TimeWindowHash>)
                                + 2 * sizeof(void*));
    for (size_t cardinality : {size_t(0), size_t(1), size_t(3), size_t(13), size_t(16), size_t(35)}) {
        Map source{Map::allocator_type(&first)};
        source.reserve(cardinality);
        for (size_t i = 0; i < cardinality; ++i) source[{int64_t(i), -3}] = {int64_t(9 * i), -7};
        Map same(std::move(source), Map::allocator_type(&first));
        ASSERT_TRUE(source.empty());
        Map copy(same, Map::allocator_type(&second));
        ASSERT_EQ(copy.size(), cardinality);
        ASSERT_TRUE(copy.get_allocator().resource() == &second);
        for (size_t i = cardinality; i < 40; ++i) copy[{int64_t(i), -3}] = {int64_t(9 * i), -7};
        ASSERT_EQ(copy.size(), 40u);
        ASSERT_EQ(same.size(), cardinality);
        for (int64_t i = 0; i < 40; ++i) {
            ASSERT_TRUE(copy.at({i, -3}) == TimeWindow(9 * i, -7));
            ASSERT_EQ(copy.erase({i, -3}), 1u);
        }
        ASSERT_TRUE(copy.empty());
    }
}

TEST(TypedInnerMapTest, GrowingInnerMapsEarnAdmissionWhenTheirExistingEntriesAreReused) {
    constexpr size_t quota = 512 * 1024;
    auto manager = std::make_shared<HotCacheManager>(2 * quota, [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 2, true);
    for (int64_t outer = 0; outer < 8; ++outer) state.put(0, outer, InnerMapLongLong{{-1, outer}});
    state.enable_namespace_storage(manager, quota);
    for (int64_t i = 0; i < 2048; ++i)
        state.update_or_create(0, i % 8, [&](auto& m) { m[i] = i; });
    auto growth = state.namespace_cache_stats();
    ASSERT_EQ(growth.growing_map_updates, 2048u);
    ASSERT_EQ(growth.promotions, 0u);
    ASSERT_EQ(growth.growth_demotions, 0u);
    state.prepare_snapshot();
    for (int64_t i = 0; i < 2048; ++i)
        ASSERT_TRUE(state.modify_in_place(0, i % 8, [&](auto& m) { m.at(-1) = i; }));
    ASSERT_GT(state.namespace_cache_stats().promotions, 0u);
    ASSERT_EQ(state.namespace_cache_stats().growing_map_updates, growth.growing_map_updates);
    size_t frozen = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const InnerMapLongLong& values) {
        ASSERT_EQ(values.size(), 257u);
        ASSERT_EQ(values.at(-1), key);
        ASSERT_EQ(values.at(key + 8), key + 8);
        ++frozen;
    });
    ASSERT_EQ(frozen, 8u);
    state.release_snapshot();
    ASSERT_EQ(state.get(0, 7)->at(-1), 2047);
    ASSERT_EQ(state.get(1, 7), nullptr);
}

TEST(TypedInnerMapTest, PackedPointerMaskAcceptsEveryLiveKeyAfterCopiesAndCollidingErasures) {
    using Map = InnerMapLongLong;
    static_assert(sizeof(Map) == sizeof(Map::allocator_type) + 8 + sizeof(std::pair<int64_t, int64_t>));
    auto mask = [](const Map& map) {
        uint64_t bits;
        std::memcpy(&bits, static_cast<const char*>(map.storage_address()) + sizeof(void*), sizeof(bits));
        return bits;
    };
    Map values;
    for (const int64_t key : {1, 65, 129, 2, 3, 4, 5, 6, 7}) values[key] = -key;
    ASSERT_EQ(mask(values), UINT64_C(0xfe));
    ASSERT_TRUE(values.find_value(193) == nullptr);
    values.erase(1);
    ASSERT_EQ(values.at(65), -65);
    Map copied(values);
    copied.reserve(16);
    Map moved(std::move(copied));
    ASSERT_EQ(mask(moved), mask(values));
    for (int key = 2; key <= 6; ++key) moved.erase(key);
    for (int key = 61; key <= 63; ++key) moved[key] = key;
    ASSERT_TRUE((mask(moved) & (UINT64_C(1) << 60)) == 0);
    ASSERT_TRUE((mask(moved) & (UINT64_C(1) << 2)) == 0);
    for (const auto& [key, value] : moved) {
        ASSERT_NE(moved.find_value(key), nullptr);
        ASSERT_EQ(moved.at(key), value);
    }
    ASSERT_EQ(values.at(2), -2);
}

TEST(TypedInnerMapTest, WholeTableMovesPreservePackedMapControlsAndOwnEveryPayload) {
    constexpr size_t quota = 512 * 1024;
    auto manager = std::make_shared<HotCacheManager>(2 * quota, [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    auto row = [](int i) { std::string x(96, 'r'); x[30] = static_cast<char>(i); return x; };
    InnerMapLongString ordinary;
    for (int i = 0; i < 47; ++i) ordinary.try_emplace(128 * i + 3, row(i));
    ASSERT_FALSE(ordinary.supports_compact_lookup());
    InnerMapLongString cached(ordinary, InnerMapLongString::allocator_type(&arena));
    ASSERT_TRUE(cached.supports_compact_lookup());
    NamespaceDramAllocator ordinary_table(&DefaultAllocator::instance());
    InnerMapLongString returned(cached, InnerMapLongString::allocator_type(&ordinary_table));
    ASSERT_TRUE(returned.supports_compact_lookup());
    for (int i = 0; i < 47; ++i) {
        const auto* hot = cached.find_value(128 * i + 3);
        const auto* cold = returned.find_value(128 * i + 3);
        ASSERT_NE(hot, nullptr); ASSERT_NE(cold, nullptr);
        ASSERT_TRUE(arena.contains(hot->data())); ASSERT_FALSE(arena.contains(cold->data()));
        ASSERT_EQ(std::string_view(*cold), row(i));
    }
    ASSERT_EQ(returned.erase(131), 1u);
    ASSERT_NE(cached.find_value(131), nullptr);
    for (int i = 47; i < 80; ++i) returned.try_emplace(128 * i + 3, row(i));
    ASSERT_FALSE(returned.supports_compact_lookup());
    for (int i = 0; i < 80; ++i) {
        const auto* value = returned.find_value(128 * i + 3);
        if (i == 1) ASSERT_EQ(value, nullptr);
        else { ASSERT_NE(value, nullptr); ASSERT_EQ(std::string_view(*value), row(i)); }
    }
    const size_t free = arena.free_bytes();
    InnerMapLongString large;
    for (int i = 0; i < 8; ++i) large.try_emplace(i, std::string(quota / 4, 'x'));
    ASSERT_THROW(InnerMapLongString(large, InnerMapLongString::allocator_type(&arena)), NamespaceCacheFull);
    ASSERT_EQ(arena.free_bytes(), free);
    ASSERT_EQ(large.size(), 8u);
}

TEST(TypedInnerMapTest, SampledByteTagsVerifyEqualLengthCollisionsAcrossAllMapSizes) {
    InnerMapInline map;
    auto key = [](unsigned id) {
        NamespaceBlob bytes(132, 'x');
        bytes[0] = bytes[1] = bytes[2] = 0;
        bytes[3] = char(128);
        bytes[33] = static_cast<char>(id);
        return bytes;
    };
    auto lookup = [&](const NamespaceBlob& probe) {
        ASSERT_TRUE(map.supports_compact_lookup());
        auto tag = [&] { return packed_byte_key_tag(std::string_view(probe).substr(4), probe.size()); };
        auto* result = map.find_compact_value(tag, [&](const auto& k) { return k == probe; });
        ASSERT_EQ(result, map.find_value(probe));
    };
    for (unsigned i = 0; i < 16; ++i) {
        map[key(i)] = NamespaceBlob(1, static_cast<char>(i));
        lookup(key(i));
        lookup(key(30));
    }
    map.erase(key(3));
    lookup(key(3));
    lookup(key(15));
    map[key(20)] = NamespaceBlob("a");
    map[key(21)] = NamespaceBlob("b");
    ASSERT_FALSE(map.supports_compact_lookup());
    for (unsigned i = 0; i < 16; ++i) ASSERT_EQ(map.count(key(i)), i == 3 ? 0u : 1u);
}

TEST(TypedInnerMapTest, ShortMapValuesStayInlineWhenAnEntireNamespaceMoves) {
    constexpr size_t budget = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(budget * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapInline> state(0, 1, false);
    state.enable_namespace_storage(manager, budget);
    for (int i = 0; i < 16; ++i) {
        const NamespaceBlob user(41, static_cast<char>(i));
        state.update_or_create(0, int64_t{7}, int64_t{3}, [&](auto& map) {
            map[user] = NamespaceBlob(12, static_cast<char>(i + 1));
        });
    }
    const auto* arena = state.namespace_arena();
    const auto* map = state.get(0, int64_t{7}, int64_t{3});
    ASSERT_TRUE(arena->contains(map));
    for (const auto& [k, v] : *map) {
        ASSERT_TRUE(arena->contains(k.data()));
        ASSERT_TRUE(arena->contains(v.data()));
        ASSERT_EQ(v.size(), 12u);
        ASSERT_EQ(v.data()[11], k[0] + 1);
    }
    ASSERT_TRUE(sizeof(InlineMapValue) < sizeof(NamespaceBlob));
    ASSERT_TRUE((sizeof(std::pair<NamespaceBlob, InlineMapValue>) < sizeof(std::pair<NamespaceBlob, NamespaceBlob>)));
}

TEST(TypedInnerMapTest, ResidentMapMovesNestedAllocationsAndSpillsOneMutation) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMap> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const NamespaceBlob user(100, 'k'), original(100, 'v');
    InnerMap map;
    map.emplace(user, original);
    state.put(0, 1, map);
    state.put(0, 1, map);
    const auto* resident = state.get(0, 1);
    ASSERT_TRUE(arena->contains(resident));
    ASSERT_TRUE(arena->contains(&resident->begin()->second));
    ASSERT_TRUE(arena->contains(resident->begin()->first.data()));
    ASSERT_TRUE(arena->contains(resident->begin()->second.data()));

    state.prepare_snapshot();
    int calls = 0;
    const NamespaceBlob large(quota * 2, 'x');
    ASSERT_TRUE(state.modify_in_place(0, 1, [&](InnerMap& items) {
        ++calls;
        items[user].append(large);
        items.emplace(NamespaceBlob(200, 'n'), large);
    }));
    ASSERT_EQ(calls, 1);
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 1u);
    ASSERT_EQ(arena->free_bytes(), arena->capacity());
    const auto* moved = state.get(0, 1);
    ASSERT_FALSE(arena->contains(moved));
    ASSERT_EQ(moved->size(), 2u);
    ASSERT_EQ(moved->at(user).size(), original.size() + large.size());
    state.for_each_snapshot_in_key_group(0, [&](int64_t, const InnerMap& saved) {
        ASSERT_EQ(saved.size(), 1u);
        ASSERT_EQ(saved.at(user), original);
    });
    state.release_snapshot();
}

TEST(TypedInnerMapTest, PackedEntriesStayInTheTableTierAcrossSnapshotAndGrowth) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongString> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    InnerMapLongString map;
    for (int64_t i = 0; i < 4; ++i) map.try_emplace(i, 80, char('a' + i));
    state.put(0, 1, map);
    const auto* stored = state.put(0, 1, map);
    ASSERT_TRUE(arena->contains(stored));
    auto it = stored->begin();
    const auto first = reinterpret_cast<uintptr_t>(&it->second);
    ASSERT_TRUE(arena->contains(it->second.data()));
    ++it;
    const auto second = reinterpret_cast<uintptr_t>(&it->second);
    const size_t stride = sizeof(std::pair<int64_t, NamespaceRow>);
    ASSERT_EQ(second - first, stride);
    ASSERT_TRUE(arena->contains(it->second.data()));

    state.prepare_snapshot();
    ASSERT_TRUE(state.modify_in_place(0, 1, [](auto& entries) {
        entries.try_emplace(4, 80, 'e');
        ASSERT_EQ(entries.erase(0), 1u);
    }));
    const auto* current = state.get(0, 1);
    ASSERT_EQ(current->size(), 4u);
    ASSERT_EQ(current->count(0), 0u);
    ASSERT_TRUE(arena->contains(current->at(4).data()));
    state.for_each_snapshot_in_key_group(0, [&](int64_t, const auto& saved) {
        ASSERT_EQ(saved.size(), 4u);
        ASSERT_EQ(saved.at(0).front(), 'a');
        ASSERT_EQ(saved.count(4), 0u);
    });
    state.release_snapshot();
}

TEST(TypedInnerMapTest, TimeWindowEncodingMatchesLegacyStringMap) {
    TypeLayout layout(TypeId::MAP);
    InnerMapTimeWindow windows;
    windows.emplace(TimeWindow(-10, 0), TimeWindow(-20, 30));
    WriteBuffer typed;
    write_flink_value(typed, windows, layout);
    WriteBuffer key, value;
    key.write_long(-10); key.write_long(0);
    value.write_long(-20); value.write_long(30);
    InnerMap legacy;
    legacy.emplace(NamespaceBlob(reinterpret_cast<const char*>(key.data()), key.size()),
                   NamespaceBlob(reinterpret_cast<const char*>(value.data()), value.size()));
    WriteBuffer expected;
    write_flink_value(expected, legacy, layout);
    ASSERT_EQ(typed.size(), expected.size());
    ASSERT_EQ(std::memcmp(typed.data(), expected.data(), typed.size()), 0);
    ReadBuffer input(expected.data(), expected.size());
    auto restored = read_flink_value<InnerMapTimeWindow>(input, layout);
    ASSERT_EQ(restored.size(), 1u);
    ASSERT_TRUE(restored.at(TimeWindow(-10, 0)) == TimeWindow(-20, 30));
    ASSERT_EQ(input.remaining(), 0u);
}

TEST(TypedInnerMapTest, TimeWindowSnapshotKeepsValuesBeforeMutation) {
    StateTable<int64_t, InnerMapTimeWindow> table(0, 1, true);
    InnerMapTimeWindow initial;
    initial.emplace(TimeWindow(0, 10), TimeWindow(0, 20));
    table.put(0, 7, std::move(initial));
    table.prepare_snapshot();
    table.modify_in_place(0, int64_t(7), [](InnerMapTimeWindow& map) { map[TimeWindow(0, 10)] = TimeWindow(0, 99); });
    TypeLayout keys(TypeId::INT64), values(TypeId::MAP);
    CheckpointStateWriter<int64_t, InnerMapTimeWindow> writer(keys, values, true);
    WriteBuffer bytes;
    ASSERT_EQ(writer.write_key_group(table, 0, bytes), 1u);
    StateTable<int64_t, InnerMapTimeWindow> restored(0, 1, true);
    CheckpointStateReader<int64_t, InnerMapTimeWindow> reader(keys, values, true);
    ReadBuffer input(bytes.data(), bytes.size());
    reader.read_entries(input, restored, 0, 1);
    ASSERT_TRUE(restored.get(0, 7)->at(TimeWindow(0, 10)) == TimeWindow(0, 20));
    ASSERT_TRUE(table.get(0, 7)->at(TimeWindow(0, 10)) == TimeWindow(0, 99));
    table.release_snapshot();
}

TEST(TypedInnerMapTest, TimeWindowReplacementRemovesEmptyParent) {
    StateTable<int64_t, InnerMapTimeWindow> table(0, 1, true);
    put_window_entry(&table, 0, 7, TimeWindow(0, 10), TimeWindow(0, 20));
    table.prepare_snapshot();
    remove_window_entry(&table, 0, 7, TimeWindow(0, 10));
    ASSERT_EQ(table.total_size(), 0u);
    put_window_entry(&table, 0, 7, TimeWindow(0, 30), TimeWindow(0, 40));
    ASSERT_EQ(table.total_size(), 1u);
    TypeLayout keys(TypeId::INT64), values(TypeId::MAP);
    CheckpointStateWriter<int64_t, InnerMapTimeWindow> writer(keys, values, true);
    WriteBuffer bytes;
    ASSERT_EQ(writer.write_key_group(table, 0, bytes), 1u);
    StateTable<int64_t, InnerMapTimeWindow> restored(0, 1, true);
    CheckpointStateReader<int64_t, InnerMapTimeWindow> reader(keys, values, true);
    ReadBuffer input(bytes.data(), bytes.size());
    reader.read_entries(input, restored, 0, 1);
    ASSERT_TRUE(restored.get(0, 7)->at(TimeWindow(0, 10)) == TimeWindow(0, 20));
    table.release_snapshot();
    for (int64_t i = 0; i < 100; ++i)
        put_window_entry(&table, 0, 8, TimeWindow(i, i + 1), TimeWindow(i, i + 2));
    for (int64_t i = 0; i < 100; ++i)
        remove_window_entry(&table, 0, 8, TimeWindow(i, i + 1));
}

// ============================================================================
//  InnerMapLongLong
// ============================================================================

TEST(TypedInnerMapTest, LongLongBasicOps) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    map[1] = 100;
    map[2] = 200;
    map[3] = 300;
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 3u);
    ASSERT_EQ((*m)[1], 100);
    ASSERT_EQ((*m)[2], 200);
    ASSERT_EQ((*m)[3], 300);
}

TEST(TypedInnerMapTest, LongLongUpdate) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    map[1] = 100;
    st.put(0, 10, std::move(map));

    // Update inner map entry
    auto* m = st.get(0, static_cast<int64_t>(10));
    (*m)[1] = 999;
    (*m)[4] = 400;  // add new entry

    auto* m2 = st.get(0, static_cast<int64_t>(10));
    ASSERT_EQ((*m2)[1], 999);
    ASSERT_EQ((*m2)[4], 400);
}

TEST(TypedInnerMapTest, LongLongDelete) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map{{1, 100}, {2, 200}};
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    m->erase(1);

    ASSERT_EQ(m->size(), 1u);
    ASSERT_EQ(m->count(1), 0u);
    ASSERT_EQ((*m)[2], 200);
}

// ============================================================================
//  InnerMapLongString
// ============================================================================

TEST(TypedInnerMapTest, LongStringBasicOps) {
    StateTable<int64_t, InnerMapLongString> st(0, 1, true);

    InnerMapLongString map;
    map[1] = "hello";
    map[2] = "world";
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 2u);
    ASSERT_EQ((*m)[1], "hello");
    ASSERT_EQ((*m)[2], "world");
}

TEST(TypedInnerMapTest, LongStringEmptyValue) {
    StateTable<int64_t, InnerMapLongString> st(0, 1, true);

    InnerMapLongString map;
    map[1] = "";  // empty string value
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_EQ((*m)[1], "");
}

// ============================================================================
//  InnerMapStringLong
// ============================================================================

TEST(TypedInnerMapTest, StringLongBasicOps) {
    StateTable<int64_t, InnerMapStringLong> st(0, 1, true);

    InnerMapStringLong map;
    map["key1"] = 100;
    map["key2"] = 200;
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 2u);
    ASSERT_EQ((*m)["key1"], 100);
    ASSERT_EQ((*m)["key2"], 200);
}

// ============================================================================
//  InnerMap (generic string → string)
// ============================================================================

TEST(TypedInnerMapTest, GenericStringString) {
    StateTable<int64_t, InnerMap> st(0, 1, true);

    InnerMap map;
    map["uk1"] = "uv1";
    map["uk2"] = "uv2";
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ((*m)["uk1"], "uv1");
    ASSERT_EQ((*m)["uk2"], "uv2");
}

// ============================================================================
//  Different MapState Types in Same StateEngine
// ============================================================================

TEST(TypedInnerMapTest, MultipleMapTypesInEngine) {
    StateEngine engine(0, 1, 4);

    int64_t id_ll = engine.register_state<int64_t, InnerMapLongLong>("ll_map", true);
    int64_t id_ls = engine.register_state<int64_t, InnerMapLongString>("ls_map", true);
    int64_t id_sl = engine.register_state<int64_t, InnerMapStringLong>("sl_map", true);
    int64_t id_ss = engine.register_state<int64_t, InnerMap>("ss_map", true);

    auto* t_ll = engine.get_state_table<int64_t, InnerMapLongLong>(id_ll);
    auto* t_ls = engine.get_state_table<int64_t, InnerMapLongString>(id_ls);
    auto* t_sl = engine.get_state_table<int64_t, InnerMapStringLong>(id_sl);
    auto* t_ss = engine.get_state_table<int64_t, InnerMap>(id_ss);

    ASSERT_NE(t_ll, nullptr);
    ASSERT_NE(t_ls, nullptr);
    ASSERT_NE(t_sl, nullptr);
    ASSERT_NE(t_ss, nullptr);

    // Each operates independently
    InnerMapLongLong ll_data{{1, 100}};
    InnerMapLongString ls_data{{1, "hello"}};
    InnerMapStringLong sl_data{{"k", 200}};
    InnerMap ss_data{{"uk", "uv"}};

    t_ll->put(0, 1, std::move(ll_data));
    t_ls->put(0, 1, std::move(ls_data));
    t_sl->put(0, 1, std::move(sl_data));
    t_ss->put(0, 1, std::move(ss_data));

    ASSERT_EQ((*t_ll->get(0, 1))[1], 100);
    ASSERT_EQ((*t_ls->get(0, 1))[1], "hello");
    ASSERT_EQ((*t_sl->get(0, 1))["k"], 200);
    ASSERT_EQ((*t_ss->get(0, 1))["uk"], "uv");
}

// ============================================================================
//  COW Snapshot with TypedInnerMap
// ============================================================================

TEST(TypedInnerMapTest, InterleavedLargeRootEditsReuseOnlyTheRightGroupsLiveSlot) {
    constexpr size_t quota = 16 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 4, true);
    state.enable_namespace_storage(manager, quota);
    ASSERT_NE(state.resident_control_views(), nullptr);
    for (int group = 0; group < 4; ++group)
        for (int key = 0; key < 512; ++key)
            state.update_or_create(group, key, [&](auto& m) { m[3] = group + key; });
    for (int rep = 0; rep < 300; ++rep)
        for (int group = 0; group < 4; ++group)
            state.update_or_create(group, 77, [&](auto& m) { m[4] = 1000 * group + rep; });
    ASSERT_GT(state.namespace_cache_stats().repeated_root_writes, 1100u);
    // These keys share the 32-bit position tag.
    const int64_t collision = (int64_t{1} << 32) | 76;
    state.prepare_snapshot();
    state.update_or_create(0, collision, [](auto& m) { m[9] = 19; });
    ASSERT_TRUE(state.remove(0, 77));
    state.update_or_create(0, 77, [](auto& m) { m[4] = -5; });
    ASSERT_EQ(state.get(0, collision)->at(9), 19);
    ASSERT_EQ(state.get(0, 77)->at(4), -5);
    ASSERT_EQ(state.get(1, 77)->at(4), 1299);
    bool frozen = false;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const auto& m) {
        ASSERT_NE(key, collision);
        if (key == 77) frozen = m.at(4) == 299;
    });
    ASSERT_TRUE(frozen);
    state.release_snapshot();
}

TEST(TypedInnerMapTest, InterleavedRootEditHintsRevalidateSlotsAfterEraseAndGrowth) {
    constexpr size_t quota = 32 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 2, true);
    state.enable_namespace_storage(manager, quota);
    ASSERT_NE(state.resident_control_views(), nullptr);
    for (int group = 0; group < 2; ++group)
        for (int key = 0; key < 600; ++key)
            state.update_or_create(group, key, [&](auto& m) { m[1] = key; });

    for (int round = 0; round < 160; ++round) {
        for (int64_t key : {17, 29, 31, 47}) {
            state.update_or_create(0, key, [&](auto& m) { m[1] = round + key; });
            state.update_or_create(1, 17, [&](auto& m) { m[1] = -round; });
        }
    }
    const auto warm = state.namespace_cache_stats();
    ASSERT_GT(warm.root_edit_hint_bytes, 0u);
    ASSERT_GT(warm.root_edit_hint_hits, 500u);

    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, 29));
    const int64_t collision = (int64_t{1} << 32) | 28;
    state.update_or_create(0, collision, [](auto& m) { m[1] = 700; });
    for (int64_t key = 600; key < 1400; ++key)
        state.update_or_create(0, key, [&](auto& m) { m[1] = key; });
    state.update_or_create(0, 31, [](auto& m) { m[1] = -31; });
    ASSERT_EQ(state.get(0, 29), nullptr);
    ASSERT_EQ(state.get(0, collision)->at(1), 700);
    ASSERT_EQ(state.get(0, 31)->at(1), -31);
    ASSERT_EQ(state.get(1, 17)->at(1), -159);
    bool saved_removed = false, saved_updated = false;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const auto& map) {
        ASSERT_NE(key, collision);
        if (key == 29) saved_removed = map.at(1) == 188;
        if (key == 31) saved_updated = map.at(1) == 190;
    });
    ASSERT_TRUE(saved_removed && saved_updated);
    state.release_snapshot();
}

TEST(TypedInnerMapTest, OversizedVoidRootMovesOnlyReusedMapPayloadsAndReclaimsThem) {
    constexpr size_t quota = 32 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 1, true);
    for (int64_t key = 0; key < 1600; ++key) {
        state.update_or_create(0, key, [&](auto& m) {
            m[1] = key;
            m[2] = -key;
        });
    }
    state.update_or_create(0, 11, [](auto& m) {
        for (int64_t inner = 3; inner <= 32; ++inner) m[inner] = inner;
    });
    state.enable_namespace_storage(manager, quota);
    ASSERT_NE(state.resident_control_views(), nullptr);
    auto edit = [&](int64_t key, int64_t value) {
        state.update_or_create(0, key, [&](auto& m) { m[2] = value; });
    };
    for (int64_t key = 100; key < 1500; ++key) {
        ASSERT_EQ(state.get(0, key)->at(2), -key);
        edit(key, key);
    }
    ASSERT_EQ(state.namespace_cache_stats().root_map_payload_promotions, 0u);
    for (int i = 0; i < 800; ++i) edit(8 + i % 4, i);
    const auto initial = state.namespace_cache_stats();
    ASSERT_EQ(initial.resident_tables, 0u);
    ASSERT_EQ(initial.root_map_payload_promotions, 4u);
    ASSERT_GT(initial.root_map_payload_bytes, 0u);
    const auto* arena = state.namespace_arena();
    ASSERT_FALSE(arena->contains(state.get(0, 11)));
    ASSERT_TRUE(arena->contains(state.get(0, 11)->find_value(2)));
    ASSERT_TRUE(arena->contains_primary(state.get(0, 11)->find_value(32)));
    ASSERT_FALSE(arena->contains(state.get(0, 12)->find_value(2)));

    state.prepare_snapshot();
    for (int64_t key = 1600; key < 4500; ++key) {
        state.update_or_create(0, key, [&](auto& m) { m[1] = key; });
    }
    ASSERT_TRUE(arena->contains(state.get(0, 11)->find_value(2)));
    for (int i = 0; i < 800; ++i) edit(12, 1000 + i);
    ASSERT_EQ(state.namespace_cache_stats().root_map_payload_evictions, 1u);
    ASSERT_TRUE(arena->contains(state.get(0, 12)->find_value(2)));
    ASSERT_EQ(state.get(0, 12)->at(2), 1799);
    for (int64_t key = 8; key <= 12; ++key) ASSERT_TRUE(state.remove(0, key));
    state.update_or_create(0, 12, [](auto& m) { m[3] = 100; });
    ASSERT_EQ(state.namespace_cache_stats().root_map_payload_bytes, 0u);
    ASSERT_EQ(state.get(0, 12)->at(3), 100);
    int checked = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const auto& map) {
        if (key < 8 || key > 12) return;
        ASSERT_EQ(map.at(2), key == 12 ? -12 : 796 + (key - 8));
        ++checked;
    });
    ASSERT_EQ(checked, 5);
    state.release_snapshot();
}

TEST(TypedInnerMapTest, ConsecutiveMapEditsReuseAnOccupiedOuterSlotWithinItsNamespace) {
    constexpr size_t quota = 4096 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 2, true);
    state.enable_namespace_storage(manager, quota);
    for (int rep = 0; rep < 640; ++rep) {
        int group = rep % 2;
        int64_t key = rep % 17;
        state.update_or_create(group, key, [&](auto& m) { m[1] = rep; });
        state.update_or_create(group, key, [&](auto& m) { m[2] = -rep; });
        state.modify_or_remove_in_place(group, key, [](auto& m) { m.erase(2); });
    }
    const auto initial = state.namespace_cache_stats();
    ASSERT_GT(initial.map_edit_reuses, 600u);
    ASSERT_GT(initial.map_edit_bytes, 0u);
    state.update_or_create(0, 99, [](auto& m) { m[7] = 777; });
    state.update_or_create(1, 99, [](auto& m) { m[7] = 888; });
    state.prepare_snapshot();
    state.modify_or_remove_in_place(1, 99, [](auto& m) { m.erase(7); });
    ASSERT_EQ(state.get(1, 99), nullptr);
    state.update_or_create(1, 99, [](auto& m) { m[3] = 333; });
    for (int64_t key = 100; key < 1750; ++key) {
        state.update_or_create(1, key, [&](auto& m) { m[7] = key; });
    }
    ASSERT_GT(state.namespace_cache_stats().map_edit_pauses, initial.map_edit_pauses);
    ASSERT_EQ(*state.get(0, 99)->find_value(7), 777);
    ASSERT_EQ(*state.get(1, 99)->find_value(3), 333);
    state.for_each_snapshot_in_key_group(1, [](const int64_t& key, const auto& m) {
        ASSERT_TRUE(key < 100);
        if (key == 99) {
            ASSERT_EQ(*m.find_value(7), 888);
            ASSERT_EQ(m.find_value(3), nullptr);
        }
    });
    state.release_snapshot();
}

TEST(TypedInnerMapTest, COWSnapshotLongLong) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map{{1, 100}, {2, 200}};
    st.put(0, 10, std::move(map));

    st.prepare_snapshot();

    // Modify after snapshot
    InnerMapLongLong new_map{{1, 999}};
    st.put(0, 10, std::move(new_map));

    // Snapshot should see old map
    std::map<int64_t, InnerMapLongLong> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const InnerMapLongLong& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state[10].size(), 2u);
    ASSERT_EQ(snapshot_state[10][1], 100);
    ASSERT_EQ(snapshot_state[10][2], 200);

    st.release_snapshot();
}

// ============================================================================
//  Large InnerMap
// ============================================================================

TEST(TypedInnerMapTest, LargeInnerMapLongLong) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    for (int i = 0; i < 1000; ++i) {
        map[i] = i * 10;
    }
    st.put(0, 1, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_EQ(m->size(), 1000u);
    ASSERT_EQ((*m)[0], 0);
    ASSERT_EQ((*m)[500], 5000);
    ASSERT_EQ((*m)[999], 9990);
}

TEST(TypedInnerMapTest, PackedFingerprintsKeepFullIdentityAfterEraseCopyAndPromotion) {
    struct OneHash { size_t operator()(int64_t) const { return 3; } };
    using Map = StateMap<int64_t, NamespaceBlob, OneHash>;
    Map map;
    for (int key = 0; key < 16; ++key) {
        const std::string value = std::to_string(key);
        map[key].assign(value.data(), value.size());
    }
    ASSERT_TRUE(map.find_value(40) == nullptr);
    ASSERT_EQ(map.erase(5), 1u);
    ASSERT_EQ(map.erase(1), 1u);
    ASSERT_EQ(map.erase(14), 1u);
    for (int key = 0; key < 16; ++key) {
        const auto* found = map.find_value(key);
        if (key == 1 || key == 5 || key == 14) ASSERT_TRUE(found == nullptr);
        else { ASSERT_TRUE(found != nullptr); ASSERT_EQ(std::string(*found), std::to_string(key)); }
    }
    std::pmr::monotonic_buffer_resource other;
    Map copy(map, Map::allocator_type(&other));
    for (int key = 16; key < 35; ++key) copy[key].assign(40, char(key));
    ASSERT_EQ(copy.size(), 32u);
    for (const auto& entry : map) ASSERT_EQ(copy.at(entry.first), entry.second);
    ASSERT_EQ(copy.at(31).get_allocator().resource(), &other);
    for (int key = 0; key < 13; ++key) map.erase(key);
    for (int key = 40; key < 51; ++key) map[key].assign(1, 'x');
    ASSERT_TRUE(map.find_value(5) == nullptr);
    ASSERT_EQ(map.at(46), NamespaceBlob("x"));
}

TEST(TypedInnerMapTest, LargeInnerMapStringString) {
    StateTable<int64_t, InnerMap> st(0, 1, true);

    InnerMap map;
    for (int i = 0; i < 1000; ++i) {
        map[NamespaceBlob("key_" + std::to_string(i))] = "val_" + std::to_string(i);
    }
    st.put(0, 1, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_EQ(m->size(), 1000u);
    ASSERT_EQ((*m)["key_0"], "val_0");
    ASSERT_EQ((*m)["key_999"], "val_999");
}

// ============================================================================
//  Empty InnerMap
// ============================================================================

TEST(TypedInnerMapTest, EmptyInnerMap) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong empty_map;
    st.put(0, 1, std::move(empty_map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_NE(m, nullptr);
    ASSERT_TRUE(m->empty());
}

TEST(TypedInnerMapTest, FirstEditUsesTableAllocatorAndRollsBackAnIncompleteNewKey) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMap> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    TimeWindow window{5, 10};
    auto add = [](InnerMap& map) { map.try_emplace(NamespaceBlob(44, 'a'), NamespaceBlob(80, 'b')); };
    state.update_or_create(0, window, 5, add);
    state.update_or_create(0, window, 6, add);
    const auto* found = state.get(0, window, 6);
    ASSERT_NE(found, nullptr);
    ASSERT_TRUE(state.namespace_arena()->contains(found->begin()->first.data()));
    ASSERT_TRUE(state.namespace_arena()->contains(found->begin()->second.data()));
    state.prepare_snapshot();
    state.update_or_create(0, window, 5, [](InnerMap& m) { m.begin()->second.assign(81, 'x'); });
    bool failed = false;
    try {
        state.update_or_create(0, window, 8, [&](InnerMap& m) {
            add(m);
            throw std::runtime_error("test abort");
        });
    } catch (const std::runtime_error&) { failed = true; }
    ASSERT_TRUE(failed);
    ASSERT_EQ(state.get(0, window, 8), nullptr);
    size_t snapshot_entries = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0, [&](const auto& ns, const auto&, const auto& m) {
        ASSERT_TRUE(ns == window);
        ASSERT_EQ(m.begin()->second.size(), 80u);
        ASSERT_EQ(m.begin()->second.front(), 'b');
        ++snapshot_entries;
    });
    ASSERT_EQ(snapshot_entries, 2u);
    state.release_snapshot();
}

TEST(TypedInnerMapTest, EverySerializedKeyAndValueUsesTheCopyDestinationForAllMapSizes) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    for (size_t entries : {1u, 2u, 3u, 4u, 5u, 12u}) {
        InnerMap source;
        for (size_t i = 0; i < entries; ++i) {
            source.try_emplace(NamespaceBlob(64, char('A' + i)), NamespaceBlob(96, char('a' + i)));
        }
        {
            InnerMap resident(source, InnerMap::allocator_type(&arena));
            ASSERT_EQ(resident.size(), entries);
            for (const auto& [key, value] : resident) {
                ASSERT_TRUE(arena.contains(key.data()));
                ASSERT_TRUE(arena.contains(value.data()));
                ASSERT_TRUE(source.at(key) == value);
            }
            InnerMap restored(resident);
            for (const auto& [key, value] : restored) {
                ASSERT_FALSE(arena.contains(key.data()));
                ASSERT_FALSE(arena.contains(value.data()));
                ASSERT_TRUE(source.at(key) == value);
            }
        }
        ASSERT_EQ(arena.free_bytes(), arena.capacity());
    }
}

TEST(TypedInnerMapTest, WholeResidentTablePacksFourTagGroupsAndRebuildsAcrossTiers) {
    constexpr size_t budget = 65536;
    auto manager = std::make_shared<HotCacheManager>(2 * budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, 1, false);
    state.enable_namespace_storage(manager, budget);
    InnerMapLongLong ordinary;
    for (int64_t i = 0; i < 23; ++i) ordinary[i] = 7 - i;
    ASSERT_FALSE(ordinary.supports_compact_lookup());
    auto* map = state.put(0, int64_t{11}, 9, ordinary);
    ASSERT_TRUE(map->supports_compact_lookup());
    for (int64_t i = 23; i < 65; ++i) {
        state.update_or_create(0, int64_t{11}, int64_t{9}, [i](auto& values) { values[i] = 7 - i; });
        map = state.get(0, int64_t{11}, 9);
        ASSERT_EQ(map->supports_compact_lookup(), i < 64);
        for (int64_t k = 0; k <= i; ++k) ASSERT_EQ(map->at(k), 7 - k);
        if (i == 48) {
            InnerMapLongLong exported(*map);
            ASSERT_FALSE(exported.supports_compact_lookup());
            for (int64_t j = 0; j <= i; ++j) ASSERT_EQ(exported.at(j), 7 - j);
            for (const auto& [key, value] : *map) ASSERT_TRUE(state.namespace_arena()->contains(&value));
            for (int64_t j : {3, 17, 31, 47}) ASSERT_EQ(map->erase(j), 1u);
            for (int64_t j : {3, 17, 31, 47}) ASSERT_EQ(map->count(j), 0u);
            for (int64_t j : {3, 17, 31, 47}) (*map)[j] = 7 - j;
        }
    }
}
