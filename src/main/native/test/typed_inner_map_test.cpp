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
    legacy.emplace(std::string(reinterpret_cast<const char*>(key.data()), key.size()),
                   std::string(reinterpret_cast<const char*>(value.data()), value.size()));
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

TEST(TypedInnerMapTest, TimeWindowReplacementReusesNodeAndRemovesEmptyParent) {
    StateTable<int64_t, InnerMapTimeWindow> table(0, 1, true);
    TimeWindowMapScratch scratch;
    put_window_entry(&table, scratch, 0, 7, TimeWindow(0, 10), TimeWindow(0, 20));
    auto* original_value = &table.get(0, 7)->begin()->second;
    size_t buckets = table.get(0, 7)->bucket_count();
    table.prepare_snapshot();
    remove_window_entry(&table, scratch, 0, 7, TimeWindow(0, 10));
    ASSERT_EQ(table.total_size(), 0u);
    ASSERT_FALSE(scratch.node.empty());
    ASSERT_EQ(scratch.empty_map.bucket_count(), buckets);
    put_window_entry(&table, scratch, 0, 7, TimeWindow(0, 30), TimeWindow(0, 40));
    ASSERT_EQ(table.total_size(), 1u);
    ASSERT_EQ(&table.get(0, 7)->begin()->second, original_value);
    ASSERT_EQ(table.get(0, 7)->bucket_count(), buckets);
    ASSERT_TRUE(scratch.node.empty());
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
        put_window_entry(&table, scratch, 0, 8, TimeWindow(i, i + 1), TimeWindow(i, i + 2));
    for (int64_t i = 0; i < 100; ++i)
        remove_window_entry(&table, scratch, 0, 8, TimeWindow(i, i + 1));
    ASSERT_LT(scratch.empty_map.bucket_count(), 65u);
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

TEST(TypedInnerMapTest, LargeInnerMapStringString) {
    StateTable<int64_t, InnerMap> st(0, 1, true);

    InnerMap map;
    for (int i = 0; i < 1000; ++i) {
        map["key_" + std::to_string(i)] = "val_" + std::to_string(i);
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
