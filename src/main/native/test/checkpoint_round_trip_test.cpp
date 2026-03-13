// Checkpoint round-trip tests — write → read for all types.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "state_engine.h"
#include "checkpoint_writer.h"
#include "checkpoint_reader.h"
#include "flink_binary_format.h"
#include "type_layout.h"

using namespace forl0;

// ============================================================================
//  Helper: creates TypeLayout for common types
// ============================================================================

static TypeLayout make_layout(TypeId id) {
    TypeLayout layout;
    layout.type_id = id;
    switch (id) {
        case TypeId::INT32:   layout.cpp_size = 4; break;
        case TypeId::INT64:   layout.cpp_size = 8; break;
        case TypeId::FLOAT32: layout.cpp_size = 4; break;
        case TypeId::FLOAT64: layout.cpp_size = 8; break;
        case TypeId::BOOL:    layout.cpp_size = 1; break;
        case TypeId::STRING:  layout.cpp_size = sizeof(std::string); break;
        case TypeId::BYTES:   layout.cpp_size = sizeof(std::string); break;
        default: break;
    }
    return layout;
}

static TypeLayout make_fixed_row_layout(uint16_t arity) {
    TypeLayout layout;
    layout.type_id = TypeId::FIXED_ROW;
    layout.fixed_row_arity = arity;
    layout.cpp_size = sizeof(FixedRow);
    return layout;
}

static TypeLayout make_list_layout(TypeId element_type) {
    TypeLayout layout;
    layout.type_id = TypeId::LIST;
    layout.cpp_size = sizeof(std::vector<char>);
    auto child = std::make_unique<TypeLayout>(make_layout(element_type));
    layout.children.push_back(std::move(child));
    return layout;
}

static TypeLayout make_map_layout(TypeId uk_type, TypeId uv_type) {
    TypeLayout layout;
    layout.type_id = TypeId::MAP;
    layout.cpp_size = 64;
    layout.children.push_back(std::make_unique<TypeLayout>(make_layout(uk_type)));
    layout.children.push_back(std::make_unique<TypeLayout>(make_layout(uv_type)));
    return layout;
}

// ============================================================================
//  ValueState: Int64 → Int64
// ============================================================================

TEST(CheckpointRoundTripTest, ValueStateInt64) {
    StateTable<int64_t, int64_t> source(0, 1, true);
    source.put(0, 10, 1000);
    source.put(0, 20, 2000);
    source.put(0, 30, 3000);

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_layout(TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, int64_t> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    ASSERT_EQ(count, 3u);
    ASSERT_GT(buf.size(), 0u);

    // Restore
    StateTable<int64_t, int64_t> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, int64_t> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(restored.total_size(), 3u);
    ASSERT_EQ(*restored.get(0, 10), 1000);
    ASSERT_EQ(*restored.get(0, 20), 2000);
    ASSERT_EQ(*restored.get(0, 30), 3000);

    source.release_snapshot();
}

// ============================================================================
//  ValueState: String → String
// ============================================================================

TEST(CheckpointRoundTripTest, ValueStateString) {
    StateTable<std::string, std::string> source(0, 1, true);
    source.put(0, std::string("key1"), std::string("value1"));
    source.put(0, std::string("key2"), std::string("value2"));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::STRING);
    TypeLayout vl = make_layout(TypeId::STRING);
    WriteBuffer buf;
    CheckpointStateWriter<std::string, std::string> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    // Restore
    StateTable<std::string, std::string> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<std::string, std::string> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(restored.total_size(), 2u);
    ASSERT_EQ(*restored.get(0, std::string("key1")), "value1");
    ASSERT_EQ(*restored.get(0, std::string("key2")), "value2");

    source.release_snapshot();
}

// ============================================================================
//  ValueState: FixedRow → Int64
// ============================================================================

TEST(CheckpointRoundTripTest, ValueStateFixedRow) {
    StateTable<FixedRow, int64_t> source(0, 1, true);

    FixedRow k1(2);
    k1.f[0] = 100;
    k1.f[1] = 200;

    FixedRow k2(2);
    k2.f[0] = 300;
    k2.f[1] = 400;

    source.put(0, k1, 1000);
    source.put(0, k2, 2000);

    source.prepare_snapshot();

    TypeLayout kl = make_fixed_row_layout(2);
    TypeLayout vl = make_layout(TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<FixedRow, int64_t> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    // Restore
    StateTable<FixedRow, int64_t> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<FixedRow, int64_t> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(restored.total_size(), 2u);
    ASSERT_EQ(*restored.get(0, k1), 1000);
    ASSERT_EQ(*restored.get(0, k2), 2000);

    source.release_snapshot();
}

// ============================================================================
//  ValueState with TimeWindow namespace
// ============================================================================

TEST(CheckpointRoundTripTest, ValueStateTimeWindow) {
    StateTable<int64_t, int64_t> source(0, 1, false);

    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{2000, 3000};

    source.put<TimeWindow>(0, tw1, 100, 10);
    source.put<TimeWindow>(0, tw2, 100, 20);
    source.put<TimeWindow>(0, tw1, 200, 30);

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_layout(TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, int64_t> writer(kl, vl, false);
    size_t count = writer.write_key_group_tw(source, 0, buf);

    ASSERT_EQ(count, 3u);

    // Restore
    StateTable<int64_t, int64_t> restored(0, 1, false);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, int64_t> reader_obj(kl, vl, false);
    reader_obj.read_entries_tw(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(*restored.get<TimeWindow>(0, tw1, 100), 10);
    ASSERT_EQ(*restored.get<TimeWindow>(0, tw2, 100), 20);
    ASSERT_EQ(*restored.get<TimeWindow>(0, tw1, 200), 30);

    source.release_snapshot();
}

// ============================================================================
//  MapState: InnerMap (string → string)
// ============================================================================

TEST(CheckpointRoundTripTest, MapStateStringString) {
    StateTable<int64_t, InnerMap> source(0, 1, true);

    InnerMap map1{{"k1", "v1"}, {"k2", "v2"}};
    InnerMap map2{{"k3", "v3"}};
    source.put(0, 10, std::move(map1));
    source.put(0, 20, std::move(map2));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_map_layout(TypeId::STRING, TypeId::STRING);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, InnerMap> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    // Restore
    StateTable<int64_t, InnerMap> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, InnerMap> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(restored.total_size(), 2u);
    auto* m1 = restored.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m1, nullptr);
    ASSERT_EQ(m1->size(), 2u);

    source.release_snapshot();
}

// ============================================================================
//  MapState: InnerMapLongLong
// ============================================================================

TEST(CheckpointRoundTripTest, MapStateLongLong) {
    StateTable<int64_t, InnerMapLongLong> source(0, 1, true);

    InnerMapLongLong map1{{1, 100}, {2, 200}};
    source.put(0, 10, std::move(map1));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_map_layout(TypeId::INT64, TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, InnerMapLongLong> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    // Restore
    StateTable<int64_t, InnerMapLongLong> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, InnerMapLongLong> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    auto* m1 = restored.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m1, nullptr);
    ASSERT_EQ(m1->size(), 2u);
    ASSERT_EQ((*m1)[1], 100);
    ASSERT_EQ((*m1)[2], 200);

    source.release_snapshot();
}

// ============================================================================
//  MapState: InnerMapLongString
// ============================================================================

TEST(CheckpointRoundTripTest, MapStateLongString) {
    StateTable<int64_t, InnerMapLongString> source(0, 1, true);

    InnerMapLongString map1{{1, "hello"}, {2, "world"}};
    source.put(0, 10, std::move(map1));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_map_layout(TypeId::INT64, TypeId::STRING);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, InnerMapLongString> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    StateTable<int64_t, InnerMapLongString> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, InnerMapLongString> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    auto* m1 = restored.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m1, nullptr);
    ASSERT_EQ((*m1)[1], "hello");
    ASSERT_EQ((*m1)[2], "world");

    source.release_snapshot();
}

// ============================================================================
//  MapState: InnerMapStringLong
// ============================================================================

TEST(CheckpointRoundTripTest, MapStateStringLong) {
    StateTable<int64_t, InnerMapStringLong> source(0, 1, true);

    InnerMapStringLong map1{{"a", 100}, {"b", 200}};
    source.put(0, 10, std::move(map1));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_map_layout(TypeId::STRING, TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, InnerMapStringLong> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    StateTable<int64_t, InnerMapStringLong> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, InnerMapStringLong> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    auto* m1 = restored.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m1, nullptr);
    ASSERT_EQ((*m1)["a"], 100);
    ASSERT_EQ((*m1)["b"], 200);

    source.release_snapshot();
}

// ============================================================================
//  ListState: ElementList round-trip
// ============================================================================

TEST(CheckpointRoundTripTest, ListState) {
    StateTable<int64_t, ElementList> source(0, 1, true);

    ElementList list1{"elem1", "elem2", "elem3"};
    ElementList list2{"single"};
    source.put(0, 10, std::move(list1));
    source.put(0, 20, std::move(list2));

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_list_layout(TypeId::STRING);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, ElementList> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    StateTable<int64_t, ElementList> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, ElementList> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    auto* l1 = restored.get(0, static_cast<int64_t>(10));
    ASSERT_NE(l1, nullptr);
    ASSERT_EQ(l1->size(), 3u);

    auto* l2 = restored.get(0, static_cast<int64_t>(20));
    ASSERT_NE(l2, nullptr);
    ASSERT_EQ(l2->size(), 1u);

    source.release_snapshot();
}

// ============================================================================
//  COW + Checkpoint: snapshot consistency
// ============================================================================

TEST(CheckpointRoundTripTest, COWConsistency) {
    StateTable<int64_t, int64_t> source(0, 1, true);

    source.put(0, 1, 100);
    source.put(0, 2, 200);

    source.prepare_snapshot();

    // Modify after snapshot
    source.put(0, 1, 999);
    source.put(0, 3, 300);
    source.remove(0, 2);

    // Write using snapshot-consistent iteration
    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_layout(TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, int64_t> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    // Should have written pre-snapshot state: {1→100, 2→200}
    ASSERT_EQ(count, 2u);

    StateTable<int64_t, int64_t> restored(0, 1, true);
    ReadBuffer reader(buf.data(), buf.size());
    CheckpointStateReader<int64_t, int64_t> reader_obj(kl, vl, true);
    reader_obj.read_entries(reader, restored, 0, static_cast<int32_t>(count));

    ASSERT_EQ(*restored.get(0, 1), 100);  // old value
    ASSERT_EQ(*restored.get(0, 2), 200);  // deleted key restored
    ASSERT_EQ(restored.get(0, 3), nullptr);  // not in snapshot

    source.release_snapshot();
}

// ============================================================================
//  Empty State Round-Trip
// ============================================================================

TEST(CheckpointRoundTripTest, EmptyState) {
    StateTable<int64_t, int64_t> source(0, 1, true);
    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_layout(TypeId::INT64);
    WriteBuffer buf;
    CheckpointStateWriter<int64_t, int64_t> writer(kl, vl, true);
    size_t count = writer.write_key_group(source, 0, buf);

    ASSERT_EQ(count, 0u);

    source.release_snapshot();
}

// ============================================================================
//  Multiple Key Groups
// ============================================================================

TEST(CheckpointRoundTripTest, MultipleKeyGroups) {
    StateTable<int64_t, int64_t> source(0, 3, true);

    source.put(0, 1, 100);
    source.put(0, 2, 200);
    source.put(1, 3, 300);
    source.put(2, 4, 400);

    source.prepare_snapshot();

    TypeLayout kl = make_layout(TypeId::INT64);
    TypeLayout vl = make_layout(TypeId::INT64);

    // Write and restore each key group
    StateTable<int64_t, int64_t> restored(0, 3, true);

    for (int kg = 0; kg < 3; ++kg) {
        WriteBuffer buf;
        CheckpointStateWriter<int64_t, int64_t> writer(kl, vl, true);
        size_t count = writer.write_key_group(source, kg, buf);

        ReadBuffer reader(buf.data(), buf.size());
        CheckpointStateReader<int64_t, int64_t> reader_obj(kl, vl, true);
        reader_obj.read_entries(reader, restored, kg, static_cast<int32_t>(count));
    }

    ASSERT_EQ(restored.total_size(), 4u);
    ASSERT_EQ(*restored.get(0, 1), 100);
    ASSERT_EQ(*restored.get(0, 2), 200);
    ASSERT_EQ(*restored.get(1, 3), 300);
    ASSERT_EQ(*restored.get(2, 4), 400);

    source.release_snapshot();
}
