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
//  Helper: convert plain string to Flink StringValue serialized bytes
// ============================================================================

static std::string flink_string(const std::string& s) {
    WriteBuffer buf;
    buf.write_string_flink(s);
    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

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
    layout.cpp_size = sizeof(ElementList);
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

    // InnerMap stores Flink-serialized byte strings (as from JNI)
    InnerMap map1{{NamespaceBlob(flink_string("k1")), NamespaceBlob(flink_string("v1"))}, {NamespaceBlob(flink_string("k2")), NamespaceBlob(flink_string("v2"))}};
    InnerMap map2{{NamespaceBlob(flink_string("k3")), NamespaceBlob(flink_string("v3"))}};
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

TEST(CheckpointRoundTripTest, RootRowsExchangeExistingByteCheckpointsInBothDirections) {
    const auto kl = make_layout(TypeId::INT64), vl = make_layout(TypeId::BYTES);
    const std::string payloads[] = {"", std::string("ab\0c", 4), std::string(24, 's'), std::string(25, 'm'), std::string(400, 'l')};
    StateTable<int64_t, NamespaceBlob> old(0, 1, true), legacy_reader(0, 1, true);
    for (int i = 0; i < 5; ++i) old.put(0, i, NamespaceBlob(payloads[i]));
    WriteBuffer fixture;
    CheckpointStateWriter<int64_t, NamespaceBlob>(kl, vl, true).write_key_group(old, 0, fixture);
    StateTable<int64_t, NamespaceRow> rows(0, 1, true);
    ReadBuffer old_bytes(fixture.data(), fixture.size());
    CheckpointStateReader<int64_t, NamespaceRow>(kl, vl, true).read_entries(old_bytes, rows, 0, 5);
    ASSERT_EQ(old_bytes.remaining(), 0u);
    rows.prepare_snapshot();
    rows.remove(0, 1);
    rows.put(0, 4, NamespaceRow("new", 3));
    WriteBuffer frozen;
    CheckpointStateWriter<int64_t, NamespaceRow> writer(kl, vl, true);
    ASSERT_EQ(writer.write_key_group(rows, 0, frozen), 5u);
    ReadBuffer new_bytes(frozen.data(), frozen.size());
    CheckpointStateReader<int64_t, NamespaceBlob>(kl, vl, true).read_entries(new_bytes, legacy_reader, 0, 5);
    rows.release_snapshot();
    for (int i = 0; i < 5; ++i) {
        const auto* bytes = legacy_reader.get(0, i);
        ASSERT_NE(bytes, nullptr);
        ASSERT_TRUE(std::string_view(*bytes) == std::string_view(payloads[i]));
    }
}

TEST(CheckpointRoundTripTest, MapStateLongString) {
    StateTable<int64_t, InnerMapLongString> source(0, 1, true);

    // Typed map string values store Flink-serialized bytes (as from JNI)
    InnerMapLongString map1{{1, NamespaceBlob(flink_string("hello"))}, {2, NamespaceBlob(flink_string("world"))}};
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
    ASSERT_EQ((*m1)[1], NamespaceBlob(flink_string("hello")));
    ASSERT_EQ((*m1)[2], NamespaceBlob(flink_string("world")));

    source.release_snapshot();
}

// ============================================================================
//  MapState: InnerMapStringLong
// ============================================================================

TEST(CheckpointRoundTripTest, MapStateStringLong) {
    StateTable<int64_t, InnerMapStringLong> source(0, 1, true);

    // Typed map string keys store Flink-serialized bytes (as from JNI)
    InnerMapStringLong map1{{NamespaceBlob(flink_string("a")), 100}, {NamespaceBlob(flink_string("b")), 200}};
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
    ASSERT_EQ((*m1)[NamespaceBlob(flink_string("a"))], 100);
    ASSERT_EQ((*m1)[NamespaceBlob(flink_string("b"))], 200);

    source.release_snapshot();
}

// ============================================================================
//  ListState: ElementList round-trip
// ============================================================================

TEST(CheckpointRoundTripTest, ListState) {
    StateTable<int64_t, ElementList> source(0, 1, true);

    // ElementList stores Flink-serialized byte strings (as from JNI)
    ElementList list1{NamespaceBlob(flink_string("elem1")), NamespaceBlob(flink_string("elem2")), NamespaceBlob(flink_string("elem3"))};
    ElementList list2{NamespaceBlob(flink_string("single"))};
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

TEST(CheckpointRoundTripTest, PackedListRetainsAliasedAndZeroLengthElements) {
    ElementList values;
    values.append({});
    const std::string payload(80, 'b');
    values.append(payload);
    values.append(values.payload().substr(1, 70));
    ASSERT_EQ(values.size(), 3u);
    ASSERT_EQ(values.payload(), std::string(150, 'b'));
    values.append_list(values);
    ASSERT_EQ(values.size(), 6u);
    const std::string frozen(values.serialized());
    ASSERT_THROW(values.append_bytes(1, 137, [](char*) { throw std::runtime_error("write failed"); }),
                 std::runtime_error);
    ASSERT_EQ(values.serialized(), frozen);
    const auto restored = ElementList::from_serialized(frozen);
    ASSERT_EQ(restored.serialized(), std::string_view(frozen));
    ElementList zero;
    zero.append({});
    ASSERT_EQ(zero.size(), 1u);
    ASSERT_TRUE(zero.payload().empty());
    ASSERT_FALSE(ElementList::from_serialized(zero.serialized()).empty());
}

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
