// CheckpointWriter: serializes SwissTable state data to Flink-compatible
// checkpoint binary format. Called from JNI per key group.
//
// Output format per key group (inside the compression stream):
//   For each registered state:
//     stateId (short)
//     entryCount (int)
//     For each entry:
//       namespace_bytes (via TypeSerializer format)
//       key_bytes (via TypeSerializer format)
//       value_bytes (via TypeSerializer format)

#pragma once

#include "flink_binary_format.h"
#include "state_engine.h"
#include "type_layout.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace forl0 {

// ============================================================================
//  Typed checkpoint writer functions
// ============================================================================

// Write a single primitive value to the buffer in Flink format.
template <typename T>
inline void write_flink_value(WriteBuffer& buf, const T& value, const TypeLayout& layout);

template <>
inline void write_flink_value<int32_t>(WriteBuffer& buf, const int32_t& value, const TypeLayout&) {
    buf.write_int(value);
}

template <>
inline void write_flink_value<int64_t>(WriteBuffer& buf, const int64_t& value, const TypeLayout&) {
    buf.write_long(value);
}

template <>
inline void write_flink_value<float>(WriteBuffer& buf, const float& value, const TypeLayout&) {
    buf.write_float(value);
}

template <>
inline void write_flink_value<double>(WriteBuffer& buf, const double& value, const TypeLayout&) {
    buf.write_double(value);
}

template <>
inline void write_flink_value<bool>(WriteBuffer& buf, const bool& value, const TypeLayout&) {
    buf.write_bool(value);
}

template <>
inline void write_flink_value<std::string>(WriteBuffer& buf, const std::string& value, const TypeLayout& layout) {
    if (layout.type_id == TypeId::BYTES) {
        buf.write_bytes_flink(value);
    } else {
        buf.write_string_flink(value);
    }
}

// ElementList: write as [total_byte_length(4)][count(4)][elem1_bytes][elem2_bytes]...
// The outer 4-byte length prefix allows Java ForL0KeyValueStateIterator to
// read the value with FMT_LEN_PREFIXED (readInt + readBytes).
// Each element is already serialized bytes — written raw.
template <>
inline void write_flink_value<ElementList>(WriteBuffer& buf, const ElementList& value, const TypeLayout&) {
    // OPT-9: Write directly with placeholder length, then patch
    size_t len_pos = buf.size();
    buf.write_int(0);  // placeholder
    buf.write_int(static_cast<int32_t>(value.size()));
    for (const auto& elem : value) {
        buf.write_raw(reinterpret_cast<const uint8_t*>(elem.data()), elem.size());
    }
    buf.patch_int(len_pos, static_cast<int32_t>(buf.size() - len_pos - 4));
}

// InnerMap: write as [total_byte_length(4)][count(4)][uk1][uv1][uk2][uv2]...
// The outer 4-byte length prefix allows Java ForL0KeyValueStateIterator to
// read the value with FMT_LEN_PREFIXED (readInt + readBytes).
// User keys and values are already serialized bytes — written raw.
template <>
inline void write_flink_value<InnerMap>(WriteBuffer& buf, const InnerMap& value, const TypeLayout&) {
    // OPT-9: Write directly with placeholder length, then patch
    size_t len_pos = buf.size();
    buf.write_int(0);  // placeholder
    buf.write_int(static_cast<int32_t>(value.size()));
    for (const auto& entry : value) {
        buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
        buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
    }
    buf.patch_int(len_pos, static_cast<int32_t>(buf.size() - len_pos - 4));
}

// Typed InnerMap variants — same checkpoint format as InnerMap but with typed entries.

template <>
inline void write_flink_value<InnerMapLongLong>(WriteBuffer& buf, const InnerMapLongLong& value, const TypeLayout&) {
    // OPT-9: Direct write with placeholder
    size_t len_pos = buf.size();
    buf.write_int(0);
    buf.write_int(static_cast<int32_t>(value.size()));
    for (const auto& entry : value) {
        buf.write_long(entry.first);
        buf.write_long(entry.second);
    }
    buf.patch_int(len_pos, static_cast<int32_t>(buf.size() - len_pos - 4));
}

template <>
inline void write_flink_value<InnerMapLongString>(WriteBuffer& buf, const InnerMapLongString& value, const TypeLayout&) {
    // OPT-9: Direct write with placeholder
    size_t len_pos = buf.size();
    buf.write_int(0);
    buf.write_int(static_cast<int32_t>(value.size()));
    for (const auto& entry : value) {
        buf.write_long(entry.first);
        buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
    }
    buf.patch_int(len_pos, static_cast<int32_t>(buf.size() - len_pos - 4));
}

template <>
inline void write_flink_value<InnerMapStringLong>(WriteBuffer& buf, const InnerMapStringLong& value, const TypeLayout&) {
    // OPT-9: Direct write with placeholder
    size_t len_pos = buf.size();
    buf.write_int(0);
    buf.write_int(static_cast<int32_t>(value.size()));
    for (const auto& entry : value) {
        buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
        buf.write_long(entry.second);
    }
    buf.patch_int(len_pos, static_cast<int32_t>(buf.size() - len_pos - 4));
}

// Write VoidNamespace marker (Flink VoidNamespaceSerializer writes a single 0 byte)
inline void write_void_namespace(WriteBuffer& buf) {
    buf.write_byte(0);
}

// Write TimeWindow namespace: [ns_start(8B BE)][ns_end(8B BE)]
inline void write_time_window_namespace(WriteBuffer& buf, const TimeWindow& tw) {
    buf.write_long(tw.first);   // start, big-endian
    buf.write_long(tw.second);  // end, big-endian
}

// FixedRow: write as BinaryRowData format (length-prefixed).
// Format: [sizeInBytes (4B BE)][header (8B)][field0 (8B LE)][field1 (8B LE)]...
// Header: byte 0 = RowKind.INSERT (0x00), bytes 1-7 = null bits (all 0).
// Fields stored in little-endian (matching Flink BinaryRowData native byte order).
template <>
inline void write_flink_value<FixedRow>(WriteBuffer& buf, const FixedRow& value, const TypeLayout& layout) {
    uint8_t arity = layout.fixed_row_arity;
    int32_t total_size = 8 + static_cast<int32_t>(arity) * 8;
    buf.write_int(total_size);  // length prefix (big-endian)
    // Header: RowKind.INSERT=0, no null bits
    for (int i = 0; i < 8; ++i) buf.write_byte(0);
    // Fields in little-endian
    for (uint8_t i = 0; i < arity; ++i) {
        int64_t f = value.f[i];
        for (int b = 0; b < 8; ++b) {
            buf.write_byte(static_cast<uint8_t>((f >> (b * 8)) & 0xFF));
        }
    }
}

// ============================================================================
//  CheckpointWriter — writes one key group's data for one state
// ============================================================================

template <typename K, typename V>
class CheckpointStateWriter {
public:
    CheckpointStateWriter(const TypeLayout& key_layout,
                          const TypeLayout& value_layout,
                          bool void_namespace)
        : key_layout_(key_layout),
          value_layout_(value_layout),
          void_namespace_(void_namespace) {}

    // Write all entries for a key group into the buffer.
    // Uses snapshot-consistent iteration (COW) when a snapshot is active.
    // Returns the number of entries written.
    size_t write_key_group(const StateTable<K, V>& table,
                           int key_group,
                           WriteBuffer& buf) {
        size_t count = 0;
        table.for_each_snapshot_in_key_group(key_group, [&](const K& key, const V& value) {
            if (void_namespace_) {
                write_void_namespace(buf);
            }
            write_flink_value(buf, key, key_layout_);
            write_flink_value(buf, value, value_layout_);
            ++count;
        });
        return count;
    }

    // Write all entries for a key group with TimeWindow namespaces.
    // Uses snapshot-consistent iteration (COW) when a snapshot is active.
    size_t write_key_group_tw(const StateTable<K, V>& table,
                              int key_group,
                              WriteBuffer& buf) {
        size_t count = 0;
        table.template for_each_snapshot_in_key_group_ns<TimeWindow>(key_group,
            [&](const TimeWindow& tw, const K& key, const V& value) {
                write_time_window_namespace(buf, tw);
                write_flink_value(buf, key, key_layout_);
                write_flink_value(buf, value, value_layout_);
                ++count;
            });
        return count;
    }

    // Write for ListState: value is std::vector<T>
    // Flink ListSerializer writes: element_count(int) + each element
    template <typename T>
    size_t write_key_group_list(const StateTable<K, std::vector<T>>& table,
                                int key_group,
                                WriteBuffer& buf,
                                const TypeLayout& elem_layout) {
        size_t count = 0;
        table.for_each_snapshot_in_key_group(key_group, [&](const K& key, const std::vector<T>& list) {
            if (void_namespace_) {
                write_void_namespace(buf);
            }
            write_flink_value(buf, key, key_layout_);
            // ListSerializer format: element count + elements
            buf.write_int(static_cast<int32_t>(list.size()));
            for (const auto& elem : list) {
                write_flink_value(buf, elem, elem_layout);
            }
            ++count;
        });
        return count;
    }

private:
    const TypeLayout& key_layout_;
    const TypeLayout& value_layout_;
    bool void_namespace_;
};

}  // namespace forl0
