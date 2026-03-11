// CheckpointReader: restores SwissTable state data from Flink-compatible
// checkpoint binary format.
//
// Input format per key group (inside compression stream):
//   For each state block:
//     stateId (short)
//     entryCount (int) — -1 means end of key group
//     For each entry:
//       namespace_bytes
//       key_bytes
//       value_bytes

#pragma once

#include "flink_binary_format.h"
#include "state_engine.h"
#include "type_layout.h"

#include <cstdint>
#include <string>
#include <vector>

namespace forl0 {

// ============================================================================
//  Typed checkpoint reader functions
// ============================================================================

template <typename T>
inline T read_flink_value(ReadBuffer& buf, const TypeLayout& layout);

template <>
inline int32_t read_flink_value<int32_t>(ReadBuffer& buf, const TypeLayout&) {
    return buf.read_int();
}

template <>
inline int64_t read_flink_value<int64_t>(ReadBuffer& buf, const TypeLayout&) {
    return buf.read_long();
}

template <>
inline float read_flink_value<float>(ReadBuffer& buf, const TypeLayout&) {
    return buf.read_float();
}

template <>
inline double read_flink_value<double>(ReadBuffer& buf, const TypeLayout&) {
    return buf.read_double();
}

template <>
inline bool read_flink_value<bool>(ReadBuffer& buf, const TypeLayout&) {
    return buf.read_bool();
}

template <>
inline std::string read_flink_value<std::string>(ReadBuffer& buf, const TypeLayout& layout) {
    if (layout.type_id == TypeId::BYTES) {
        return buf.read_bytes_flink();
    }
    return buf.read_string_flink();
}

// ElementList: read from [total_byte_length(4)][count(4)][elem1][elem2]...
// Each element is captured as raw serialized bytes using element_layout to determine boundaries.
template <>
inline ElementList read_flink_value<ElementList>(ReadBuffer& buf, const TypeLayout& layout) {
    int32_t total_bytes = buf.read_int();  // outer length prefix
    if (total_bytes < 4) return {};
    ElementList result;
    int32_t count = buf.read_int();
    result.reserve(count);
    // The layout should be LIST with one child (element type)
    const TypeLayout* elem_layout = nullptr;
    if (layout.type_id == TypeId::LIST && !layout.children.empty()) {
        elem_layout = layout.children[0].get();
    }
    for (int32_t i = 0; i < count; ++i) {
        if (elem_layout) {
            result.push_back(buf.read_element_bytes(*elem_layout));
        } else {
            // Fallback: read remaining content bytes as single element
            // total_bytes - 4 (count int) = remaining content bytes
            size_t content_remaining = static_cast<size_t>(total_bytes) - 4;
            // Subtract bytes already consumed for previous elements
            size_t rem = std::min(content_remaining, buf.remaining());
            result.emplace_back(reinterpret_cast<const char*>(buf.current()), rem);
            buf.skip(rem);
            break;
        }
    }
    return result;
}

// InnerMap: read from [total_byte_length(4)][count(4)][uk1][uv1][uk2][uv2]...
// Each UK/UV is captured as raw serialized bytes using user_key/user_value layouts.
template <>
inline InnerMap read_flink_value<InnerMap>(ReadBuffer& buf, const TypeLayout& layout) {
    int32_t total_bytes = buf.read_int();  // outer length prefix
    if (total_bytes < 4) return {};
    InnerMap result;
    int32_t count = buf.read_int();
    // The layout should be MAP with two children (uk_type, uv_type)
    const TypeLayout* uk_layout = nullptr;
    const TypeLayout* uv_layout = nullptr;
    if (layout.type_id == TypeId::MAP && layout.children.size() >= 2) {
        uk_layout = layout.children[0].get();
        uv_layout = layout.children[1].get();
    }
    for (int32_t i = 0; i < count; ++i) {
        std::string uk, uv;
        if (uk_layout) {
            uk = buf.read_element_bytes(*uk_layout);
        }
        if (uv_layout) {
            uv = buf.read_element_bytes(*uv_layout);
        }
        result[std::move(uk)] = std::move(uv);
    }
    return result;
}

// Skip VoidNamespace bytes (1 byte: 0)
inline void skip_void_namespace(ReadBuffer& buf) {
    buf.skip(1);
}

// FixedRow: read from BinaryRowData format (length-prefixed).
// Format: [sizeInBytes (4B BE)][header (8B)][field0 (8B LE)][field1 (8B LE)]...
template <>
inline FixedRow read_flink_value<FixedRow>(ReadBuffer& buf, const TypeLayout& layout) {
    buf.read_int();  // skip length prefix
    buf.skip(8);     // skip header (RowKind + null bits)
    uint8_t arity = layout.fixed_row_arity;
    FixedRow row(arity);
    for (uint8_t i = 0; i < arity; ++i) {
        uint64_t v = 0;
        for (int b = 0; b < 8; ++b) {
            v |= static_cast<uint64_t>(buf.read_byte()) << (b * 8);
        }
        row.f[i] = static_cast<int64_t>(v);
    }
    return row;
}

// ============================================================================
//  CheckpointStateReader — reads one key group's data for one state
// ============================================================================

template <typename K, typename V>
class CheckpointStateReader {
public:
    CheckpointStateReader(const TypeLayout& key_layout,
                          const TypeLayout& value_layout,
                          bool void_namespace)
        : key_layout_(key_layout),
          value_layout_(value_layout),
          void_namespace_(void_namespace) {}

    // Read entry_count entries from buffer and insert into the state table.
    void read_entries(ReadBuffer& buf,
                      StateTable<K, V>& table,
                      int key_group,
                      int32_t entry_count) {
        for (int32_t i = 0; i < entry_count; ++i) {
            if (void_namespace_) {
                skip_void_namespace(buf);
            }
            K key = read_flink_value<K>(buf, key_layout_);
            V value = read_flink_value<V>(buf, value_layout_);
            table.put(key_group, key, std::move(value));
        }
    }

    // Read ListState entries: value is std::vector<T>
    template <typename T>
    void read_list_entries(ReadBuffer& buf,
                           StateTable<K, std::vector<T>>& table,
                           int key_group,
                           int32_t entry_count,
                           const TypeLayout& elem_layout) {
        for (int32_t i = 0; i < entry_count; ++i) {
            if (void_namespace_) {
                skip_void_namespace(buf);
            }
            K key = read_flink_value<K>(buf, key_layout_);
            int32_t list_size = buf.read_int();
            std::vector<T> list;
            list.reserve(list_size);
            for (int32_t j = 0; j < list_size; ++j) {
                list.push_back(read_flink_value<T>(buf, elem_layout));
            }
            table.put(key_group, key, std::move(list));
        }
    }

private:
    const TypeLayout& key_layout_;
    const TypeLayout& value_layout_;
    bool void_namespace_;
};

}  // namespace forl0
