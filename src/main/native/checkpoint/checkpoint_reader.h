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

template <>
inline NamespaceBlob read_flink_value<NamespaceBlob>(ReadBuffer& buf, const TypeLayout& layout) {
    return NamespaceBlob(read_flink_value<std::string>(buf, layout));
}

template <>
inline NamespaceRow read_flink_value<NamespaceRow>(ReadBuffer& buf, const TypeLayout&) {
    const int32_t length = buf.read_int();
    if (length < 0) throw std::runtime_error("negative binary row length");
    const auto* bytes = buf.current();
    buf.skip(static_cast<size_t>(length));
    return NamespaceRow(reinterpret_cast<const char*>(bytes), static_cast<size_t>(length));
}

// ElementList: read from [total_byte_length(4)][count(4)][elem1][elem2]...
// The outer length delimits the entire serialized list, including opaque elements.
template <>
inline ElementList read_flink_value<ElementList>(ReadBuffer& buf, const TypeLayout&) {
    const int32_t length = buf.read_int();
    if (!length) return {};
    if (length < 4 || size_t(length) > buf.remaining())
        throw std::runtime_error("Invalid checkpoint ListState length");
    auto result = ElementList::from_serialized({reinterpret_cast<const char*>(buf.current()), size_t(length)});
    buf.skip(size_t(length));
    return result;
}

template <>
inline InnerMapTimeWindow read_flink_value<InnerMapTimeWindow>(ReadBuffer& buf, const TypeLayout&) {
    int32_t length = buf.read_int();
    if (length < 4) return {};
    int32_t count = buf.read_int();
    if (count < 0 || int64_t(count) * 32 + 4 != length)
        throw std::runtime_error("Invalid TimeWindow map payload size");
    InnerMapTimeWindow result;
    result.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) {
        int64_t start = buf.read_long(), end = buf.read_long();
        int64_t value_start = buf.read_long(), value_end = buf.read_long();
        result.emplace(TimeWindow(start, end), TimeWindow(value_start, value_end));
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
        NamespaceBlob uk, uv;
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

// The checkpoint bytes are also readable by an ordinary serialized map.
template<class Map>
inline Map read_inline_map(ReadBuffer& buf, const TypeLayout& layout) {
    const int32_t length = buf.read_int();
    if (length < 4 || static_cast<size_t>(length) > buf.remaining() || layout.children.size() != 2) {
        throw std::runtime_error("Invalid serialized map checkpoint");
    }
    const int32_t width = layout.children[1]->serialized_fixed_size;
    if (width < 1 || width > static_cast<int32_t>(InlineMapValue::capacity)) {
        throw std::runtime_error("Invalid fixed map value width");
    }
    ReadBuffer payload(buf.current(), static_cast<size_t>(length));
    const int32_t count = payload.read_int();
    if (count < 0 || count > (length - 4) / width) throw std::runtime_error("Invalid map entry count");
    Map result;
    for (int32_t i = 0; i < count; ++i) {
        const auto* key = payload.current();
        payload.skip_typed_value(*layout.children[0]);
        auto& value = result[NamespaceBlob(reinterpret_cast<const char*>(key), payload.current() - key)];
        const auto* data = payload.current();
        payload.skip(width);
        value.resize(width);
        std::memcpy(value.data(), data, width);
    }
    if (payload.remaining()) throw std::runtime_error("Unexpected bytes after map entries");
    buf.skip(length);
    return result;
}

template <>
inline InnerMapInline read_flink_value<InnerMapInline>(ReadBuffer& buf, const TypeLayout& layout) {
    return read_inline_map<InnerMapInline>(buf, layout);
}

template <>
inline InnerMapRowFour read_flink_value<InnerMapRowFour>(ReadBuffer& buf, const TypeLayout& layout) {
    return read_inline_map<InnerMapRowFour>(buf, layout);
}

template <>
inline InnerMapWideRowFour read_flink_value<InnerMapWideRowFour>(ReadBuffer& buf, const TypeLayout& layout) {
    return read_inline_map<InnerMapWideRowFour>(buf, layout);
}

// Typed InnerMap reader variants.

template <>
inline InnerMapLongLong read_flink_value<InnerMapLongLong>(ReadBuffer& buf, const TypeLayout&) {
    int32_t total_bytes = buf.read_int();
    if (total_bytes < 4) return {};
    InnerMapLongLong result;
    int32_t count = buf.read_int();
    for (int32_t i = 0; i < count; ++i) {
        int64_t uk = buf.read_long();
        int64_t uv = buf.read_long();
        result[uk] = uv;
    }
    return result;
}

template <>
inline InnerMapLongString read_flink_value<InnerMapLongString>(ReadBuffer& buf, const TypeLayout& layout) {
    int32_t total_bytes = buf.read_int();
    if (total_bytes < 4) return {};
    InnerMapLongString result;
    int32_t count = buf.read_int();
    const TypeLayout* uv_layout = nullptr;
    if (layout.type_id == TypeId::MAP && layout.children.size() >= 2) {
        uv_layout = layout.children[1].get();
    }
    for (int32_t i = 0; i < count; ++i) {
        int64_t uk = buf.read_long();
        NamespaceBlob uv;
        if (uv_layout) {
            uv = buf.read_element_bytes(*uv_layout);
        }
        result[uk] = std::move(uv);
    }
    return result;
}

template <>
inline InnerMapStringLong read_flink_value<InnerMapStringLong>(ReadBuffer& buf, const TypeLayout& layout) {
    int32_t total_bytes = buf.read_int();
    if (total_bytes < 4) return {};
    InnerMapStringLong result;
    int32_t count = buf.read_int();
    const TypeLayout* uk_layout = nullptr;
    if (layout.type_id == TypeId::MAP && layout.children.size() >= 2) {
        uk_layout = layout.children[0].get();
    }
    for (int32_t i = 0; i < count; ++i) {
        NamespaceBlob uk;
        if (uk_layout) {
            uk = buf.read_element_bytes(*uk_layout);
        }
        int64_t uv = buf.read_long();
        result[std::move(uk)] = uv;
    }
    return result;
}

// Skip VoidNamespace bytes (1 byte: 0)
inline void skip_void_namespace(ReadBuffer& buf) {
    buf.skip(1);
}

// Read TimeWindow namespace: [ns_start(8B BE)][ns_end(8B BE)]
inline TimeWindow read_time_window_namespace(ReadBuffer& buf) {
    int64_t start = buf.read_long();
    int64_t end = buf.read_long();
    return TimeWindow(start, end);
}

// FixedRow: read from BinaryRowData format (length-prefixed).
// Format: [sizeInBytes (4B BE)][header (8B)][field0 (8B LE)][field1 (8B LE)]...
template<class Row>
inline Row read_fixed_row(ReadBuffer& buf, Row row) {
    if (buf.read_int() != 8 + row.arity * 8) throw std::runtime_error("Invalid fixed row key length");
    row.header = buf.read_byte();
    row.header |= static_cast<uint16_t>(buf.read_byte()) << 8;
    buf.skip(6);
    for (uint8_t i = 0; i < row.arity; ++i) {
        uint64_t word = 0;
        for (int b = 0; b < 8; ++b) word |= static_cast<uint64_t>(buf.read_byte()) << (b * 8);
        row.f[i] = static_cast<int64_t>(word);
    }
    return row;
}

template <>
inline FixedRow read_flink_value<FixedRow>(ReadBuffer& buf, const TypeLayout& layout) {
    if (layout.fixed_row_arity > FIXED_ROW_MAX_ARITY) throw std::runtime_error("Unsupported fixed row key arity");
    return read_fixed_row(buf, FixedRow(static_cast<uint8_t>(layout.fixed_row_arity)));
}

template <>
inline FixedPair read_flink_value<FixedPair>(ReadBuffer& buf, const TypeLayout&) {
    return read_fixed_row(buf, FixedPair{});
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

    // Read entries with TimeWindow namespaces: reads [ns_start][ns_end]
    // and inserts into the namespace-aware table.
    void read_entries_tw(ReadBuffer& buf,
                         StateTable<K, V>& table,
                         int key_group,
                         int32_t entry_count) {
        for (int32_t i = 0; i < entry_count; ++i) {
            TimeWindow tw = read_time_window_namespace(buf);
            K key = read_flink_value<K>(buf, key_layout_);
            V value = read_flink_value<V>(buf, value_layout_);
            table.template put<TimeWindow>(key_group, tw, key, std::move(value));
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
