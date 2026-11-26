// TypeLayout: C++ side type descriptor system.
// Describes how Java types map to C++ native types.
// Generated at registration time from TypeAnalyzer on the Java side,
// serialized as a descriptor byte stream, and parsed here.

#pragma once

#include "allocator.h"
#include "element_list.h"
#include "state_map.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <cstring>

namespace forl0 {

// Type IDs matching the Java-side TypeAnalyzer encoding.
enum class TypeId : uint8_t {
    INT32    = 1,
    INT64    = 2,
    FLOAT32  = 3,
    FLOAT64  = 4,
    BOOL     = 5,
    STRING   = 6,
    BYTES    = 7,   // byte[]
    STRUCT   = 10,
    LIST     = 11,
    MAP      = 12,
    FIXED_ROW = 13, // FixedLengthRow — multi-field all-fixed RowData (int64_t[N])
    VOID_NS  = 20,  // VoidNamespace marker
    TIME_WINDOW = 21, // TimeWindow namespace {start: int64, end: int64}
};

// Field descriptor within a StructLayout.
struct FieldDesc {
    std::string name;
    size_t java_offset;   // Unsafe field offset in the Java object
    size_t cpp_offset;    // Offset within the C++ struct (for interpreted path)
    size_t cpp_size;      // Size of the C++ field
    TypeId type_id;
    // For nested types (struct, list, map), index into the TypeLayout's children vector.
    int32_t child_index;  // -1 if not applicable
};

// TypeLayout describes a single type (primitive, string, struct, container).
struct TypeLayout {
    TypeId type_id;

    // For STRUCT: field descriptors
    std::vector<FieldDesc> fields;

    // For LIST / MAP: element / key-value type layouts (children)
    std::vector<std::unique_ptr<TypeLayout>> children;

    // Total size of the C++ representation (for struct: sizeof the generated struct)
    size_t cpp_size = 0;

    // The Flink TypeSerializer class name (for checkpoint format compatibility).
    std::string serializer_class;

    TypeLayout() : type_id(TypeId::INT32) {}
    explicit TypeLayout(TypeId id) : type_id(id) {}

    bool is_primitive() const {
        return type_id >= TypeId::INT32 && type_id <= TypeId::BOOL;
    }

    bool is_string() const {
        return type_id == TypeId::STRING || type_id == TypeId::BYTES;
    }

    bool is_fixed_size() const {
        return is_primitive();
    }

    // Size of primitive types
    size_t primitive_size() const {
        switch (type_id) {
            case TypeId::BOOL:    return 1;
            case TypeId::INT32:   return 4;
            case TypeId::FLOAT32: return 4;
            case TypeId::INT64:   return 8;
            case TypeId::FLOAT64: return 8;
            default: return 0;
        }
    }

    // For FIXED_ROW: number of fields (arity)
    uint16_t fixed_row_arity = 0;

    // Physical encoding of values serialized by Java inside ListState/MapState.
    // Kept separate from type_id, which selects native table specializations.
    bool serialized_row = false;
    bool wide_serialized_row = false;
    uint8_t fixed_window_row_bytes = 0;
    bool serialized_container = false;
    int32_t serialized_fixed_size = -1;
};

// ============================================================================
//  FixedRow: fixed-length RowData key stored as int64_t array.
//  Supports arity 1-8 (common SQL key shapes).
//  Stored inline in SwissTable slots.
// ============================================================================

static constexpr int FIXED_ROW_MAX_ARITY = 8;

struct FixedRow {
    int64_t f[FIXED_ROW_MAX_ARITY];
    uint8_t arity;
    uint16_t header = 0;  // Flink row kind and up to eight null flags.

    FixedRow() : f{}, arity(0) {}

    explicit FixedRow(uint8_t n) : f{}, arity(n) {}

    FixedRow(const int64_t* fields, uint8_t n) : arity(n) {
        std::memcpy(f, fields, n * sizeof(int64_t));
    }

    bool operator==(const FixedRow& other) const {
        if (arity != other.arity || header != other.header) return false;
        return std::memcmp(f, other.f, arity * sizeof(int64_t)) == 0;
    }

    bool operator!=(const FixedRow& other) const {
        return !(*this == other);
    }
};

struct FixedPair {
    int64_t f[2] = {};
    uint16_t header = 0;
    static constexpr uint8_t arity = 2;

    bool operator==(const FixedPair& other) const noexcept {
        return f[0] == other.f[0] && f[1] == other.f[1] && header == other.header;
    }
};

struct FixedRowHash {
    template<class Row>
    size_t operator()(const Row& row) const noexcept {
        // FNV-1a inspired hash over the fields
        size_t h = 0xcbf29ce484222325ULL;
        for (uint8_t i = 0; i < row.arity; ++i) {
            h ^= static_cast<size_t>(row.f[i]);
            h *= 0x100000001b3ULL;
        }
        return row.header ? (h ^ row.header) * UINT64_C(0x100000001b3) : h;
    }
};

// TimeWindow: {start, end} — both int64_t
using TimeWindow = std::pair<int64_t, int64_t>;

struct TimeWindowHash {
    size_t operator()(const TimeWindow& tw) const {
        const uint64_t h = static_cast<uint64_t>(tw.first);
        return static_cast<size_t>(h ^ (static_cast<uint64_t>(tw.second) + UINT64_C(0x9e3779b97f4a7c15)
                + (h << 6) + (h >> 2)));
    }
};

} // namespace forl0

// std::hash specialization for FixedRow so SwissTable<FixedRow, V> works
// with default template parameters (std::hash<K>).
template <>
struct std::hash<forl0::FixedPair> : forl0::FixedRowHash {};

template <>
struct std::hash<forl0::FixedRow> {
    size_t operator()(const forl0::FixedRow& row) const {
        forl0::FixedRowHash h;
        return h(row);
    }
};

namespace forl0 {

// ============================================================================
//  TypeLayout descriptor parser (from byte stream generated by Java TypeAnalyzer)
// ============================================================================

class TypeLayoutParser {
public:
    static std::unique_ptr<TypeLayout> parse(const uint8_t* data, size_t len) {
        size_t offset = 0;
        return parse_one(data, len, offset);
    }

    // Parse one type layout starting at the given offset (advances offset).
    static std::unique_ptr<TypeLayout> parse_at(const uint8_t* data, size_t len, size_t& offset) {
        return parse_one(data, len, offset);
    }

private:
    static std::unique_ptr<TypeLayout> parse_one(const uint8_t* data, size_t len, size_t& offset) {
        if (offset >= len) {
            throw std::runtime_error("TypeLayout: unexpected end of descriptor");
        }

        auto layout = std::make_unique<TypeLayout>();
        layout->type_id = static_cast<TypeId>(data[offset++]);

        switch (layout->type_id) {
            case TypeId::INT32:
            case TypeId::INT64:
            case TypeId::FLOAT32:
            case TypeId::FLOAT64:
            case TypeId::BOOL:
                layout->cpp_size = layout->primitive_size();
                break;

            case TypeId::STRING:
            case TypeId::BYTES:
                layout->cpp_size = sizeof(std::string);
                break;

            case TypeId::STRUCT: {
                // Read field count (uint16)
                uint16_t field_count = read_u16(data, len, offset);
                layout->fields.resize(field_count);
                size_t cpp_off = 0;
                for (uint16_t i = 0; i < field_count; ++i) {
                    FieldDesc& fd = layout->fields[i];
                    // Read name length (uint16) + name bytes
                    uint16_t name_len = read_u16(data, len, offset);
                    fd.name.assign(reinterpret_cast<const char*>(data + offset), name_len);
                    offset += name_len;
                    // Java unsafe offset (uint32)
                    fd.java_offset = read_u32(data, len, offset);
                    // Field type
                    fd.type_id = static_cast<TypeId>(data[offset++]);
                    // Determine C++ size and child index
                    fd.child_index = -1;
                    if (fd.type_id == TypeId::STRUCT || fd.type_id == TypeId::LIST || fd.type_id == TypeId::MAP) {
                        fd.child_index = static_cast<int32_t>(layout->children.size());
                        layout->children.push_back(parse_one(data, len, offset));
                        fd.cpp_size = layout->children.back()->cpp_size;
                    } else if (fd.type_id == TypeId::STRING || fd.type_id == TypeId::BYTES) {
                        fd.cpp_size = sizeof(std::string);
                    } else {
                        TypeLayout tmp(fd.type_id);
                        fd.cpp_size = tmp.primitive_size();
                    }
                    // Align cpp_offset
                    size_t align = fd.cpp_size >= 8 ? 8 : (fd.cpp_size >= 4 ? 4 : 1);
                    cpp_off = (cpp_off + align - 1) & ~(align - 1);
                    fd.cpp_offset = cpp_off;
                    cpp_off += fd.cpp_size;
                }
                layout->cpp_size = cpp_off;
                break;
            }

            case TypeId::LIST: {
                // Element type follows
                layout->children.push_back(parse_one(data, len, offset));
                layout->cpp_size = sizeof(ElementList);
                break;
            }

            case TypeId::MAP: {
                // Key type, then value type
                layout->children.push_back(parse_one(data, len, offset));
                layout->children.push_back(parse_one(data, len, offset));
                layout->cpp_size = 64;  // approximate flat_hash_map object size
                break;
            }

            case TypeId::FIXED_ROW: {
                // FixedLengthRow: [arity(uint16)]
                layout->fixed_row_arity = read_u16(data, len, offset);
                layout->cpp_size = sizeof(FixedRow);
                break;
            }

            case TypeId::VOID_NS:
                layout->cpp_size = 0;
                break;

            case TypeId::TIME_WINDOW:
                layout->cpp_size = sizeof(TimeWindow);
                break;

            default:
                throw std::runtime_error("TypeLayout: unknown type id");
        }

        return layout;
    }

    static uint16_t read_u16(const uint8_t* data, size_t len, size_t& offset) {
        if (offset + 2 > len) throw std::runtime_error("TypeLayout: truncated u16");
        uint16_t val = (static_cast<uint16_t>(data[offset]) << 8) | data[offset + 1];
        offset += 2;
        return val;
    }

    static uint32_t read_u32(const uint8_t* data, size_t len, size_t& offset) {
        if (offset + 4 > len) throw std::runtime_error("TypeLayout: truncated u32");
        uint32_t val = (static_cast<uint32_t>(data[offset]) << 24)
                     | (static_cast<uint32_t>(data[offset + 1]) << 16)
                     | (static_cast<uint32_t>(data[offset + 2]) << 8)
                     | data[offset + 3];
        offset += 4;
        return val;
    }
};

// ============================================================================
//  Type ID for template dispatch — maps TypeId to C++ types
// ============================================================================

// Tag type for VoidNamespace
struct VoidNamespace {};

// Compile-time mapping from TypeId to C++ type (for high-frequency specializations)
template <TypeId id> struct CppType;
template <> struct CppType<TypeId::INT32>   { using type = int32_t; };
template <> struct CppType<TypeId::INT64>   { using type = int64_t; };
template <> struct CppType<TypeId::FLOAT32> { using type = float; };
template <> struct CppType<TypeId::FLOAT64> { using type = double; };
template <> struct CppType<TypeId::BOOL>    { using type = bool; };
template <> struct CppType<TypeId::STRING>  { using type = std::string; };
template <> struct CppType<TypeId::BYTES>   { using type = std::string; };
template <> struct CppType<TypeId::VOID_NS> { using type = VoidNamespace; };
template <> struct CppType<TypeId::TIME_WINDOW> { using type = TimeWindow; };

// ============================================================================
//  Type aliases for ListState and MapState native storage
// ============================================================================

// MapState stores a nested map of serialized user-key → serialized user-value.
using InnerMap = StateMap<NamespaceBlob, NamespaceBlob>;

// Map serializers with a known short value width can keep their exact Flink
// bytes beside the user key, without storing a string allocator per entry.
class InlineMapValue {
public:
    static constexpr size_t capacity = 15;
    size_t size() const noexcept { return length_; }
    const char* data() const noexcept { return bytes_; }
    char* data() noexcept { return bytes_; }
    operator std::string_view() const noexcept { return {data(), size()}; }

    void resize(size_t n) {
        if (n > capacity) throw std::length_error("Invalid inline map value size");
        length_ = static_cast<uint8_t>(n);
    }

    InlineMapValue& operator=(const NamespaceBlob& bytes) {
        resize(bytes.size());
        std::memcpy(data(), bytes.data(), size());
        return *this;
    }

private:
    char bytes_[capacity]{};
    uint8_t length_ = 0;
};

using InnerMapInline = StateMap<NamespaceBlob, InlineMapValue>;

// Serializer-compatible four-byte values share one small map entry with the
// full key. Larger row keys keep exactly one external owner in their namespace.
class FourByteMapValue {
public:
    void resize(size_t n) {
        if (n != sizeof(bytes_)) throw std::length_error("Invalid four-byte map value");
    }
    FourByteMapValue& operator=(const NamespaceBlob& bytes) {
        resize(bytes.size());
        std::memcpy(bytes_, bytes.data(), sizeof(bytes_));
        return *this;
    }
    const char* data() const noexcept { return bytes_; }
    char* data() noexcept { return bytes_; }
    size_t size() const noexcept { return sizeof(bytes_); }
    operator std::string_view() const noexcept { return {bytes_, sizeof(bytes_)}; }
private:
    char bytes_[4]{};
};

using InnerMapRowFour = StateMap<CompactMapKey, FourByteMapValue>;
using InnerMapWideRowFour = StateMap<WideMapKey, FourByteMapValue>;
static_assert(sizeof(std::pair<CompactMapKey, FourByteMapValue>) == 56);



// Typed InnerMap variants — avoid string encoding for primitive UK/UV.
using InnerMapLongLong   = StateMap<int64_t, int64_t>;
using InnerMapLongString = StateMap<int64_t, NamespaceRow>;
using InnerMapStringLong = StateMap<NamespaceBlob, int64_t>;
using InnerMapTimeWindow = StateMap<TimeWindow, TimeWindow, TimeWindowHash>;

}  // namespace forl0
