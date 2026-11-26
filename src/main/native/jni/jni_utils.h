// JNI utility macros and helpers shared across all JNI entry point files.

#pragma once

#include <jni.h>
#include <cstdint>
#include <string>
#include <stdexcept>

#include "state_engine.h"
#include "type_layout.h"
#include "hot_cache.h"
#include "flink_binary_format.h"

namespace forl0 {

inline void write_window_state_key(WriteBuffer& out, const WindowStateKey& key, bool row_key) {
    if (!row_key) {
        out.write_long(key.number);
        return;
    }
    out.write_int(16);
    for (int byte = 0; byte < 8; ++byte) out.write_byte(uint8_t(key.header >> (8 * byte)));
    const auto number = static_cast<uint64_t>(key.number);
    for (int byte = 0; byte < 8; ++byte) out.write_byte(uint8_t(number >> (8 * byte)));
}

inline void write_window_state_key(WriteBuffer& out, const NamespaceBlob& key, bool) {
    out.write_raw(reinterpret_cast<const uint8_t*>(key.data()), key.size());
}

inline WindowStateKey read_window_state_key(ReadBuffer& in, bool row_key) {
    if (!row_key) return {in.read_long(), 0};
    if (in.read_int() != 16) throw std::runtime_error("invalid numeric RowData key width");
    uint64_t header = 0;
    uint64_t number = 0;
    for (int byte = 0; byte < 8; ++byte) header |= uint64_t(in.read_byte()) << (8 * byte);
    for (int byte = 0; byte < 8; ++byte) number |= uint64_t(in.read_byte()) << (8 * byte);
    return {static_cast<int64_t>(number), header};
}

// ============================================================================
//  Handle casting — C++ pointer ↔ jlong
// ============================================================================

template <typename T>
inline T* from_handle(jlong handle) {
    return reinterpret_cast<T*>(static_cast<uintptr_t>(handle));
}

template <typename T>
inline jlong to_handle(T* ptr) {
    return static_cast<jlong>(reinterpret_cast<uintptr_t>(ptr));
}

// ============================================================================
//  JNI string / byte[] helpers
// ============================================================================

inline NamespaceBlob jbytearray_to_string(JNIEnv* env, jbyteArray arr) {
    if (!arr) return {};
    jsize len = env->GetArrayLength(arr);
    NamespaceBlob s(len, '\0');
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(s.data()));
    return s;
}

inline jbyteArray string_to_jbytearray(JNIEnv* env, std::string_view s) {
    jbyteArray arr = env->NewByteArray(static_cast<jsize>(s.size()));
    if (arr) {
        env->SetByteArrayRegion(arr, 0, static_cast<jsize>(s.size()),
                                reinterpret_cast<const jbyte*>(s.data()));
    }
    return arr;
}

/** Direct buffer→jbyteArray without intermediate std::string copy. */
inline jbyteArray buffer_to_jbytearray(JNIEnv* env, const uint8_t* data, size_t size) {
    jbyteArray arr = env->NewByteArray(static_cast<jsize>(size));
    if (arr) {
        env->SetByteArrayRegion(arr, 0, static_cast<jsize>(size),
                                reinterpret_cast<const jbyte*>(data));
    }
    return arr;
}

inline std::string jstring_to_string(JNIEnv* env, jstring str) {
    if (!str) return {};
    const char* chars = env->GetStringUTFChars(str, nullptr);
    std::string s(chars);
    env->ReleaseStringUTFChars(str, chars);
    return s;
}

// ============================================================================
//  Exception handling — throw Java exceptions from C++
// ============================================================================

inline void throw_java_exception(JNIEnv* env, const char* msg) {
    jclass cls = env->FindClass("java/lang/RuntimeException");
    if (cls) {
        env->ThrowNew(cls, msg);
    }
}

// Macro for JNI functions: wraps body in try/catch, converts C++ exceptions to Java.
// Uses variadic args to handle commas in template parameters (e.g., get_state_table<K, V>).
#define JNI_ENTRY_VOID(...)                \
    try {                                   \
        __VA_ARGS__                         \
    } catch (const std::exception& e) {     \
        throw_java_exception(env, e.what());\
    } catch (...) {                         \
        throw_java_exception(env, "Unknown native error"); \
    }

#define JNI_ENTRY_RETURN(rettype, default_val, ...)   \
    try {                                              \
        __VA_ARGS__                                    \
    } catch (const std::exception& e) {                \
        throw_java_exception(env, e.what());           \
        return default_val;                            \
    } catch (...) {                                    \
        throw_java_exception(env, "Unknown native error"); \
        return default_val;                            \
    }

// ============================================================================
//  StateHandle: wraps a typed StateTable pointer + type metadata
// ============================================================================

// Discriminated union for state handle — allows JNI to route to the correct
// template instantiation without virtual dispatch.
struct StateHandle {
    enum class KeyType : uint8_t { INT32, INT64, STRING, BYTES, FIXED_ROW, FIXED_PAIR };
    enum class ValueType : uint8_t { INT32, INT64, FLOAT32, FLOAT64, BOOL, STRING, BYTES, LIST, MAP };
    enum class StateKind : uint8_t { VALUE, LIST, MAP_, REDUCING, AGGREGATING };
    enum class NsType : uint8_t { VOID_NS, BYTES, TIME_WINDOW, LONG_NS, SQL_WINDOW };
    // MapState InnerMap specialization kind
    enum class MapInnerKind : uint8_t { STRING_STRING, LONG_LONG, LONG_STRING, STRING_LONG, WINDOW_WINDOW, STRING_INLINE, ROW_FOUR, WIDE_ROW_FOUR };

    std::string state_name;
    StateEngine* engine;
    int64_t table_id;       // StateEngine table registration id
    // Registered tables remain stable until engine destruction.
    StateTableHandle* table_handle = nullptr;
    template<class K, class V> StateTable<K, V>* table() const {
        return static_cast<TypedStateTableHandle<K, V>*>(table_handle)->get();
    }
    KeyType key_type;
    ValueType value_type;
    StateKind kind;
    NsType ns_type = NsType::VOID_NS;
    bool void_namespace;
    bool row_data_key = false;
    bool row_data_value = false;
    bool root_rows = false;
    bool namespace_value_tables = false;
    uint8_t window_row_width = 0;
    template<class T> struct RowType { using type = T; };
    template<class Fn> static decltype(auto) dispatch_window_row(uint8_t width, Fn&& fn) {
        if (width == 8) return fn(RowType<FixedWindowRow<8>>{});
        if (width == 16) return fn(RowType<FixedWindowRow<16>>{});
        return fn(RowType<NamespaceRow>{});
    }
    template<class K, class Fn> decltype(auto) visit_window_rows(Fn&& fn) const {
        return dispatch_window_row(window_row_width, [&](auto tag) -> decltype(auto) {
            return fn(table<K, typename decltype(tag)::type>());
        });
    }
    bool numeric_window_lists = false;
    bool namespace_serialized_keys = false;
    MapInnerKind map_inner_kind = MapInnerKind::STRING_STRING;

    template<class K, class Fn> decltype(auto) visit_byte_table(Fn&& fn) const {
        if (root_rows) return fn(table<K, NamespaceRow>());
        return fn(table<K, NamespaceBlob>());
    }

    // Synchronous read results, owned by this handle and accessed only by its
    // task thread. Avoid JNI primitive-array copies for two-number results.
    alignas(8) jlong read_result[2] = {};

    HotCacheLL* cache_backing() const {
        const auto* arena = table_handle->namespace_arena();
        return arena ? arena->backing() : nullptr;
    }

    // Type descriptors (for checkpoint format — used by C++ checkpoint codec).
    // key_layout holds the parsed key type layout (needed for FIXED_ROW arity).
    std::unique_ptr<TypeLayout> key_layout;
    // value_layout holds the COMPLETE parsed tree for the value type.
    // For LIST: value_layout->type_id == LIST, children[0] = element type.
    // For MAP:  value_layout->type_id == MAP, children[0] = user key, children[1] = user value.
    std::unique_ptr<TypeLayout> value_layout;

    // Accessors for child layouts (convenience, avoids null unique_ptr issues).
    const TypeLayout* get_element_layout() const {
        if (value_layout && value_layout->type_id == TypeId::LIST
            && !value_layout->children.empty()) {
            return value_layout->children[0].get();
        }
        return nullptr;
    }

    const TypeLayout* get_user_key_layout() const {
        if (value_layout && value_layout->type_id == TypeId::MAP
            && value_layout->children.size() >= 2) {
            return value_layout->children[0].get();
        }
        return nullptr;
    }

    const TypeLayout* get_user_value_layout() const {
        if (value_layout && value_layout->type_id == TypeId::MAP
            && value_layout->children.size() >= 2) {
            return value_layout->children[1].get();
        }
        return nullptr;
    }
};

template<class Visit>
decltype(auto) visit_serialized_map(StateHandle* handle, Visit&& visit) {
    if (handle->map_inner_kind == StateHandle::MapInnerKind::WIDE_ROW_FOUR) {
        return visit(*handle->table<int64_t, InnerMapWideRowFour>());
    }
    if (handle->map_inner_kind == StateHandle::MapInnerKind::ROW_FOUR) {
        return visit(*handle->table<int64_t, InnerMapRowFour>());
    }
    if (handle->map_inner_kind == StateHandle::MapInnerKind::STRING_INLINE) {
        return visit(*handle->table<int64_t, InnerMapInline>());
    }
    return visit(*handle->table<int64_t, InnerMap>());
}

template<class Visit>
decltype(auto) visit_long_key_map(StateHandle* handle, Visit&& visit) {
    switch (handle->map_inner_kind) {
        case StateHandle::MapInnerKind::WINDOW_WINDOW: return visit(*handle->table<int64_t, InnerMapTimeWindow>());
        case StateHandle::MapInnerKind::LONG_LONG: return visit(*handle->table<int64_t, InnerMapLongLong>());
        case StateHandle::MapInnerKind::LONG_STRING: return visit(*handle->table<int64_t, InnerMapLongString>());
        case StateHandle::MapInnerKind::STRING_LONG: return visit(*handle->table<int64_t, InnerMapStringLong>());
        default: return visit_serialized_map(handle, std::forward<Visit>(visit));
    }
}

inline void check_inline_map_width(const StateHandle& handle, size_t bytes) {
    if ((handle.map_inner_kind == StateHandle::MapInnerKind::STRING_INLINE
            || handle.map_inner_kind == StateHandle::MapInnerKind::ROW_FOUR
            || handle.map_inner_kind == StateHandle::MapInnerKind::WIDE_ROW_FOUR)
            && bytes != static_cast<size_t>(handle.get_user_value_layout()->serialized_fixed_size)) {
        throw std::invalid_argument("Map value does not match its declared serialized width");
    }
}

inline void set_blob_result(JNIEnv* env, StateHandle* handle, std::string_view value, jlongArray out = nullptr) {
    handle->read_result[0] = reinterpret_cast<jlong>(value.data());
    handle->read_result[1] = static_cast<int64_t>(value.size());
    if (out) env->SetLongArrayRegion(out, 0, 2, handle->read_result);
}

inline void set_long_result(JNIEnv* env, StateHandle* handle, jlong value, jlongArray out) {
    handle->read_result[0] = value;
    if (out) env->SetLongArrayRegion(out, 0, 1, handle->read_result);
}

}  // namespace forl0
