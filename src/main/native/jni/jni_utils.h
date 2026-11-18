// JNI utility macros and helpers shared across all JNI entry point files.

#pragma once

#include <jni.h>
#include <cstdint>
#include <string>
#include <stdexcept>

#include "state_engine.h"
#include "type_layout.h"
#include "hot_cache.h"
#include "serialized_hot_cache.h"

namespace forl0 {

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

inline std::string jbytearray_to_string(JNIEnv* env, jbyteArray arr) {
    if (!arr) return {};
    jsize len = env->GetArrayLength(arr);
    std::string s(len, '\0');
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
    enum class KeyType : uint8_t { INT32, INT64, STRING, BYTES, FIXED_ROW };
    enum class ValueType : uint8_t { INT32, INT64, FLOAT32, FLOAT64, BOOL, STRING, BYTES, LIST, MAP };
    enum class StateKind : uint8_t { VALUE, LIST, MAP_, REDUCING, AGGREGATING };
    enum class NsType : uint8_t { VOID_NS, BYTES, TIME_WINDOW, LONG_NS };
    // MapState InnerMap specialization kind
    enum class MapInnerKind : uint8_t { STRING_STRING, LONG_LONG, LONG_STRING, STRING_LONG, WINDOW_WINDOW };

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
    MapInnerKind map_inner_kind = MapInnerKind::STRING_STRING;

    // Synchronous read results, owned by this handle and accessed only by its
    // task thread. Avoid JNI primitive-array copies for two-number results.
    alignas(8) jlong read_result[2] = {};

    // Primitive scalar shortcut. SQL rows, complete composite identities and
    // MapState payloads use serialized_cache instead. Both share the selected
    // L0/DRAM pool accounting and counters.
    HotCacheLL* hot_cache_ll = nullptr;
    std::unique_ptr<SerializedHotCache> serialized_cache;
    HotCacheLL* cache_backing() const {
        return serialized_cache ? serialized_cache->backing() : hot_cache_ll;
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
    TimeWindowMapScratch window_scratch;
};

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
