// jni_value_state.cpp — JNI entry points for ValueState operations.
// Maps to NativeEngine.java native methods: valueGetLongLong, valuePutLongLong, etc.

#include <jni.h>
#include <cstring>
#include <stdexcept>
#include "jni_utils.h"
#include "state_cache_access.h"
#include "state_engine.h"
#include "type_layout.h"

using namespace forl0;

namespace {

template<class Lookup>
decltype(auto) with_window_blob_key(StateHandle* handle, std::string_view encoded, Lookup&& lookup) {
    ReadBuffer input(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
    auto key = [&] { return NamespaceBlob(reinterpret_cast<const char*>(input.current()), input.remaining()); };
    const int64_t first = input.read_long();
    if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
        const TimeWindow ns{first, input.read_long()};
        return lookup(ns, key());
    }
    return lookup(first, key());
}

template<class K>
std::optional<std::string_view> get_byte_value(StateHandle* handle, int group, const K& key) {
    return handle->visit_byte_table<K>([&](auto* table) -> std::optional<std::string_view> {
        const auto* value = table->get(group, key);
        if (!value) return std::nullopt;
        return std::string_view(value->data(), value->size());
    });
}

template<class K>
void put_byte_array(JNIEnv* env, StateHandle* handle, int group, const K& key, jbyteArray bytes) {
    if (!bytes) throw std::invalid_argument("missing serialized state value");
    if (handle->root_rows) {
        const auto length = env->GetArrayLength(bytes);
        handle->table<K, NamespaceRow>()->update_or_create(group, key, [&](NamespaceRow& row) {
            row.overwrite(length, [&](char* target) noexcept {
                env->GetByteArrayRegion(bytes, 0, length, reinterpret_cast<jbyte*>(target));
            });
        });
    } else {
        handle->table<K, NamespaceBlob>()->put(group, key, jbytearray_to_string(env, bytes));
    }
}

template<class Fill>
void put_root_row(StateHandle* handle, int group, int64_t first, int64_t second, size_t length, Fill fill) {
    if (!handle->void_namespace || handle->kind != StateHandle::StateKind::VALUE
            || handle->value_type != StateHandle::ValueType::BYTES) throw std::invalid_argument("expected binary root state");
    auto store = [&](const auto& key) {
        using K = std::decay_t<decltype(key)>;
        handle->visit_byte_table<K>([&](auto* table) {
            table->update_or_create(group, key, [&](auto& value) {
                if constexpr (std::is_same_v<std::decay_t<decltype(value)>, NamespaceRow>) value.overwrite(length, fill);
                else { value.resize(length); fill(value.data()); }
            });
        });
    };
    switch (handle->key_type) {
        case StateHandle::KeyType::INT64: return store(first);
        case StateHandle::KeyType::INT32: return store(static_cast<int32_t>(first));
        case StateHandle::KeyType::FIXED_PAIR: return store(FixedPair{{first, second}});
        default: throw std::invalid_argument("unsupported numeric root key");
    }
}

std::optional<std::string_view> get_generic_blob(StateHandle* handle, int group, const NamespaceBlob& key) {
    if (handle->namespace_serialized_keys) {
        return with_window_blob_key(handle, key, [&](const auto& ns, const NamespaceBlob& inner) {
            return get_namespace_blob(handle, group, ns, inner);
        });
    }
    return get_byte_value(handle, group, key);
}

void put_generic_blob(JNIEnv* env, StateHandle* handle, int group, const NamespaceBlob& key, jbyteArray bytes) {
    if (handle->namespace_serialized_keys) {
        const auto value = jbytearray_to_string(env, bytes);
        with_window_blob_key(handle, key, [&](const auto& ns, const NamespaceBlob& inner) {
            handle->visit_window_rows<NamespaceBlob>([&](auto* table) {
                table->update_or_create(group, ns, inner, [&](auto& row) { row.assign(value.data(), value.size()); });
            });
        });
        return;
    }
    put_byte_array(env, handle, group, key, bytes);
}

// Arrays from older callers contain only fields. The current state path can
// append the Flink binary header for nullable keys and changelog row kinds.
FixedRow read_fixed_fields(JNIEnv* env, const StateHandle& handle, jlongArray fields) {
    const jsize length = env->GetArrayLength(fields);
    const jsize arity = handle.key_layout ? handle.key_layout->fixed_row_arity : length;
    if (arity < 0 || arity > FIXED_ROW_MAX_ARITY || (length != arity && length != arity + 1)) {
        throw std::runtime_error("Fixed row field count does not match the state key");
    }
    FixedRow row(static_cast<uint8_t>(arity));
    env->GetLongArrayRegion(fields, 0, arity, reinterpret_cast<jlong*>(row.f));
    if (length != arity) {
        jlong header;
        env->GetLongArrayRegion(fields, arity, 1, &header);
        row.header = static_cast<uint16_t>(header);
    }
    return row;
}

template<class Key, class Visit>
decltype(auto) visit_fixed_key(StateHandle* handle, const Key& key, Visit&& visit) {
    if (handle->key_type == StateHandle::KeyType::FIXED_PAIR) {
        if constexpr (std::is_same_v<Key, FixedPair>) return visit(key);
        else return visit(FixedPair{{key.f[0], key.f[1]}, key.header});
    }
    if (handle->key_type != StateHandle::KeyType::FIXED_ROW) throw std::runtime_error("Expected fixed row key");
    if constexpr (std::is_same_v<Key, FixedRow>) {
        return visit(key);
    } else {
        FixedRow row(key.f, 2);
        row.header = key.header;
        return visit(row);
    }
}

template<class V, class Key>
auto get_fixed_value(StateHandle* handle, int group, const Key& key) {
    return visit_fixed_key(handle, key, [&](const auto& k) {
        if constexpr (std::is_same_v<V, NamespaceBlob>) return get_byte_value(handle, group, k);
        else return get_value(handle->table<std::decay_t<decltype(k)>, V>(), group, k);
    });
}

template<class Key, class V>
void put_fixed_value(StateHandle* handle, int group, const Key& key, V value) {
    visit_fixed_key(handle, key, [&](const auto& k) {
        handle->table<std::decay_t<decltype(k)>, V>()->put(group, k, std::move(value));
    });
}

template<class Key>
void clear_fixed_value(StateHandle* handle, int group, const Key& key) {
    visit_fixed_key(handle, key, [&](const auto& k) {
        using K = std::decay_t<decltype(k)>;
        const auto type = handle->value_type;
        if (type == StateHandle::ValueType::INT64 || type == StateHandle::ValueType::INT32) {
            handle->table<K, int64_t>()->remove(group, k);
        } else if (type == StateHandle::ValueType::FLOAT64) {
            handle->table<K, double>()->remove(group, k);
        } else {
            handle->visit_byte_table<K>([&](auto* table) { table->remove(group, k); });
        }
    });
}

}  // namespace

extern "C" {

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutRootRowRange(JNIEnv* env, jclass, jlong state,
        jint group, jlong first, jlong second, jbyteArray data, jint offset, jint length) {
    JNI_ENTRY_VOID({
        if (!data || offset < 0 || length < 0 || offset > env->GetArrayLength(data) - length)
            throw std::invalid_argument("invalid root row array range");
        put_root_row(from_handle<StateHandle>(state), group, first, second, size_t(length), [&](char* target) noexcept {
            env->GetByteArrayRegion(data, offset, length, reinterpret_cast<jbyte*>(target));
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutRootRowAddress(JNIEnv* env, jclass, jlong state,
        jint group, jlong first, jlong second, jobject owner, jlong address, jint length) {
    JNI_ENTRY_VOID({
        if (!owner || !address || length < 0) throw std::invalid_argument("invalid direct root row");
        // A direct Flink input may be a borrowed row of this engine, including a DRAM row.
        // Read it before COW or promotion can retire its storage.
        std::array<char, 256> local;
        std::string large;
        const auto* source = reinterpret_cast<const char*>(address);
        if (size_t(length) <= local.size()) { std::memcpy(local.data(), source, length); source = local.data(); }
        else { large.assign(source, length); source = large.data(); }
        put_root_row(from_handle<StateHandle>(state), group, first, second, size_t(length), [&](char* target) noexcept {
            std::memcpy(target, source, length);
        });
    })
}

// ============================================================================
//  long key + long value (VoidNamespace) — most common path
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, int64_t>();
        auto val = get_value(table, keyGroup, k);
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        int64_t k = static_cast<int64_t>(key);
        int64_t v = static_cast<int64_t>(value);
        table->put(keyGroup, k, v);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueAddAndGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        int64_t k = static_cast<int64_t>(key);
        int64_t d = static_cast<int64_t>(delta);
        int64_t updated = d;
        bool modified = table->modify_in_place(keyGroup, k, [&](int64_t& existing) {
            existing += d;
            updated = existing;
        });
        if (!modified) {
            table->put(keyGroup, k, d);
        }
        return static_cast<jlong>(updated);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto vt = handle->value_type;
        if (vt == StateHandle::ValueType::FLOAT64) {
            handle->table<int64_t, double>()->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->visit_byte_table<int64_t>([&](auto* table) { table->remove(keyGroup, k); });
        } else if (vt == StateHandle::ValueType::LIST) {
            handle->table<int64_t, ElementList>()->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::MAP) {
            visit_long_key_map(handle, [&](auto& table) { table.remove(keyGroup, k); });
        } else {
            handle->table<int64_t, int64_t>()->remove(keyGroup, k);
        }
    })
}

// ============================================================================
//  long key + int value
// ============================================================================

JNIEXPORT jint JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongInt(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jint, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        auto val = get_value(table, keyGroup, static_cast<int64_t>(key));
        return val ? static_cast<jint>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongInt(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jint value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        table->put(keyGroup, static_cast<int64_t>(key), static_cast<int64_t>(value));
    })
}

// ============================================================================
//  long key + double value
// ============================================================================

JNIEXPORT jdouble JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jdouble, 0.0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, double>();
        auto val = get_value(table, keyGroup, k);
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, double>();
        int64_t k = static_cast<int64_t>(key);
        double v = static_cast<double>(value);
        table->put(keyGroup, k, v);
    })
}

// ============================================================================
//  long key + String value
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongString(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto val = get_byte_value(handle, keyGroup, static_cast<int64_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongString(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        put_byte_array(env, handle, keyGroup, static_cast<int64_t>(key), value);
    })
}

// ============================================================================
//  int key + long value
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetIntLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int32_t k = static_cast<int32_t>(key);
        auto* table = handle->table<int32_t, int64_t>();
        auto val = get_value(table, keyGroup, k);
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int32_t, int64_t>();
        int32_t k = static_cast<int32_t>(key);
        int64_t v = static_cast<int64_t>(value);
        table->put(keyGroup, k, v);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueAddAndGetIntLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int32_t, int64_t>();
        int32_t k = static_cast<int32_t>(key);
        int64_t d = static_cast<int64_t>(delta);
        int64_t updated = d;
        bool modified = table->modify_in_place(keyGroup, k, [&](int64_t& existing) {
            existing += d;
            updated = existing;
        });
        if (!modified) {
            table->put(keyGroup, k, d);
        }
        return static_cast<jlong>(updated);
    })
}

// ============================================================================
//  int key + double value
// ============================================================================

JNIEXPORT jdouble JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetIntDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup) {
    JNI_ENTRY_RETURN(jdouble, 0.0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int32_t k = static_cast<int32_t>(key);
        auto* table = handle->table<int32_t, double>();
        auto val = get_value(table, keyGroup, k);
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int32_t, double>();
        int32_t k = static_cast<int32_t>(key);
        double v = static_cast<double>(value);
        table->put(keyGroup, k, v);
    })
}

// ============================================================================
//  int key + String value
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetIntString(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto val = get_byte_value(handle, keyGroup, static_cast<int32_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntString(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        put_byte_array(env, handle, keyGroup, static_cast<int32_t>(key), value);
    })
}

// ============================================================================
//  int key existence check — dispatch on stored value type
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContainsInt(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto vt = handle->value_type;
        int32_t k = static_cast<int32_t>(key);

        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            auto* table = handle->table<int32_t, int64_t>();
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->table<int32_t, double>();
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        // BYTES/STRING fallback → <int32_t, NamespaceBlob>
        return get_byte_value(handle, keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

// ============================================================================
//  int key clear — dispatch on stored value type
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearInt(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        int32_t k = static_cast<int32_t>(key);
        auto vt = handle->value_type;
        if (vt == StateHandle::ValueType::FLOAT64) {
            handle->table<int32_t, double>()->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->visit_byte_table<int32_t>([&](auto* table) { table->remove(keyGroup, k); });
        } else {
            handle->table<int32_t, int64_t>()->remove(keyGroup, k);
        }
    })
}

// ============================================================================
//  Generic path (serialized byte[] key + value)
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        NamespaceBlob k = jbytearray_to_string(env, key);
        auto val = get_generic_blob(handle, keyGroup, k);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        put_generic_blob(env, handle, keyGroup, jbytearray_to_string(env, key), value);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        NamespaceBlob k = jbytearray_to_string(env, key);
        if (handle->namespace_serialized_keys) {
            with_window_blob_key(handle, k, [&](const auto& ns, const NamespaceBlob& inner) {
                handle->visit_window_rows<NamespaceBlob>([&](auto* table) { table->remove(keyGroup, ns, inner); });
            });
            return;
        }
        handle->visit_byte_table<NamespaceBlob>([&](auto* table) { table->remove(keyGroup, k); });
    })
}

// ============================================================================
//  Existence check — dispatch on stored value type
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContains(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto vt = handle->value_type;
        int64_t k = static_cast<int64_t>(key);

        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            auto* table = handle->table<int64_t, int64_t>();
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->table<int64_t, double>();
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::LIST) {
            auto* table = handle->table<int64_t, ElementList>();
            auto* vec = table->get(keyGroup, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            return visit_long_key_map(handle, [&](auto& table) {
                const auto* inner = table.get(keyGroup, k);
                return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
            });
        }
        // BYTES/STRING fallback → <int64_t, NamespaceBlob>
        return get_byte_value(handle, keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContainsGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        NamespaceBlob k = jbytearray_to_string(env, key);
        if (handle->namespace_serialized_keys) return get_generic_blob(handle, keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        auto vt = handle->value_type;

        if (vt == StateHandle::ValueType::LIST) {
            auto* table = handle->table<NamespaceBlob, ElementList>();
            auto* vec = table->get(keyGroup, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* table = handle->table<NamespaceBlob, InnerMap>();
            auto* inner = table->get(keyGroup, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        return get_byte_value(handle, keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

// ============================================================================
//  FixedRow key + long value
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<int64_t>(handle, keyGroup, key);
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        put_fixed_value(handle, keyGroup, key, static_cast<int64_t>(value));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Long(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        put_fixed_value(handle, keyGroup, key, static_cast<int64_t>(value));
    })
}

// ============================================================================
//  FixedRow key + double value
// ============================================================================

JNIEXPORT jdouble JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_RETURN(jdouble, 0.0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<double>(handle, keyGroup, key);
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        put_fixed_value(handle, keyGroup, key, static_cast<double>(value));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Double(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        put_fixed_value(handle, keyGroup, key, static_cast<double>(value));
    })
}

// ============================================================================
//  FixedRow key + generic (byte[]) value
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<NamespaceBlob>(handle, keyGroup, key);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRow2Generic(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        auto val = get_fixed_value<NamespaceBlob>(handle, keyGroup, key);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        visit_fixed_key(handle, key, [&](const auto& k) { put_byte_array(env, handle, keyGroup, k, value); });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Generic(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        visit_fixed_key(handle, key, [&](const auto& k) { put_byte_array(env, handle, keyGroup, k, value); });
    })
}

// ============================================================================
//  FixedRow key: contains + clear (dispatch on value type)
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContainsFixedRow(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto vt = handle->value_type;
        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            return get_fixed_value<int64_t>(handle, keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            return get_fixed_value<double>(handle, keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        return get_fixed_value<NamespaceBlob>(handle, keyGroup, key) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearFixedRow(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        clear_fixed_value(handle, keyGroup, key);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearFixedRow2(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        clear_fixed_value(handle, keyGroup, key);
    })
}

// ============================================================================
//  Zero-copy results use the handle area and optionally copy to jlongArray out
//  Returns JNI_TRUE if value found, JNI_FALSE if not.
//  The returned pointer is NamespaceBlob::data() within the SwissTable — valid
//  until the next mutation on the same key.
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongStringPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto val = get_byte_value(handle, keyGroup, static_cast<int64_t>(key));
        if (!val || val->empty()) return JNI_FALSE;
        set_blob_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetGenericPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        NamespaceBlob k = jbytearray_to_string(env, key);
        auto val = get_generic_blob(handle, keyGroup, k);
        if (!val || val->empty()) return JNI_FALSE;
        set_blob_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

// ============================================================================
//  Combined get (single JNI call, single hash lookup).
//  Returns JNI_TRUE if found and writes value to out[0]; JNI_FALSE otherwise.
//  For double values, the raw bits are written as jlong (caller uses
//  Double.longBitsToDouble on Java side).
// ============================================================================

// Long key + Long/Int value → <int64_t, int64_t>
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, int64_t>();
        auto val = get_value(table, keyGroup, k);
        if (!val) return JNI_FALSE;
        set_long_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

// Long key + Double value → <int64_t, double>, bit-cast to long
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongDoubleSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, double>();
        auto val = get_value(table, keyGroup, k);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

// Int key + Long/Int value → <int32_t, int64_t>
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetIntLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int32_t k = static_cast<int32_t>(key);
        auto* table = handle->table<int32_t, int64_t>();
        auto val = get_value(table, keyGroup, k);
        if (!val) return JNI_FALSE;
        set_long_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

// Int key + Double value → <int32_t, double>, bit-cast to long
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetIntDoubleSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int32_t k = static_cast<int32_t>(key);
        auto* table = handle->table<int32_t, double>();
        auto val = get_value(table, keyGroup, k);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

// FixedRow key + Long/Int value → <FixedRow, int64_t>
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<int64_t>(handle, keyGroup, key);
        if (!val) return JNI_FALSE;
        set_long_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRow2LongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        auto val = get_fixed_value<int64_t>(handle, keyGroup, key);
        if (!val) return JNI_FALSE;
        set_long_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

// FixedRow key + Double value → <FixedRow, double>, bit-cast to long
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowDoubleSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<double>(handle, keyGroup, key);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRow2DoubleSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        auto val = get_fixed_value<double>(handle, keyGroup, key);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

// FixedRow key + string value → <FixedRow, NamespaceBlob>, zero-copy pointer return
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowGenericPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto key = read_fixed_fields(env, *handle, keyFields);
        auto val = get_fixed_value<NamespaceBlob>(handle, keyGroup, key);
        if (!val || val->empty()) return JNI_FALSE;
        set_blob_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRow2GenericPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const FixedPair key{{key0, key1}};
        auto val = get_fixed_value<NamespaceBlob>(handle, keyGroup, key);
        if (!val || val->empty()) return JNI_FALSE;
        set_blob_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

// ============================================================================
//  ReducingState: combined get-and-put for Long-Long path.
//  If key exists: returns JNI_TRUE, writes old value to out[0], does NOT modify.
//  If key absent: inserts newValue, returns JNI_FALSE.
//  Saves one JNI call on first insert (no separate put needed).
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetAndPutLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong newValue, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        auto val = get_value(table, keyGroup, static_cast<int64_t>(key));
        if (val) {
            set_long_result(env, handle, *val, out);
            return JNI_TRUE;
        }
        table->put(keyGroup, static_cast<int64_t>(key), static_cast<int64_t>(newValue));
        return JNI_FALSE;
    })
}

// TimeWindow namespace variant
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetAndPutLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong newValue, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw{static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd)};
        auto val = get_value(table, keyGroup, tw, static_cast<int64_t>(key));
        if (val) {
            set_long_result(env, handle, *val, out);
            return JNI_TRUE;
        }
        table->put(keyGroup, tw, static_cast<int64_t>(key), static_cast<int64_t>(newValue));
        return JNI_FALSE;
    })
}

// ============================================================================
//  OPT-2: Combined get-and-put for bytes value path (ReducingState/AggregatingState).
//  If key exists: returns old value as byte[], writes newValue to slot.
//  If key absent: writes newValue, returns null.
//  Single SwissTable lookup for the read-modify-write pattern.
// ============================================================================

// int64 key + VoidNamespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetAndPutLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto k = static_cast<int64_t>(key);
        const auto previous = get_byte_value(handle, keyGroup, k);
        jbyteArray result = previous ? string_to_jbytearray(env, *previous) : nullptr;
        put_byte_array(env, handle, keyGroup, k, newValue);
        return result;
    })
}

// int64 key + TimeWindow namespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetAndPutLongBytesWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, NamespaceBlob>();
        int64_t k = static_cast<int64_t>(key);
        TimeWindow tw{static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd)};
        NamespaceBlob* existing = table->get(keyGroup, tw, k);
        NamespaceBlob nv = jbytearray_to_string(env, newValue);
        if (existing) {
            jbyteArray result = string_to_jbytearray(env, *existing);
            table->put(keyGroup, tw, k, std::move(nv));
            return result;
        }
        table->put(keyGroup, tw, k, std::move(nv));
        return nullptr;
    })
}

// generic key (bytes) + VoidNamespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetAndPutGenericBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto k = jbytearray_to_string(env, key);
        const auto previous = get_generic_blob(handle, keyGroup, k);
        jbyteArray result = previous ? string_to_jbytearray(env, *previous) : nullptr;
        put_generic_blob(env, handle, keyGroup, k, newValue);
        return result;
    })
}

// ============================================================================
//  OPT-11: Combined get-or-put for bytes value path.
//  If key exists: returns old value as byte[] and does NOT modify slot.
//  If key absent: writes newValue and returns null.
//  Avoids extra write amplification on hot existing keys.
// ============================================================================

// int64 key + VoidNamespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetOrPutLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto k = static_cast<int64_t>(key);
        const auto previous = get_byte_value(handle, keyGroup, k);
        if (previous) return string_to_jbytearray(env, *previous);
        put_byte_array(env, handle, keyGroup, k, newValue);
        return nullptr;
    })
}

// int64 key + TimeWindow namespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetOrPutLongBytesWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, NamespaceBlob>();
        int64_t k = static_cast<int64_t>(key);
        TimeWindow tw{static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd)};
        NamespaceBlob* existing = table->get(keyGroup, tw, k);
        if (existing) {
            return string_to_jbytearray(env, *existing);
        }
        NamespaceBlob nv = jbytearray_to_string(env, newValue);
        table->put(keyGroup, tw, k, std::move(nv));
        return nullptr;
    })
}

// generic key (bytes) + VoidNamespace
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetOrPutGenericBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray newValue) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        const auto k = jbytearray_to_string(env, key);
        const auto previous = get_generic_blob(handle, keyGroup, k);
        if (previous) return string_to_jbytearray(env, *previous);
        put_generic_blob(env, handle, keyGroup, k, newValue);
        return nullptr;
    })
}

}  // extern "C"
