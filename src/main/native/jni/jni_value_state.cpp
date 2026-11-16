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

extern "C" {

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
        // L0 hot-key cache fast-path (Phase A).
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(k, &cached)) {
                return static_cast<jlong>(cached);
            }
        }
        auto* table = handle->table<int64_t, int64_t>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (val && handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, *val);  // backfill on miss
        }
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
        // Write-through: SwissTable is the source of truth, then update cache.
        cached_put(handle, table, keyGroup, k, v);
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, v);
        }
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
        bool modified = cached_modify(handle, table, keyGroup, k, [&](int64_t& existing) {
            existing += d;
            updated = existing;
        });
        if (!modified) {
            cached_put(handle, table, keyGroup, k, d);
        }
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, updated);
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
            cached_remove(handle, handle->table<int64_t, double>(), keyGroup, k);
            if (handle->hot_cache_ll) handle->hot_cache_ll->invalidate(k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            cached_remove(handle, handle->table<int64_t, std::string>(), keyGroup, k);
        } else if (vt == StateHandle::ValueType::LIST) {
            cached_remove(handle, handle->table<int64_t, ElementList>(), keyGroup, k);
        } else if (vt == StateHandle::ValueType::MAP) {
            cached_remove(handle, handle->table<int64_t, InnerMap>(), keyGroup, k);
        } else {
            cached_remove(handle, handle->table<int64_t, int64_t>(), keyGroup, k);
            if (handle->hot_cache_ll) handle->hot_cache_ll->invalidate(k);
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
        auto val = cached_get(handle, table, keyGroup, static_cast<int64_t>(key));
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
        cached_put(handle, table, keyGroup, static_cast<int64_t>(key), static_cast<int64_t>(value));
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
        if (handle->hot_cache_ll) {
            int64_t bits;
            if (handle->hot_cache_ll->get(k, &bits)) {
                return hotcache_val_to_double(bits);
            }
        }
        auto* table = handle->table<int64_t, double>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (val && handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, hotcache_val_from_double(*val));
        }
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
        cached_put(handle, table, keyGroup, k, v);
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, hotcache_val_from_double(v));
        }
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
        auto* table = handle->table<int64_t, std::string>();
        auto val = cached_get(handle, table, keyGroup, static_cast<int64_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongString(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, std::string>();
        std::string val = jbytearray_to_string(env, value);
        cached_put(handle, table, keyGroup, static_cast<int64_t>(key), std::move(val));
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
        int64_t kc = hotcache_key_from_i32(k);
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(kc, &cached)) {
                return static_cast<jlong>(cached);
            }
        }
        auto* table = handle->table<int32_t, int64_t>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (val && handle->hot_cache_ll) {
            handle->hot_cache_ll->put(kc, *val);
        }
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
        cached_put(handle, table, keyGroup, k, v);
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_key_from_i32(k), v);
        }
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
        bool modified = cached_modify(handle, table, keyGroup, k, [&](int64_t& existing) {
            existing += d;
            updated = existing;
        });
        if (!modified) {
            cached_put(handle, table, keyGroup, k, d);
        }
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_key_from_i32(k), updated);
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
        int64_t kc = hotcache_key_from_i32(k);
        if (handle->hot_cache_ll) {
            int64_t bits;
            if (handle->hot_cache_ll->get(kc, &bits)) {
                return hotcache_val_to_double(bits);
            }
        }
        auto* table = handle->table<int32_t, double>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (val && handle->hot_cache_ll) {
            handle->hot_cache_ll->put(kc, hotcache_val_from_double(*val));
        }
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
        cached_put(handle, table, keyGroup, k, v);
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_key_from_i32(k), hotcache_val_from_double(v));
        }
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
        auto* table = handle->table<int32_t, std::string>();
        auto val = cached_get(handle, table, keyGroup, static_cast<int32_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntString(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int32_t, std::string>();
        std::string val = jbytearray_to_string(env, value);
        cached_put(handle, table, keyGroup, static_cast<int32_t>(key), std::move(val));
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
        // BYTES/STRING fallback → <int32_t, std::string>
        auto* table = handle->table<int32_t, std::string>();
        return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
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
            cached_remove(handle, handle->table<int32_t, double>(), keyGroup, k);
            if (handle->hot_cache_ll) handle->hot_cache_ll->invalidate(hotcache_key_from_i32(k));
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            cached_remove(handle, handle->table<int32_t, std::string>(), keyGroup, k);
        } else {
            cached_remove(handle, handle->table<int32_t, int64_t>(), keyGroup, k);
            if (handle->hot_cache_ll) handle->hot_cache_ll->invalidate(hotcache_key_from_i32(k));
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
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        auto val = cached_get(handle, table, keyGroup, k);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string v = jbytearray_to_string(env, value);
        cached_put(handle, table, keyGroup, k, std::move(v));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        cached_remove(handle, table, keyGroup, k);
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
            auto* table = handle->table<int64_t, InnerMap>();
            auto* inner = table->get(keyGroup, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        // BYTES/STRING fallback → <int64_t, std::string>
        auto* table = handle->table<int64_t, std::string>();
        return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContainsGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        std::string k = jbytearray_to_string(env, key);
        auto vt = handle->value_type;

        if (vt == StateHandle::ValueType::LIST) {
            auto* table = handle->table<std::string, ElementList>();
            auto* vec = table->get(keyGroup, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* table = handle->table<std::string, InnerMap>();
            auto* inner = table->get(keyGroup, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        auto* table = handle->table<std::string, std::string>();
        return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

// ============================================================================
//  Helper: convert jlongArray → FixedRow
// ============================================================================

static FixedRow jlongarray_to_fixedrow(JNIEnv* env, jlongArray arr) {
    jsize len = env->GetArrayLength(arr);
    if (len < 0 || len > FIXED_ROW_MAX_ARITY) {
        throw std::runtime_error("FixedRow key arity exceeds native limit");
    }
    FixedRow row(static_cast<uint8_t>(len));
    env->GetLongArrayRegion(arr, 0, len, reinterpret_cast<jlong*>(row.f));
    return row;
}

static inline FixedRow fixedrow2_from_longs(jlong key0, jlong key1) {
    FixedRow row(2);
    row.f[0] = static_cast<int64_t>(key0);
    row.f[1] = static_cast<int64_t>(key1);
    return row;
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
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        if (handle->hot_cache_ll) {
            int64_t folded = hotcache_fold_fixed_row_key(key.f, key.arity);
            int64_t cached;
            if (handle->hot_cache_ll->get(folded, &cached)) {
                return static_cast<jlong>(cached);
            }
            auto* table = handle->table<FixedRow, int64_t>();
            auto val = cached_get(handle, table, keyGroup, key);
            if (!val) return 0;
            handle->hot_cache_ll->put(folded, *val);
            return static_cast<jlong>(*val);
        }
        auto* table = handle->table<FixedRow, int64_t>();
        auto val = cached_get(handle, table, keyGroup, key);
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, int64_t>();
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        cached_put(handle, table, keyGroup, key, static_cast<int64_t>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(
                hotcache_fold_fixed_row_key(key.f, key.arity),
                static_cast<int64_t>(value));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Long(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, int64_t>();
        FixedRow key = fixedrow2_from_longs(key0, key1);
        cached_put(handle, table, keyGroup, key, static_cast<int64_t>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(
                hotcache_fold_fixed_row_key(key.f, key.arity),
                static_cast<int64_t>(value));
        }
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
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        if (handle->hot_cache_ll) {
            int64_t folded = hotcache_fold_fixed_row_key(key.f, key.arity);
            int64_t cached;
            if (handle->hot_cache_ll->get(folded, &cached)) {
                return hotcache_val_to_double(cached);
            }
            auto* table = handle->table<FixedRow, double>();
            auto val = cached_get(handle, table, keyGroup, key);
            if (!val) return 0.0;
            handle->hot_cache_ll->put(folded, hotcache_val_from_double(*val));
            return *val;
        }
        auto* table = handle->table<FixedRow, double>();
        auto val = cached_get(handle, table, keyGroup, key);
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, double>();
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        cached_put(handle, table, keyGroup, key, static_cast<double>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(
                hotcache_fold_fixed_row_key(key.f, key.arity),
                hotcache_val_from_double(static_cast<double>(value)));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Double(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, double>();
        FixedRow key = fixedrow2_from_longs(key0, key1);
        cached_put(handle, table, keyGroup, key, static_cast<double>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(
                hotcache_fold_fixed_row_key(key.f, key.arity),
                hotcache_val_from_double(static_cast<double>(value)));
        }
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
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto val = cached_get(handle, table, keyGroup, key);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRow2Generic(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = fixedrow2_from_longs(key0, key1);
        auto val = cached_get(handle, table, keyGroup, key);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        std::string v = jbytearray_to_string(env, value);
        cached_put(handle, table, keyGroup, key, std::move(v));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRow2Generic(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = fixedrow2_from_longs(key0, key1);
        std::string v = jbytearray_to_string(env, value);
        cached_put(handle, table, keyGroup, key, std::move(v));
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
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto vt = handle->value_type;
        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            auto* table = handle->table<FixedRow, int64_t>();
            return table->get(keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->table<FixedRow, double>();
            return table->get(keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        auto* table = handle->table<FixedRow, std::string>();
        return table->get(keyGroup, key) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearFixedRow(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto vt = handle->value_type;
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->invalidate(
                hotcache_fold_fixed_row_key(key.f, key.arity));
        }
        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            cached_remove(handle, handle->table<FixedRow, int64_t>(), keyGroup, key);
        } else if (vt == StateHandle::ValueType::FLOAT64) {
            cached_remove(handle, handle->table<FixedRow, double>(), keyGroup, key);
        } else {
            cached_remove(handle, handle->table<FixedRow, std::string>(), keyGroup, key);
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearFixedRow2(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key0, jlong key1, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        FixedRow key = fixedrow2_from_longs(key0, key1);
        auto vt = handle->value_type;
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->invalidate(
                hotcache_fold_fixed_row_key(key.f, key.arity));
        }
        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            cached_remove(handle, handle->table<FixedRow, int64_t>(), keyGroup, key);
        } else if (vt == StateHandle::ValueType::FLOAT64) {
            cached_remove(handle, handle->table<FixedRow, double>(), keyGroup, key);
        } else {
            cached_remove(handle, handle->table<FixedRow, std::string>(), keyGroup, key);
        }
    })
}

// ============================================================================
//  Zero-copy results use the handle area and optionally copy to jlongArray out
//  Returns JNI_TRUE if value found, JNI_FALSE if not.
//  The returned pointer is std::string::data() within the SwissTable — valid
//  until the next mutation on the same key.
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongStringPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, std::string>();
        auto val = cached_get(handle, table, keyGroup, static_cast<int64_t>(key));
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
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        auto val = cached_get(handle, table, keyGroup, k);
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
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(k, &cached)) {
                jlong out_val = static_cast<jlong>(cached);
                set_long_result(env, handle, out_val, out);
                return JNI_TRUE;
            }
        }
        auto* table = handle->table<int64_t, int64_t>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (!val) return JNI_FALSE;
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(k, *val);
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
        if (handle->hot_cache_ll) {
            int64_t bits;
            if (handle->hot_cache_ll->get(k, &bits)) {
                jlong jbits = static_cast<jlong>(bits);
                set_long_result(env, handle, jbits, out);
                return JNI_TRUE;
            }
        }
        auto* table = handle->table<int64_t, double>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(k, static_cast<int64_t>(bits));
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
        int64_t kc = hotcache_key_from_i32(k);
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(kc, &cached)) {
                jlong out_val = static_cast<jlong>(cached);
                set_long_result(env, handle, out_val, out);
                return JNI_TRUE;
            }
        }
        auto* table = handle->table<int32_t, int64_t>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (!val) return JNI_FALSE;
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(kc, *val);
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
        int64_t kc = hotcache_key_from_i32(k);
        if (handle->hot_cache_ll) {
            int64_t bits;
            if (handle->hot_cache_ll->get(kc, &bits)) {
                jlong jbits = static_cast<jlong>(bits);
                set_long_result(env, handle, jbits, out);
                return JNI_TRUE;
            }
        }
        auto* table = handle->table<int32_t, double>();
        auto val = cached_get(handle, table, keyGroup, k);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(kc, static_cast<int64_t>(bits));
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
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto* table = handle->table<FixedRow, int64_t>();
        auto val = cached_get(handle, table, keyGroup, key);
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
        FixedRow key = fixedrow2_from_longs(key0, key1);
        auto* table = handle->table<FixedRow, int64_t>();
        auto val = cached_get(handle, table, keyGroup, key);
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
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto* table = handle->table<FixedRow, double>();
        auto val = cached_get(handle, table, keyGroup, key);
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
        FixedRow key = fixedrow2_from_longs(key0, key1);
        auto* table = handle->table<FixedRow, double>();
        auto val = cached_get(handle, table, keyGroup, key);
        if (!val) return JNI_FALSE;
        jlong bits;
        memcpy(&bits, &*val, sizeof(jlong));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

// FixedRow key + string value → <FixedRow, std::string>, zero-copy pointer return
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetFixedRowGenericPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        auto val = cached_get(handle, table, keyGroup, key);
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
        auto* table = handle->table<FixedRow, std::string>();
        FixedRow key = fixedrow2_from_longs(key0, key1);
        auto val = cached_get(handle, table, keyGroup, key);
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
        auto val = cached_get(handle, table, keyGroup, static_cast<int64_t>(key));
        if (val) {
            set_long_result(env, handle, *val, out);
            return JNI_TRUE;
        }
        cached_put(handle, table, keyGroup, static_cast<int64_t>(key), static_cast<int64_t>(newValue));
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
        auto val = cached_get(handle, table, keyGroup, tw, static_cast<int64_t>(key));
        if (val) {
            set_long_result(env, handle, *val, out);
            return JNI_TRUE;
        }
        cached_put(handle, table, keyGroup, tw, static_cast<int64_t>(key), static_cast<int64_t>(newValue));
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
        auto* table = handle->table<int64_t, std::string>();
        int64_t k = static_cast<int64_t>(key);
        std::string* existing = table->get(keyGroup, k);
        std::string nv = jbytearray_to_string(env, newValue);
        if (existing) {
            jbyteArray result = string_to_jbytearray(env, *existing);
            cached_put(handle, table, keyGroup, k, std::move(nv));
            return result;
        }
        cached_put(handle, table, keyGroup, k, std::move(nv));
        return nullptr;
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
        auto* table = handle->table<int64_t, std::string>();
        int64_t k = static_cast<int64_t>(key);
        TimeWindow tw{static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd)};
        std::string* existing = table->get(keyGroup, tw, k);
        std::string nv = jbytearray_to_string(env, newValue);
        if (existing) {
            jbyteArray result = string_to_jbytearray(env, *existing);
            cached_put(handle, table, keyGroup, tw, k, std::move(nv));
            return result;
        }
        cached_put(handle, table, keyGroup, tw, k, std::move(nv));
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
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string* existing = table->get(keyGroup, k);
        std::string nv = jbytearray_to_string(env, newValue);
        if (existing) {
            jbyteArray result = string_to_jbytearray(env, *existing);
            cached_put(handle, table, keyGroup, k, std::move(nv));
            return result;
        }
        cached_put(handle, table, keyGroup, k, std::move(nv));
        return nullptr;
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
        auto* table = handle->table<int64_t, std::string>();
        int64_t k = static_cast<int64_t>(key);
        std::string* existing = table->get(keyGroup, k);
        if (existing) {
            return string_to_jbytearray(env, *existing);
        }
        std::string nv = jbytearray_to_string(env, newValue);
        cached_put(handle, table, keyGroup, k, std::move(nv));
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
        auto* table = handle->table<int64_t, std::string>();
        int64_t k = static_cast<int64_t>(key);
        TimeWindow tw{static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd)};
        std::string* existing = table->get(keyGroup, tw, k);
        if (existing) {
            return string_to_jbytearray(env, *existing);
        }
        std::string nv = jbytearray_to_string(env, newValue);
        cached_put(handle, table, keyGroup, tw, k, std::move(nv));
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
        auto* table = handle->table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string* existing = table->get(keyGroup, k);
        if (existing) {
            return string_to_jbytearray(env, *existing);
        }
        std::string nv = jbytearray_to_string(env, newValue);
        cached_put(handle, table, keyGroup, k, std::move(nv));
        return nullptr;
    })
}

}  // extern "C"
