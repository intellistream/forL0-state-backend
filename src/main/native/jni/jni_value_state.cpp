// jni_value_state.cpp — JNI entry points for ValueState operations.
// Maps to NativeEngine.java native methods: valueGetLongLong, valuePutLongLong, etc.

#include <jni.h>
#include "jni_utils.h"
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        int64_t* val = table->get(keyGroup, static_cast<int64_t>(key));
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        table->put(keyGroup, static_cast<int64_t>(key), static_cast<int64_t>(value));
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
            handle->engine->get_state_table<int64_t, double>(handle->table_id)->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->engine->get_state_table<int64_t, std::string>(handle->table_id)->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::LIST) {
            handle->engine->get_state_table<int64_t, ElementList>(handle->table_id)->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::MAP) {
            handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id)->remove(keyGroup, k);
        } else {
            handle->engine->get_state_table<int64_t, int64_t>(handle->table_id)->remove(keyGroup, k);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        int64_t* val = table->get(keyGroup, static_cast<int64_t>(key));
        return val ? static_cast<jint>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongInt(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jint value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, double>(handle->table_id);
        double* val = table->get(keyGroup, static_cast<int64_t>(key));
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, double>(handle->table_id);
        table->put(keyGroup, static_cast<int64_t>(key), static_cast<double>(value));
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
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        std::string* val = table->get(keyGroup, static_cast<int64_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongString(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        std::string val = jbytearray_to_string(env, value);
        table->put(keyGroup, static_cast<int64_t>(key), std::move(val));
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
        auto* table = handle->engine->get_state_table<int32_t, int64_t>(handle->table_id);
        int64_t* val = table->get(keyGroup, static_cast<int32_t>(key));
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int32_t, int64_t>(handle->table_id);
        table->put(keyGroup, static_cast<int32_t>(key), static_cast<int64_t>(value));
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
        auto* table = handle->engine->get_state_table<int32_t, double>(handle->table_id);
        double* val = table->get(keyGroup, static_cast<int32_t>(key));
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int32_t, double>(handle->table_id);
        table->put(keyGroup, static_cast<int32_t>(key), static_cast<double>(value));
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
        auto* table = handle->engine->get_state_table<int32_t, std::string>(handle->table_id);
        std::string* val = table->get(keyGroup, static_cast<int32_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutIntString(
        JNIEnv* env, jclass,
        jlong stateHandle, jint key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int32_t, std::string>(handle->table_id);
        std::string val = jbytearray_to_string(env, value);
        table->put(keyGroup, static_cast<int32_t>(key), std::move(val));
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
            auto* table = handle->engine->get_state_table<int32_t, int64_t>(handle->table_id);
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->engine->get_state_table<int32_t, double>(handle->table_id);
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        // BYTES/STRING fallback → <int32_t, std::string>
        auto* table = handle->engine->get_state_table<int32_t, std::string>(handle->table_id);
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
            handle->engine->get_state_table<int32_t, double>(handle->table_id)->remove(keyGroup, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->engine->get_state_table<int32_t, std::string>(handle->table_id)->remove(keyGroup, k);
        } else {
            handle->engine->get_state_table<int32_t, int64_t>(handle->table_id)->remove(keyGroup, k);
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
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string* val = table->get(keyGroup, k);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string v = jbytearray_to_string(env, value);
        table->put(keyGroup, k, std::move(v));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        table->remove(keyGroup, k);
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
            auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->engine->get_state_table<int64_t, double>(handle->table_id);
            return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::LIST) {
            auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
            auto* vec = table->get(keyGroup, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
            auto* inner = table->get(keyGroup, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        // BYTES/STRING fallback → <int64_t, std::string>
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
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
            auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
            auto* vec = table->get(keyGroup, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
            auto* inner = table->get(keyGroup, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        return table->get(keyGroup, k) ? JNI_TRUE : JNI_FALSE;
    })
}

// ============================================================================
//  Helper: convert jlongArray → FixedRow
// ============================================================================

static FixedRow jlongarray_to_fixedrow(JNIEnv* env, jlongArray arr) {
    jsize len = env->GetArrayLength(arr);
    FixedRow row(static_cast<uint8_t>(len));
    env->GetLongArrayRegion(arr, 0, len, reinterpret_cast<jlong*>(row.f));
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
        auto* table = handle->engine->get_state_table<FixedRow, int64_t>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        int64_t* val = table->get(keyGroup, key);
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<FixedRow, int64_t>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        table->put(keyGroup, key, static_cast<int64_t>(value));
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
        auto* table = handle->engine->get_state_table<FixedRow, double>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        double* val = table->get(keyGroup, key);
        return val ? *val : 0.0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowDouble(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<FixedRow, double>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        table->put(keyGroup, key, static_cast<double>(value));
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
        auto* table = handle->engine->get_state_table<FixedRow, std::string>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        std::string* val = table->get(keyGroup, key);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutFixedRowGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jlongArray keyFields, jint keyGroup, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<FixedRow, std::string>(handle->table_id);
        FixedRow key = jlongarray_to_fixedrow(env, keyFields);
        std::string v = jbytearray_to_string(env, value);
        table->put(keyGroup, key, std::move(v));
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
            auto* table = handle->engine->get_state_table<FixedRow, int64_t>(handle->table_id);
            return table->get(keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            auto* table = handle->engine->get_state_table<FixedRow, double>(handle->table_id);
            return table->get(keyGroup, key) ? JNI_TRUE : JNI_FALSE;
        }
        auto* table = handle->engine->get_state_table<FixedRow, std::string>(handle->table_id);
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
        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            handle->engine->get_state_table<FixedRow, int64_t>(handle->table_id)->remove(keyGroup, key);
        } else if (vt == StateHandle::ValueType::FLOAT64) {
            handle->engine->get_state_table<FixedRow, double>(handle->table_id)->remove(keyGroup, key);
        } else {
            handle->engine->get_state_table<FixedRow, std::string>(handle->table_id)->remove(keyGroup, key);
        }
    })
}

// ============================================================================
//  Zero-copy pointer return: writes [nativeAddress, size] into jlongArray out
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
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        std::string* val = table->get(keyGroup, static_cast<int64_t>(key));
        if (!val || val->empty()) return JNI_FALSE;
        jlong arr[2] = {
            reinterpret_cast<jlong>(val->data()),
            static_cast<jlong>(val->size())
        };
        env->SetLongArrayRegion(out, 0, 2, arr);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetGenericPtr(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string* val = table->get(keyGroup, k);
        if (!val || val->empty()) return JNI_FALSE;
        jlong arr[2] = {
            reinterpret_cast<jlong>(val->data()),
            static_cast<jlong>(val->size())
        };
        env->SetLongArrayRegion(out, 0, 2, arr);
        return JNI_TRUE;
    })
}

}  // extern "C"
