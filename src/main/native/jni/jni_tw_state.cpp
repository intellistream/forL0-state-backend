// jni_tw_state.cpp — JNI entry points for TimeWindow namespace state operations.
// Maps to NativeEngine.java native methods with *WithTW suffix.
// All operations take (long nsStart, long nsEnd) to construct a TimeWindow namespace.

#include <jni.h>
#include "jni_utils.h"
#include "state_cache_access.h"
#include "list_state_access.h"
#include "native_map_iterator.h"
#include "state_engine.h"
#include "type_layout.h"
#include "flink_binary_format.h"
#include "hot_cache.h"

using namespace forl0;

extern "C" {

// ============================================================================
//  ValueState: long key + long value + TimeWindow ns
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        auto val = get_value(table, keyGroup, tw, k);
        if (!val) return JNI_FALSE;
        jlong v = static_cast<jlong>(*val);
        set_long_result(env, handle, v, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        table->put(keyGroup, tw, k, static_cast<int64_t>(value));
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueAddAndGetLongLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        int64_t d = static_cast<int64_t>(delta);
        int64_t updated = d;
        bool modified = table->modify_in_place(keyGroup, tw, k, [&](int64_t& existing) {
            existing += d;
            updated = existing;
        });
        if (!modified) {
            table->put(keyGroup, tw, k, d);
        }
        return static_cast<jlong>(updated);
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueContainsWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        auto vt = handle->value_type;

        if (vt == StateHandle::ValueType::INT64 || vt == StateHandle::ValueType::INT32) {
            return handle->table<int64_t, int64_t>()
                ->get(keyGroup, tw, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            return handle->table<int64_t, double>()
                ->get(keyGroup, tw, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::LIST) {
            auto* vec = handle->table<int64_t, ElementList>()
                ->get(keyGroup, tw, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* inner = handle->table<int64_t, InnerMap>()
                ->get(keyGroup, tw, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        return handle->table<int64_t, NamespaceBlob>()
            ->get(keyGroup, tw, k) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        auto vt = handle->value_type;

        if (vt == StateHandle::ValueType::FLOAT64) {
            handle->table<int64_t, double>()->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->table<int64_t, NamespaceBlob>()->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::LIST) {
            handle->table<int64_t, ElementList>()->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::MAP) {
            handle->table<int64_t, InnerMap>()->remove(keyGroup, tw, k);
        } else {
            handle->table<int64_t, int64_t>()->remove(keyGroup, tw, k);
        }
    })
}

// ============================================================================
//  ValueState: long key + String/bytes value + TimeWindow ns
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongStringWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, NamespaceBlob>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        auto val = get_value(table, keyGroup, tw, static_cast<int64_t>(key));
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

// OPT-10: Zero-copy native pointer access for TimeWindow namespace (RowData accumulators).
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongStringPtrWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, NamespaceBlob>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        auto val = get_value(table, keyGroup, tw, static_cast<int64_t>(key));
        if (!val || val->empty()) return JNI_FALSE;
        set_blob_result(env, handle, *val, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongStringWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, NamespaceBlob>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob val = jbytearray_to_string(env, value);
        table->put(keyGroup, tw, static_cast<int64_t>(key), std::move(val));
    })
}

// ============================================================================
//  ValueState: long key + double value + TimeWindow ns
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetLongDoubleWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        auto* table = handle->table<int64_t, double>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        auto val = get_value(table, keyGroup, tw, k);
        if (!val) return JNI_FALSE;
        // Encode double as raw bits in long
        jlong bits;
        memcpy(&bits, &*val, sizeof(double));
        set_long_result(env, handle, bits, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutLongDoubleWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jdouble value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, double>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        table->put(keyGroup, tw, k, static_cast<double>(value));
    })
}

// ============================================================================
//  ReducingState: long key + long value + TimeWindow ns
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t* val = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!val) return JNI_FALSE;
        jlong v = static_cast<jlong>(*val);
        set_long_result(env, handle, v, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        int64_t v = static_cast<int64_t>(value);
        int64_t updated = v;
        bool modified = table->modify_in_place(keyGroup, tw, k, [&](int64_t& existing) {
            switch (builtinAggType) {
                case 0: existing += v; break;  // SUM
                case 1: existing = std::min(existing, v); break;  // MIN
                case 2: existing = std::max(existing, v); break;  // MAX
                default:
                    throw std::runtime_error("User-defined reduce requires Java callback");
            }
            updated = existing;
        });
        if (!modified) {
            table->put(keyGroup, tw, k, v);
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceClearWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        table->remove(keyGroup, tw, static_cast<int64_t>(key));
    })
}

// ============================================================================
//  ReducingState / ValueState: bytes/String key + long value + TimeWindow ns
//  BYTES_TW fast path: avoids embedding namespace in serialized key bytes.
// ============================================================================

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetBytesLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<NamespaceBlob, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob k = jbytearray_to_string(env, key);
        int64_t* val = table->get(keyGroup, tw, k);
        if (!val) return JNI_FALSE;
        jlong v = static_cast<jlong>(*val);
        set_long_result(env, handle, v, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddBytesLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<NamespaceBlob, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob k = jbytearray_to_string(env, key);
        int64_t v = static_cast<int64_t>(value);
        bool modified = table->modify_in_place(keyGroup, tw, k, [&](int64_t& existing) {
            switch (builtinAggType) {
                case 0: existing += v; break;  // SUM
                case 1: existing = std::min(existing, v); break;  // MIN
                case 2: existing = std::max(existing, v); break;  // MAX
                default:
                    throw std::runtime_error("User-defined reduce requires Java callback");
            }
        });
        if (!modified) {
            table->put(keyGroup, tw, std::move(k), v);
        }
    })
}

// ============================================================================
//  Batch variant: adds the same value to multiple TimeWindows in ONE JNI crossing.
//  nsArray is interleaved [start0, end0, start1, end1, ...].
//  This amortizes JNI overhead for sliding window scenarios (25 windows → 1 crossing).
// ============================================================================
JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddBytesLongWithTWBatch(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlongArray nsArray, jint windowCount, jlong value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<NamespaceBlob, int64_t>();
        NamespaceBlob k = jbytearray_to_string(env, key);
        int64_t v = static_cast<int64_t>(value);

        // Read all namespace pairs from the Java array at once (minimizes JNI access).
        const int count = static_cast<int>(windowCount);
        jlong* nsPtr = env->GetLongArrayElements(nsArray, nullptr);

        for (int i = 0; i < count; ++i) {
            TimeWindow tw(static_cast<int64_t>(nsPtr[i * 2]),
                          static_cast<int64_t>(nsPtr[i * 2 + 1]));
            bool modified = table->modify_in_place(keyGroup, tw, k, [&](int64_t& existing) {
                switch (builtinAggType) {
                    case 0: existing += v; break;
                    case 1: existing = std::min(existing, v); break;
                    case 2: existing = std::max(existing, v); break;
                    default: break;
                }
            });
            if (!modified) {
                // First insert for this window: copy the key string.
                table->put(keyGroup, tw, k, v);
            }
        }

        env->ReleaseLongArrayElements(nsArray, nsPtr, JNI_ABORT);
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetAndPutBytesLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong newValue, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<NamespaceBlob, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob k = jbytearray_to_string(env, key);
        int64_t* val = table->get(keyGroup, tw, k);
        if (val) {
            jlong v = static_cast<jlong>(*val);
            set_long_result(env, handle, v, out);
            return JNI_TRUE;
        }
        table->put(keyGroup, tw, std::move(k), static_cast<int64_t>(newValue));
        return JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutBytesLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<NamespaceBlob, int64_t>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob k = jbytearray_to_string(env, key);
        table->put(keyGroup, tw, std::move(k), static_cast<int64_t>(value));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearBytesWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        NamespaceBlob k = jbytearray_to_string(env, key);
        auto vt = handle->value_type;
        if (vt == StateHandle::ValueType::INT64) {
            handle->table<NamespaceBlob, int64_t>()
                ->remove(keyGroup, tw, k);
        } else {
            handle->table<NamespaceBlob, NamespaceBlob>()
                ->remove(keyGroup, tw, k);
        }
    })
}

// ============================================================================
//  ListState: long key + TimeWindow ns
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGetWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        return list_get(env, table, keyGroup, tw, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        NamespaceBlob elem = jbytearray_to_string(env, element);

        list_add(table, std::move(elem), keyGroup, tw, k);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listClearWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        list_clear(table, keyGroup, tw, static_cast<int64_t>(key));
    })
}

// ============================================================================
//  MapState: long key + TimeWindow ns (generic UK/UV via bytes)
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        NamespaceBlob uk = jbytearray_to_string(env, userKey);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        const auto* value = inner->find_value(uk);
        return value ? string_to_jbytearray(env, *value) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        NamespaceBlob uk = jbytearray_to_string(env, userKey);
        NamespaceBlob uv = jbytearray_to_string(env, userValue);
        if (!table->modify_in_place(keyGroup, tw, k, [&](InnerMap& m) {
            m[std::move(uk)] = std::move(uv);
    })) {
            InnerMap new_map;
            new_map[std::move(uk)] = std::move(uv);
            table->put(keyGroup, tw, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        NamespaceBlob uk = jbytearray_to_string(env, userKey);
        table->modify_or_remove_in_place(keyGroup, tw, k, [&](InnerMap& m) {
            m.erase(uk);
        });
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        NamespaceBlob uk = jbytearray_to_string(env, userKey);
        return (inner->find_value(uk)) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIsEmptyWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jboolean, JNI_TRUE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        size_t encoded_size = 4;
        for (const auto& entry : *inner) encoded_size += entry.first.size() + entry.second.size();
        WriteBuffer buf(encoded_size);
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapClearWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        table->remove(keyGroup, tw, static_cast<int64_t>(key));
    })
}

// ============================================================================
//  MapState streaming iterator for TimeWindow namespace
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorCreateWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, InnerMap>();
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return 0;
        auto* iter = new NativeMapIterator(*inner);
        return to_handle(iter);
    })
}

}  // extern "C"
