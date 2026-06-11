// jni_tw_state.cpp — JNI entry points for TimeWindow namespace state operations.
// Maps to NativeEngine.java native methods with *WithTW suffix.
// All operations take (long nsStart, long nsEnd) to construct a TimeWindow namespace.

#include <jni.h>
#include "jni_utils.h"
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
        int64_t folded = hotcache_fold_tw_key(k, nsStart, nsEnd);
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(folded, &cached)) {
                env->SetLongArrayRegion(out, 0, 1, reinterpret_cast<jlong*>(&cached));
                return JNI_TRUE;
            }
        }
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t* val = table->get(keyGroup, tw, k);
        if (!val) return JNI_FALSE;
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(folded, *val);
        jlong v = static_cast<jlong>(*val);
        env->SetLongArrayRegion(out, 0, 1, &v);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        table->put(keyGroup, tw, k, static_cast<int64_t>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_fold_tw_key(k, nsStart, nsEnd),
                                      static_cast<int64_t>(value));
        }
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueAddAndGetLongLongWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
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
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_fold_tw_key(k, nsStart, nsEnd), updated);
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
            return handle->engine->get_state_table<int64_t, int64_t>(handle->table_id)
                ->get(keyGroup, tw, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            return handle->engine->get_state_table<int64_t, double>(handle->table_id)
                ->get(keyGroup, tw, k) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::LIST) {
            auto* vec = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id)
                ->get(keyGroup, tw, k);
            return (vec && !vec->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* inner = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id)
                ->get(keyGroup, tw, k);
            return (inner && !inner->empty()) ? JNI_TRUE : JNI_FALSE;
        }
        return handle->engine->get_state_table<int64_t, std::string>(handle->table_id)
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
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->invalidate(hotcache_fold_tw_key(k, nsStart, nsEnd));
        }

        if (vt == StateHandle::ValueType::FLOAT64) {
            handle->engine->get_state_table<int64_t, double>(handle->table_id)->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING) {
            handle->engine->get_state_table<int64_t, std::string>(handle->table_id)->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::LIST) {
            handle->engine->get_state_table<int64_t, ElementList>(handle->table_id)->remove(keyGroup, tw, k);
        } else if (vt == StateHandle::ValueType::MAP) {
            handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id)->remove(keyGroup, tw, k);
        } else {
            handle->engine->get_state_table<int64_t, int64_t>(handle->table_id)->remove(keyGroup, tw, k);
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
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        std::string* val = table->get(keyGroup, tw, static_cast<int64_t>(key));
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
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        std::string* val = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!val || val->empty()) return JNI_FALSE;
        jlong arr[2] = {
            reinterpret_cast<jlong>(val->data()),
            static_cast<jlong>(val->size())
        };
        env->SetLongArrayRegion(out, 0, 2, arr);
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
        auto* table = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        std::string val = jbytearray_to_string(env, value);
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
        int64_t folded = hotcache_fold_tw_key(k, nsStart, nsEnd);
        if (handle->hot_cache_ll) {
            int64_t cached;
            if (handle->hot_cache_ll->get(folded, &cached)) {
                env->SetLongArrayRegion(out, 0, 1, reinterpret_cast<jlong*>(&cached));
                return JNI_TRUE;
            }
        }
        auto* table = handle->engine->get_state_table<int64_t, double>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        double* val = table->get(keyGroup, tw, k);
        if (!val) return JNI_FALSE;
        // Encode double as raw bits in long
        jlong bits;
        memcpy(&bits, val, sizeof(double));
        if (handle->hot_cache_ll) handle->hot_cache_ll->put(folded, static_cast<int64_t>(bits));
        env->SetLongArrayRegion(out, 0, 1, &bits);
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
        auto* table = handle->engine->get_state_table<int64_t, double>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        table->put(keyGroup, tw, k, static_cast<double>(value));
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_fold_tw_key(k, nsStart, nsEnd),
                                      hotcache_val_from_double(static_cast<double>(value)));
        }
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t* val = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!val) return JNI_FALSE;
        jlong v = static_cast<jlong>(*val);
        env->SetLongArrayRegion(out, 0, 1, &v);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
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
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(hotcache_fold_tw_key(k, nsStart, nsEnd), updated);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        table->remove(keyGroup, tw, static_cast<int64_t>(key));
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        ElementList* vec = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!vec || vec->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(vec->size()));
        for (const auto& elem : *vec) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(elem.data()), elem.size());
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        std::string elem = jbytearray_to_string(env, element);

        if (!table->modify_in_place(keyGroup, tw, k, [&](ElementList& v) {
            v.push_back(std::move(elem));
        })) {
            ElementList new_vec;
            new_vec.push_back(std::move(elem));
            table->put(keyGroup, tw, k, std::move(new_vec));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listClearWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        table->remove(keyGroup, tw, static_cast<int64_t>(key));
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        std::string uk = jbytearray_to_string(env, userKey);
        auto it = inner->find(uk);
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        std::string uk = jbytearray_to_string(env, userKey);
        return (inner->find(uk) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        table->remove(keyGroup, tw, static_cast<int64_t>(key));
    })
}

// ============================================================================
//  MapState streaming iterator for TimeWindow namespace
// ============================================================================

namespace {
struct NativeMapIterator {
    InnerMap::const_iterator current;
    InnerMap::const_iterator end;
};
}  // namespace

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorCreateWithTW(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        TimeWindow tw(static_cast<int64_t>(nsStart), static_cast<int64_t>(nsEnd));
        InnerMap* inner = table->get(keyGroup, tw, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return 0;
        auto* iter = new NativeMapIterator{inner->cbegin(), inner->cend()};
        return to_handle(iter);
    })
}

}  // extern "C"
