// jni_map_state.cpp — JNI entry points for MapState operations.
// MapState uses a nested InnerMap (std::unordered_map<std::string, std::string>)
// stored in SwissTable<K, InnerMap>. Each user-key operation directly accesses
// the inner map without serializing/deserializing the entire map.

#include <jni.h>
#include "jni_utils.h"
#include "state_engine.h"
#include "flink_binary_format.h"

using namespace forl0;

extern "C" {

// ============================================================================
//  MapState per-entry operations — int64 key path
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGet(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        std::string uk = jbytearray_to_string(env, userKey);
        auto it = inner->find(uk);
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return nullptr;
        std::string uk = jbytearray_to_string(env, userKey);
        auto it = inner->find(uk);
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPut(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jbyteArray userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        if (!table->modify_in_place(keyGroup, k, [&](InnerMap& m) {
            m[std::move(uk)] = std::move(uv);
        })) {
            InnerMap new_map;
            new_map[std::move(uk)] = std::move(uv);
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jbyteArray userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        if (!table->modify_in_place(keyGroup, k, [&](InnerMap& m) {
            m[std::move(uk)] = std::move(uv);
        })) {
            InnerMap new_map;
            new_map[std::move(uk)] = std::move(uv);
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemove(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        table->modify_or_remove_in_place(keyGroup, k, [&](InnerMap& m) {
            m.erase(uk);
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        std::string uk = jbytearray_to_string(env, userKey);
        table->modify_or_remove_in_place(keyGroup, k, [&](InnerMap& m) {
            m.erase(uk);
        });
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContains(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        std::string uk = jbytearray_to_string(env, userKey);
        return (inner->find(uk) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return JNI_FALSE;
        std::string uk = jbytearray_to_string(env, userKey);
        return (inner->find(uk) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

// ============================================================================
//  MapState bulk operations — serialize inner map entries
//  Output format: [count(4 bytes BE)][uk1_bytes][uv1_bytes][uk2_bytes]...
//  Each uk/uv is the raw serialized bytes from Java's TypeSerializer,
//  which are self-delimiting (Java knows how to deserialize each one).
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntries(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        // Generic byte[] path — only for InnerMap (STRING_STRING) or fallback
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
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

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
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

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapKeys(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapValues(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIsEmpty(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_TRUE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        switch (handle->map_inner_kind) {
            case StateHandle::MapInnerKind::LONG_LONG: {
                auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            case StateHandle::MapInnerKind::LONG_STRING: {
                auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            case StateHandle::MapInnerKind::STRING_LONG: {
                auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            default: {
                auto* table = handle->get_state_table<int64_t, InnerMap>();
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
        }
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIsEmptyGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_TRUE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapClear(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t k = static_cast<int64_t>(key);
        switch (handle->map_inner_kind) {
            case StateHandle::MapInnerKind::LONG_LONG:
                handle->get_state_table<int64_t, InnerMapLongLong>()
                    ->remove(keyGroup, k);
                break;
            case StateHandle::MapInnerKind::LONG_STRING:
                handle->get_state_table<int64_t, InnerMapLongString>()
                    ->remove(keyGroup, k);
                break;
            case StateHandle::MapInnerKind::STRING_LONG:
                handle->get_state_table<int64_t, InnerMapStringLong>()
                    ->remove(keyGroup, k);
                break;
            default:
                handle->get_state_table<int64_t, InnerMap>()
                    ->remove(keyGroup, k);
                break;
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        table->remove(keyGroup, k);
    })
}

// ============================================================================
//  MapState Long UK/UV zero-serialization paths
//  Uses InnerMapLongLong (unordered_map<int64_t, int64_t>) directly.
// ============================================================================


JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapLongLongDenseView(
        JNIEnv* env,jclass,jlong stateHandle,jlong key,jint group,jlongArray out) {
    JNI_ENTRY_VOID({
        auto* handle=from_handle<StateHandle>(stateHandle);auto* table=handle->get_state_table<int64_t,InnerMapLongLong>();
        jlong view[4]={0,0,jlong(reinterpret_cast<uintptr_t>(table->access_epoch_address())),jlong(table->access_epoch())};
        if(!table->is_snapshot_active(group)) {
            auto* row=table->get(group,static_cast<int64_t>(key));size_t span=0;auto* data=row?row->full_dense_data(span):nullptr;
            if(data){view[0]=jlong(reinterpret_cast<uintptr_t>(data));view[1]=span;}
        }
        env->SetLongArrayRegion(out,0,4,view);
    })
}
JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapLongLongCachedView(
        JNIEnv* env,jclass,jlong stateHandle,jlong key,jint group,jlongArray out) {
    JNI_ENTRY_VOID({
        auto* h=from_handle<StateHandle>(stateHandle);auto* table=h->get_state_table<int64_t,InnerMapLongLong>();auto* cache=h->hot_rows;
        jlong view[4]={0,0,jlong(reinterpret_cast<uintptr_t>(table->access_epoch_address())),0};
        if(!table->is_snapshot_active(group)) {
            auto* row=table->get(group,static_cast<int64_t>(key));size_t span=0;auto* data=row?row->full_dense_data(span):nullptr;
            if(data&&span==64&&cache&&!cache->frozen()) {
                auto epoch=table->cache_content_epoch();
                if(!cache->refresh_alias(data,key,group,epoch,h->table_id,&h->row_owner,row)) {
                    auto* cached=cache->insert(key,group,epoch,data,span,h->table_id,table->mutable_access_epoch_address(),&h->row_owner,row);
                    row->attach_cache_data(cached);table->mutable_access_epoch_address()->fetch_add(1,std::memory_order_release);data=cached;
                }
            } else if(cache)cache->record_bypass();
            if(data){view[0]=jlong(reinterpret_cast<uintptr_t>(data));view[1]=span;}
        } else if(cache)cache->record_bypass();
        view[3]=table->access_epoch();env->SetLongArrayRegion(out,0,4,view);
    })
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRowCacheLayout(JNIEnv* env,jclass,jlong stateHandle) {
    JNI_ENTRY_RETURN(jlongArray,nullptr,{
        auto* handle=from_handle<StateHandle>(stateHandle);auto* cache=handle->hot_rows;
        if(!cache)return nullptr;
        auto* table=handle->get_state_table<int64_t,InnerMapLongLong>();
        const jlong info[]={jlong(cache->base_address()),cache->num_sets(),
            jlong(cache->counter_address(0)),jlong(cache->counter_address(1)),jlong(cache->counter_address(2)),
            jlong(cache->counter_address(3)),jlong(cache->counter_address(4)),jlong(cache->counter_address(5)),
            jlong(reinterpret_cast<uintptr_t>(table->access_epoch_address())),
            jlong(reinterpret_cast<uintptr_t>(table->direct_blocked_address())),handle->table_id,
            jlong(reinterpret_cast<uintptr_t>(table->cache_content_epoch_address())),jlong(cache->frozen_address()),
            jlong(cache->admission_closed_address()),jlong(cache->resident())};
        auto result=env->NewLongArray(15);if(result)env->SetLongArrayRegion(result,0,15,info);return result;
    })
}
JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jlong, 0x8000000000000000LL, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return 0x8000000000000000LL; // Long.MIN_VALUE = absent
        const auto* value = inner->find_value(static_cast<int64_t>(userKey));
        return value ? static_cast<jlong>(*value) : 0x8000000000000000LL;
    })
}

// OPT-1: Combined get — single SwissTable + InnerMap lookup.
// Returns JNI_TRUE if found (value written to buf[0]), JNI_FALSE if absent.
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey, jlongArray buf) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        const auto* value = inner->find_value(static_cast<int64_t>(userKey));
        if (!value) return JNI_FALSE;
        jlong val = static_cast<jlong>(*value);
        env->SetLongArrayRegion(buf, 0, 1, &val);
        return JNI_TRUE;
    })
}

// OPT-1: Combined get for LongBytes — returns null if absent, byte[] if found.
// Eliminates separate mapContainsLongBytes + mapGetLongBytes calls.
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongBytesSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        InnerMapLongString* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        auto it = inner->find(static_cast<int64_t>(userKey));
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

// OPT-1: Combined get for BytesLong — returns found + writes value to buf[0].
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetBytesLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey, jlongArray buf) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        std::string uk = jbytearray_to_string(env, userKey);
        auto it = inner->find(uk);
        if (it == inner->end()) return JNI_FALSE;
        jlong val = static_cast<jlong>(it->second);
        env->SetLongArrayRegion(buf, 0, 1, &val);
        return JNI_TRUE;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        return (inner->find(static_cast<int64_t>(userKey)) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong userKey, jlong userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        int64_t k = static_cast<int64_t>(key);
        int64_t uk = static_cast<int64_t>(userKey);
        int64_t uv = static_cast<int64_t>(userValue);
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapLongLong& m) {
            m.put_value(uk, uv);
        })) {
            InnerMapLongLong new_map;
            new_map.put_value(uk, uv);
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapAddAndGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong userKey, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        int64_t k = static_cast<int64_t>(key);
        int64_t uk = static_cast<int64_t>(userKey);
        int64_t d = static_cast<int64_t>(delta);
        int64_t updated = d;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapLongLong& m) {
            auto it = m.find(uk);
            if (it == m.end()) {
                m.emplace(uk, d);
            } else {
                it->second += d;
                updated = it->second;
            }
        })) {
            InnerMapLongLong new_map;
            new_map.emplace(uk, d);
            table->put(keyGroup, k, std::move(new_map));
        }
        return static_cast<jlong>(updated);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapAddSequentialAndSumLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong startUserKey, jint count, jlong modulo, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        int64_t k = static_cast<int64_t>(key);
        int64_t start = static_cast<int64_t>(startUserKey);
        int64_t mod = static_cast<int64_t>(modulo);
        int64_t d = static_cast<int64_t>(delta);
        int n = static_cast<int>(count);
        if (n <= 0) return static_cast<jlong>(0);
        int64_t sum = 0;
        auto add_sequence = [&](InnerMapLongLong& m) {
            sum = m.add_sequence(start, n, mod, d);
        };
        if (!table->modify_in_place(keyGroup, k, add_sequence)) {
            InnerMapLongLong new_map;
            add_sequence(new_map);
            table->put(keyGroup, k, std::move(new_map));
        }
        return static_cast<jlong>(sum);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapSumSequentialLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong startUserKey, jint count, jlong modulo) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return static_cast<jlong>(0);
        return static_cast<jlong>(inner->sum_sequence(
                static_cast<int64_t>(startUserKey), static_cast<int>(count), static_cast<int64_t>(modulo)));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        int64_t k = static_cast<int64_t>(key);
        table->modify_or_remove_in_place(keyGroup, k, [&](InnerMapLongLong& m) {
            m.erase(static_cast<int64_t>(userKey));
        });
    })
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlongArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongLong>();
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        jsize count = static_cast<jsize>(inner->size());
        std::vector<jlong> longs(count * 2);
        jsize idx = 0;
        for (const auto& entry : *inner) {
            longs[idx++] = static_cast<jlong>(entry.first);
            longs[idx++] = static_cast<jlong>(entry.second);
        }
        jlongArray arr = env->NewLongArray(count * 2);
        env->SetLongArrayRegion(arr, 0, count * 2, longs.data());
        return arr;
    })
}

// ============================================================================
//  MapState Long UK + Bytes UV paths (InnerMapLongString)
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        InnerMapLongString* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        auto it = inner->find(static_cast<int64_t>(userKey));
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        int64_t k = static_cast<int64_t>(key);
        int64_t uk = static_cast<int64_t>(userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapLongString& m) {
            m[uk] = std::move(uv);
        })) {
            InnerMapLongString new_map;
            new_map[uk] = std::move(uv);
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        int64_t k = static_cast<int64_t>(key);
        table->modify_or_remove_in_place(keyGroup, k, [&](InnerMapLongString& m) {
            m.erase(static_cast<int64_t>(userKey));
        });
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        InnerMapLongString* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        return (inner->find(static_cast<int64_t>(userKey)) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapLongString>();
        InnerMapLongString* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        // Format: [count(4B)][uk0(8B BE)][uv0_len(4B)][uv0_bytes]...
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_long(entry.first);
            buf.write_int(static_cast<int32_t>(entry.second.size()));
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

// ============================================================================
//  MapState Bytes UK + Long UV paths (InnerMapStringLong)
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jlong, 0x8000000000000000LL, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return 0x8000000000000000LL;
        std::string uk = jbytearray_to_string(env, userKey);
        auto it = inner->find(uk);
        if (it == inner->end()) return 0x8000000000000000LL;
        return static_cast<jlong>(it->second);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jbyteArray userKey, jlong userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        int64_t uv = static_cast<int64_t>(userValue);
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapStringLong& m) {
            m[std::move(uk)] = uv;
        })) {
            InnerMapStringLong new_map;
            new_map[std::move(uk)] = uv;
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        table->modify_or_remove_in_place(keyGroup, k, [&](InnerMapStringLong& m) {
            m.erase(uk);
        });
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        std::string uk = jbytearray_to_string(env, userKey);
        return (inner->find(uk) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMapStringLong>();
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        // Format: [count(4B)][uk0_len(4B)][uk0_bytes][uv0(8B BE)]...
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_int(static_cast<int32_t>(entry.first.size()));
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_long(entry.second);
        }
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

// ============================================================================
//  ReducingState / AggregatingState JNI entry points
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, int64_t>();
        int64_t* val = table->get(keyGroup, static_cast<int64_t>(key));
        return val ? static_cast<jlong>(*val) : 0;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, int64_t>();
        int64_t k = static_cast<int64_t>(key);
        int64_t v = static_cast<int64_t>(value);
        int64_t updated = v;
        bool modified = table->modify_in_place(keyGroup, k, [&](int64_t& existing) {
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
            table->put(keyGroup, k, v);
        }
        if (handle->hot_cache_ll) {
            handle->hot_cache_ll->put(k, updated);
        }
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string* val = table->get(keyGroup, k);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetGenericLongSafe(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, int64_t>();
        std::string k = jbytearray_to_string(env, key);
        int64_t* val = table->get(keyGroup, k);
        if (!val) {
            return JNI_FALSE;
        }
        jlong v = static_cast<jlong>(*val);
        env->SetLongArrayRegion(out, 0, 1, &v);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutGenericLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jlong value) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, int64_t>();
        std::string k = jbytearray_to_string(env, key);
        table->put(keyGroup, std::move(k), static_cast<int64_t>(value));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddGenericLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, int64_t>();
        std::string k = jbytearray_to_string(env, key);
        int64_t v = static_cast<int64_t>(value);
        bool modified = table->modify_in_place(keyGroup, k, [&](int64_t& existing) {
            switch (builtinAggType) {
                case 0: existing += v; break;  // SUM
                case 1: existing = std::min(existing, v); break;  // MIN
                case 2: existing = std::max(existing, v); break;  // MAX
                default:
                    throw std::runtime_error("User-defined reduce requires Java callback");
            }
        });
        if (!modified) {
            table->put(keyGroup, std::move(k), v);
        }
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceGetAndPutGenericLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jlong newValue, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, int64_t>();
        std::string k = jbytearray_to_string(env, key);
        int64_t* val = table->get(keyGroup, k);
        if (val) {
            jlong v = static_cast<jlong>(*val);
            env->SetLongArrayRegion(out, 0, 1, &v);
            return JNI_TRUE;
        }
        table->put(keyGroup, std::move(k), static_cast<int64_t>(newValue));
        return JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceAddGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jbyteArray value, jint builtinAggType) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string v = jbytearray_to_string(env, value);
        // For generic path, store directly (Java side handles reduce logic)
        table->put(keyGroup, k, std::move(v));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceClear(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, int64_t>();
        table->remove(keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_aggGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string* val = table->get(keyGroup, k);
        return val ? string_to_jbytearray(env, *val) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_aggAddGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jbyteArray accumulator) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        std::string acc = jbytearray_to_string(env, accumulator);
        table->put(keyGroup, k, std::move(acc));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_reduceClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, std::string>();
        std::string k = jbytearray_to_string(env, key);
        table->remove(keyGroup, k);
    })
}

}  // extern "C"

// ============================================================================
//  MapState streaming iterator — for generic InnerMap (string→string)
// ============================================================================

namespace {

struct NativeMapIterator {
    InnerMap::const_iterator current;
    InnerMap::const_iterator end;
};

}  // namespace

extern "C" {

// Create iterator for int64 key + void namespace path
JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorCreate(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<int64_t, InnerMap>();
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return 0;
        auto* iter = new NativeMapIterator{inner->cbegin(), inner->cend()};
        return to_handle(iter);
    })
}

// Create iterator for generic key path
JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorCreateGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->get_state_table<std::string, InnerMap>();
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner || inner->empty()) return 0;
        auto* iter = new NativeMapIterator{inner->cbegin(), inner->cend()};
        return to_handle(iter);
    })
}

// Advance iterator: return [uk_bytes][uv_bytes] for one entry, or null if exhausted
JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorNext(
        JNIEnv* env, jclass, jlong iterHandle) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* iter = from_handle<NativeMapIterator>(iterHandle);
        if (iter->current == iter->end) return nullptr;
        const auto& entry = *(iter->current);
        ++(iter->current);
        jsize total = static_cast<jsize>(entry.first.size() + entry.second.size());
        jbyteArray result = env->NewByteArray(total);
        jsize ukLen = static_cast<jsize>(entry.first.size());
        env->SetByteArrayRegion(result, 0, ukLen,
                reinterpret_cast<const jbyte*>(entry.first.data()));
        env->SetByteArrayRegion(result, ukLen, static_cast<jsize>(entry.second.size()),
                reinterpret_cast<const jbyte*>(entry.second.data()));
        return result;
    })
}

// Destroy iterator
JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIteratorDestroy(
        JNIEnv* env, jclass, jlong iterHandle) {
    JNI_ENTRY_VOID({
        delete from_handle<NativeMapIterator>(iterHandle);
    })
}

}  // extern "C"
