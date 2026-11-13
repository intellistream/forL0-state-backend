// jni_map_state.cpp — JNI entry points for MapState operations.
// MapState uses a nested InnerMap (std::unordered_map<std::string, std::string>)
// stored in SwissTable<K, InnerMap>. Each user-key operation directly accesses
// the inner map without serializing/deserializing the entire map.

#include <jni.h>
#include "jni_utils.h"
#include "state_cache_access.h"
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        std::string uk = jbytearray_to_string(env, userKey);
        std::string_view cached;
        if (state_cache.get(uk, &cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        auto it = inner->find(uk);
        if (it != inner->end()) state_cache.put(uk, it->second);
        return (it != inner->end()) ? string_to_jbytearray(env, it->second) : nullptr;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        std::string k = jbytearray_to_string(env, key);
        MapCallCache state_cache(handle, keyGroup, k);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string_view cached;
        if (state_cache.get(uk, &cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return nullptr;
        auto it = inner->find(uk);
        if (it != inner->end()) state_cache.put(uk, it->second);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        state_cache.prepare(uk);
        std::string* cache_stored = nullptr;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMap& m) {
            auto& cache_target = m[std::move(uk)]; cache_target = std::move(uv); cache_stored = &cache_target;
    })) {
            InnerMap new_map;
            auto& cache_target = new_map[std::move(uk)]; cache_target = std::move(uv); cache_stored = &cache_target;
            table->put(keyGroup, k, std::move(new_map));
        }
        if (cache_stored) state_cache.put_prepared(*cache_stored);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jbyteArray userKey, jbyteArray userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        std::string k = jbytearray_to_string(env, key);
        MapCallCache state_cache(handle, keyGroup, k);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        state_cache.prepare(uk);
        std::string* cache_stored = nullptr;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMap& m) {
            auto& cache_target = m[std::move(uk)]; cache_target = std::move(uv); cache_stored = &cache_target;
    })) {
            InnerMap new_map;
            auto& cache_target = new_map[std::move(uk)]; cache_target = std::move(uv); cache_stored = &cache_target;
            table->put(keyGroup, k, std::move(new_map));
        }
        if (cache_stored) state_cache.put_prepared(*cache_stored);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemove(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        state_cache.remove(uk);
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
        std::string k = jbytearray_to_string(env, key);
        MapCallCache state_cache(handle, keyGroup, k);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string uk = jbytearray_to_string(env, userKey);
        state_cache.remove(uk);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        // Generic byte[] path — only for InnerMap (STRING_STRING) or fallback
        std::string_view cached;
        if (state_cache.get_bulk(&cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        state_cache.put_bulk(buf.data(), buf.size());
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        std::string k = jbytearray_to_string(env, key);
        MapCallCache state_cache(handle, keyGroup, k);
        std::string_view cached;
        if (state_cache.get_bulk(&cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        state_cache.put_bulk(buf.data(), buf.size());
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapKeys(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
                auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            case StateHandle::MapInnerKind::LONG_STRING: {
                auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            case StateHandle::MapInnerKind::STRING_LONG: {
                auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
                auto* inner = table->get(keyGroup, k);
                return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
            }
            default: {
                auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        state_cache.invalidate();
        int64_t k = static_cast<int64_t>(key);
        switch (handle->map_inner_kind) {
            case StateHandle::MapInnerKind::LONG_LONG:
                handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id)
                    ->remove(keyGroup, k);
                break;
            case StateHandle::MapInnerKind::LONG_STRING:
                handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id)
                    ->remove(keyGroup, k);
                break;
            case StateHandle::MapInnerKind::STRING_LONG:
                handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id)
                    ->remove(keyGroup, k);
                break;
            default:
                handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id)
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
        std::string k = jbytearray_to_string(env, key);
        MapCallCache state_cache(handle, keyGroup, k);
        state_cache.invalidate();
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        table->remove(keyGroup, k);
    })
}

// ============================================================================
//  MapState Long UK/UV zero-serialization paths
//  Uses InnerMapLongLong (unordered_map<int64_t, int64_t>) directly.
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jlong, 0x8000000000000000LL, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        int64_t uk = static_cast<int64_t>(userKey);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        int64_t cached;
        if (state_cache.get_number(uk, &cached)) return cached;
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return 0x8000000000000000LL; // Long.MIN_VALUE = absent
        auto it = inner->find(uk);
        if (it == inner->end()) return 0x8000000000000000LL;
        state_cache.put(uk, it->second);
        return static_cast<jlong>(it->second);
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
        int64_t uk = static_cast<int64_t>(userKey);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        int64_t cached;
        if (state_cache.get_number(uk, &cached)) {
            jlong value = cached; env->SetLongArrayRegion(buf, 0, 1, &value); return JNI_TRUE;
        }
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        auto it = inner->find(uk);
        if (it == inner->end()) return JNI_FALSE;
        state_cache.put(uk, it->second);
        jlong val = static_cast<jlong>(it->second);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
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
        std::string uk = jbytearray_to_string(env, userKey);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        int64_t cached;
        if (state_cache.get_number(uk, &cached)) {
            jlong value = cached; env->SetLongArrayRegion(buf, 0, 1, &value); return JNI_TRUE;
        }
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        auto it = inner->find(uk);
        if (it == inner->end()) return JNI_FALSE;
        state_cache.put(uk, it->second);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        int64_t uk = static_cast<int64_t>(userKey);
        int64_t uv = static_cast<int64_t>(userValue);
        state_cache.prepare(uk);
        int64_t* cache_stored = nullptr;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapLongLong& m) {
            auto& cache_target = m[uk]; cache_target = uv; cache_stored = &cache_target;
    })) {
            InnerMapLongLong new_map;
            auto& cache_target = new_map[uk]; cache_target = uv; cache_stored = &cache_target;
            table->put(keyGroup, k, std::move(new_map));
        }
        if (cache_stored) state_cache.put_prepared(*cache_stored);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapAddAndGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong userKey, jlong delta) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        state_cache.invalidate();
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        state_cache.invalidate();
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        int64_t start = static_cast<int64_t>(startUserKey);
        int64_t mod = static_cast<int64_t>(modulo);
        int64_t d = static_cast<int64_t>(delta);
        int n = static_cast<int>(count);
        if (n <= 0) return static_cast<jlong>(0);
        int64_t sum = 0;
        auto add_sequence = [&](InnerMapLongLong& m) {
            for (int i = 0; i < n; ++i) {
                int64_t uk = start + static_cast<int64_t>(i);
                if (mod > 0) {
                    uk %= mod;
                    if (uk < 0) uk += mod;
                }
                auto it = m.find(uk);
                if (it == m.end()) {
                    m.emplace(uk, d);
                    sum += d;
                } else {
                    it->second += d;
                    sum += it->second;
                }
            }
        };
        if (!table->modify_in_place(keyGroup, k, add_sequence)) {
            InnerMapLongLong new_map;
            new_map.reserve(static_cast<size_t>(n));
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        InnerMapLongLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return static_cast<jlong>(0);
        int64_t start = static_cast<int64_t>(startUserKey);
        int64_t mod = static_cast<int64_t>(modulo);
        int n = static_cast<int>(count);
        if (n <= 0) return static_cast<jlong>(0);
        int64_t sum = 0;
        for (int i = 0; i < n; ++i) {
            int64_t uk = start + static_cast<int64_t>(i);
            if (mod > 0) {
                uk %= mod;
                if (uk < 0) uk += mod;
            }
            auto it = inner->find(uk);
            if (it != inner->end()) {
                sum += it->second;
            }
        }
        return static_cast<jlong>(sum);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        state_cache.remove(static_cast<int64_t>(userKey));
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        std::string_view cached;
        if (state_cache.get(static_cast<int64_t>(userKey), &cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
        InnerMapLongString* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return nullptr;
        auto it = inner->find(static_cast<int64_t>(userKey));
        if (it != inner->end()) state_cache.put(static_cast<int64_t>(userKey), it->second);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        int64_t uk = static_cast<int64_t>(userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        state_cache.prepare(uk);
        std::string* cache_stored = nullptr;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapLongString& m) {
            auto& cache_target = m[uk]; cache_target = std::move(uv); cache_stored = &cache_target;
    })) {
            InnerMapLongString new_map;
            auto& cache_target = new_map[uk]; cache_target = std::move(uv); cache_stored = &cache_target;
            table->put(keyGroup, k, std::move(new_map));
        }
        if (cache_stored) state_cache.put_prepared(*cache_stored);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutLongBytesRange(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong userKey,
        jbyteArray value, jint offset, jint length) {
    JNI_ENTRY_VOID({
        if (!value || offset < 0 || length < 0 || offset > env->GetArrayLength(value) - length)
            throw std::invalid_argument("invalid serialized map value range");
        auto* handle = from_handle<StateHandle>(state);
        MapCallCache cache(handle, int(group), int64_t(key));
        cache.prepare(int64_t(userKey));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
        std::string* stored = nullptr;
        auto update = [&](InnerMapLongString& map) {
            auto& bytes = map[int64_t(userKey)];
            bytes.resize(size_t(length));
            env->GetByteArrayRegion(value, offset, length, reinterpret_cast<jbyte*>(bytes.data()));
            stored = &bytes;
        };
        if (!table->modify_in_place(group, int64_t(key), update)) {
            InnerMapLongString map; update(map);
            table->put(group, int64_t(key), std::move(map));
        }
        cache.put_prepared(*stored);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveLongBytes(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        state_cache.remove(static_cast<int64_t>(userKey));
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        std::string_view cached;
        if (state_cache.get_bulk(&cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
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
        state_cache.put_bulk(buf.data(), buf.size());
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
        std::string uk = jbytearray_to_string(env, userKey);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        int64_t cached;
        if (state_cache.get_number(uk, &cached)) return cached;
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
        InnerMapStringLong* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return 0x8000000000000000LL;
        auto it = inner->find(uk);
        if (it == inner->end()) return 0x8000000000000000LL;
        state_cache.put(uk, it->second);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        int64_t uv = static_cast<int64_t>(userValue);
        state_cache.prepare(uk);
        int64_t* cache_stored = nullptr;
        if (!table->modify_in_place(keyGroup, k, [&](InnerMapStringLong& m) {
            auto& cache_target = m[std::move(uk)]; cache_target = uv; cache_stored = &cache_target;
    })) {
            InnerMapStringLong new_map;
            auto& cache_target = new_map[std::move(uk)]; cache_target = uv; cache_stored = &cache_target;
            table->put(keyGroup, k, std::move(new_map));
        }
        if (cache_stored) state_cache.put_prepared(*cache_stored);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveBytesLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        state_cache.remove(uk);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
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
        MapCallCache state_cache(handle, keyGroup, static_cast<int64_t>(key));
        std::string_view cached;
        if (state_cache.get_bulk(&cached)) return string_to_jbytearray(env, cached);
        auto* table = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
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
        state_cache.put_bulk(buf.data(), buf.size());
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, int64_t>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
        table->remove(keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_aggGetGeneric(
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
Java_org_apache_flink_state_forl0_NativeEngine_aggAddGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup,
        jbyteArray accumulator) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
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
