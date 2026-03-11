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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        InnerMap* inner = table->get(keyGroup, k);
        if (inner) {
            (*inner)[std::move(uk)] = std::move(uv);
        } else {
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
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string uk = jbytearray_to_string(env, userKey);
        std::string uv = jbytearray_to_string(env, userValue);
        InnerMap* inner = table->get(keyGroup, k);
        if (inner) {
            (*inner)[std::move(uk)] = std::move(uv);
        } else {
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return;
        std::string uk = jbytearray_to_string(env, userKey);
        inner->erase(uk);
        if (inner->empty()) {
            table->remove(keyGroup, k);
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return;
        std::string uk = jbytearray_to_string(env, userKey);
        inner->erase(uk);
        if (inner->empty()) {
            table->remove(keyGroup, k);
        }
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner || inner->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(inner->size()));
        for (const auto& entry : *inner) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.first.data()), entry.first.size());
            buf.write_raw(reinterpret_cast<const uint8_t*>(entry.second.data()), entry.second.size());
        }
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
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
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
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
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapIsEmpty(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jboolean, JNI_TRUE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        return (!inner || inner->empty()) ? JNI_TRUE : JNI_FALSE;
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
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        table->remove(keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        table->remove(keyGroup, k);
    })
}

// ============================================================================
//  MapState Long UK/UV zero-serialization paths
// ============================================================================

// Helper: encode int64_t as 8-byte big-endian string
static std::string map_long_to_be(int64_t v) {
    uint8_t buf[8];
    for (int i = 7; i >= 0; --i) { buf[i] = static_cast<uint8_t>(v & 0xFF); v >>= 8; }
    return std::string(reinterpret_cast<const char*>(buf), 8);
}

// Helper: decode 8-byte big-endian string to int64_t
static int64_t map_be_to_long(const std::string& s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    int64_t v = 0;
    for (int i = 0; i < 8; ++i) { v = (v << 8) | p[i]; }
    return v;
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jlong, 0x8000000000000000LL, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return 0x8000000000000000LL; // Long.MIN_VALUE = absent
        std::string uk = map_long_to_be(static_cast<int64_t>(userKey));
        auto it = inner->find(uk);
        if (it == inner->end()) return 0x8000000000000000LL;
        return static_cast<jlong>(map_be_to_long(it->second));
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapContainsLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner) return JNI_FALSE;
        std::string uk = map_long_to_be(static_cast<int64_t>(userKey));
        return (inner->find(uk) != inner->end()) ? JNI_TRUE : JNI_FALSE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong userKey, jlong userValue) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string uk = map_long_to_be(static_cast<int64_t>(userKey));
        std::string uv = map_long_to_be(static_cast<int64_t>(userValue));
        InnerMap* inner = table->get(keyGroup, k);
        if (inner) {
            (*inner)[std::move(uk)] = std::move(uv);
        } else {
            InnerMap new_map;
            new_map[std::move(uk)] = std::move(uv);
            table->put(keyGroup, k, std::move(new_map));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong userKey) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        InnerMap* inner = table->get(keyGroup, k);
        if (!inner) return;
        std::string uk = map_long_to_be(static_cast<int64_t>(userKey));
        inner->erase(uk);
        if (inner->empty()) {
            table->remove(keyGroup, k);
        }
    })
}

JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapEntriesLongLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlongArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
        InnerMap* inner = table->get(keyGroup, static_cast<int64_t>(key));
        if (!inner || inner->empty()) return nullptr;
        jsize count = static_cast<jsize>(inner->size());
        std::vector<jlong> longs(count * 2);
        jsize idx = 0;
        for (const auto& entry : *inner) {
            longs[idx++] = static_cast<jlong>(map_be_to_long(entry.first));
            longs[idx++] = static_cast<jlong>(map_be_to_long(entry.second));
        }
        jlongArray arr = env->NewLongArray(count * 2);
        env->SetLongArrayRegion(arr, 0, count * 2, longs.data());
        return arr;
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
        int64_t* existing = table->get(keyGroup, k);
        if (!existing) {
            table->put(keyGroup, k, v);
        } else {
            switch (builtinAggType) {
                case 0: *existing += v; break;  // SUM
                case 1: *existing = std::min(*existing, v); break;  // MIN
                case 2: *existing = std::max(*existing, v); break;  // MAX
                default:
                    // User-defined: Java side should handle via callback
                    throw std::runtime_error("User-defined reduce requires Java callback");
            }
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
