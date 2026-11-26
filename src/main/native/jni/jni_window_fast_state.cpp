// Numeric arguments for SQL windows, retaining the original serialized table
// and checkpoint encodings. Cache hits do not allocate temporary string keys.
#include "state_cache_access.h"
#include "window_map_storage.h"
using namespace forl0;

extern "C" {
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetSqlWindowPtr(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jlong start, jlong end, jlong header) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_namespace_blob(h, group, TimeWindow{start, end}, {key, uint64_t(header)});
        if (!value || value->empty()) return JNI_FALSE;
        set_blob_result(env, h, *value);
        return JNI_TRUE;
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetSqlWindowBytes(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_namespace_blob(h, group, TimeWindow{start, end}, {key, uint64_t(header)});
        return value ? string_to_jbytearray(env, *value) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutSqlWindowRange(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header,
        jbyteArray bytes, jint offset, jint length) {
    JNI_ENTRY_VOID({
        if (offset < 0 || length < 0 || offset > env->GetArrayLength(bytes) - length)
            throw std::invalid_argument("invalid SQL window row range");
        auto* h = from_handle<StateHandle>(state);
        auto copy = [&](char* target) {
            env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(target));
        };
        put_namespace_blob(h, group, TimeWindow{start, end}, {key, uint64_t(header)}, size_t(length), copy);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutSqlWindowAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header,
        jobject owner, jlong address, jint length) {
    JNI_ENTRY_VOID({
        if (!owner || !address || length < 0) throw std::invalid_argument("invalid SQL window native row");
        auto* h = from_handle<StateHandle>(state);
        put_namespace_blob_address(h, group, TimeWindow{start, end}, {key, uint64_t(header)},
                                   reinterpret_cast<const char*>(address), size_t(length));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearSqlWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        h->visit_window_rows<WindowStateKey>([&](auto* table) { table->remove(group, TimeWindow{start, end}, {key, uint64_t(header)}); });
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        auto* table = h->table<int64_t, InnerMapTimeWindow>();
        auto* map = table->get(group, int64_t(key));
        if (!map) return JNI_FALSE;
        const auto* found = map->find_value(TimeWindow{start, end});
        if (!found) return JNI_FALSE;
        h->read_result[0] = found->first;
        h->read_result[1] = found->second;
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jlong user_start, jlong user_end, jlong value_start, jlong value_end) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        TimeWindow uk(user_start, user_end), uv(value_start, value_end);
        auto* table = h->table<int64_t, InnerMapTimeWindow>();
        put_window_entry(table, group, key, uk, uv);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        auto* table = h->table<int64_t, InnerMapTimeWindow>();
        remove_window_entry(table, group, key, TimeWindow(start, end));
    })
}
}
