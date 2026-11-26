// TVF window/slice namespaces are LongSerializer, not TimeWindow.Serializer.
// Pass their numeric identities directly, without serializing key+namespace.
#include "state_cache_access.h"
using namespace forl0;

namespace {
template<class V>
std::optional<V> get_number(StateHandle* handle, int group, int64_t ns, int64_t key, uint64_t header) {
    const V* value = handle->table<WindowStateKey, V>()->get(group, ns, {key, header});
    return value ? std::optional<V>(*value) : std::nullopt;
}

}

extern "C" {
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetNumberLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header, jlongArray out) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        jlong bits;
        if (h->value_type == StateHandle::ValueType::FLOAT64) {
            auto value = get_number<double>(h, group, ns, key, header);
            if (!value) return JNI_FALSE;
            std::memcpy(&bits, &*value, sizeof(bits));
        } else {
            auto value = get_number<int64_t>(h, group, ns, key, header);
            if (!value) return JNI_FALSE;
            bits = *value;
        }
        set_long_result(env, h, bits, out);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutNumberLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header, jlong bits) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        if (h->value_type == StateHandle::ValueType::FLOAT64) {
            double value; std::memcpy(&value, &bits, sizeof(value));
            h->table<WindowStateKey, double>()->put(group, int64_t(ns), {key, uint64_t(header)}, value);
        } else {
            h->table<WindowStateKey, int64_t>()->put(group, int64_t(ns), {key, uint64_t(header)}, int64_t(bits));
        }
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetBlobLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_namespace_blob(h, group, int64_t(ns), {key, uint64_t(header)});
        return value ? string_to_jbytearray(env, *value) : nullptr;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetBlobPtrLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_namespace_blob(h, group, int64_t(ns), {key, uint64_t(header)});
        if (!value || value->empty()) return JNI_FALSE;
        set_blob_result(env, h, *value);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutBlobLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header, jbyteArray bytes) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        const auto length = env->GetArrayLength(bytes);
        put_namespace_blob(h, group, int64_t(ns), {key, uint64_t(header)}, size_t(length), [&](char* target) {
            env->GetByteArrayRegion(bytes, 0, length, reinterpret_cast<jbyte*>(target));
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutBlobRangeLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header,
        jbyteArray bytes, jint offset, jint length) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        if (offset < 0 || length < 0 || offset > env->GetArrayLength(bytes) - length)
            throw std::invalid_argument("invalid row byte range");
        auto copy = [&](char* target) {
            env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(target));
        };
        put_namespace_blob(h, group, int64_t(ns), {key, uint64_t(header)}, size_t(length), copy);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutBlobAddressLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header,
        jobject owner, jlong address, jint length) {
    JNI_ENTRY_VOID({
        // The JNI local reference to owner keeps the MemorySegment alive for
        // this synchronous copy, including caller-owned direct buffers.
        auto* h = from_handle<StateHandle>(state);
        if (!owner || !address || length < 0) throw std::invalid_argument("invalid native row range");
        put_namespace_blob_address(h, group, int64_t(ns), {key, uint64_t(header)},
                                   reinterpret_cast<const char*>(address), size_t(length));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        if (h->value_type == StateHandle::ValueType::FLOAT64) {
            h->table<WindowStateKey, double>()->remove(group, int64_t(ns), {key, uint64_t(header)});
        } else if (h->value_type == StateHandle::ValueType::INT64 || h->value_type == StateHandle::ValueType::INT32) {
            h->table<WindowStateKey, int64_t>()->remove(group, int64_t(ns), {key, uint64_t(header)});
        } else {
            h->visit_window_rows<WindowStateKey>([&](auto* table) { table->remove(group, int64_t(ns), {key, uint64_t(header)}); });
        }
    })
}
}
