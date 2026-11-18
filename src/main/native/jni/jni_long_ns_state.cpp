// TVF window/slice namespaces are LongSerializer, not TimeWindow.Serializer.
// Pass their numeric identities directly, without serializing key+namespace.
#include "state_cache_access.h"
using namespace forl0;

namespace {
template<class F> decltype(auto) with_namespace(jlong ns, jlong header, F&& fn) {
    if (header == 0) return fn(int64_t(ns));
    // Preserve nullable RowData keys and RowKind without penalizing the usual
    // INSERT/non-null numeric namespace with a serialized-key allocation.
    std::string identity(16, '\0');
    std::memcpy(identity.data(), &ns, 8);
    std::memcpy(identity.data() + 8, &header, 8);
    return fn(identity);
}
std::optional<std::string_view> get_blob(StateHandle* h, int group, int64_t ns, int64_t header, int64_t key) {
    if (header != 0) {
        return with_namespace(ns, header, [&](const auto& identity) {
            return cached_get(h, h->table<int64_t, std::string>(), group, identity, key);
        });
    }
    if (h->serialized_cache) {
        LongNamespaceCacheKey identity(group, ns, key);
        std::string_view bytes;
        if (h->serialized_cache->get_view(identity.view(), &bytes)) return bytes;
    }
    auto* value = h->table<int64_t, std::string>()->get(group, ns, key);
    return value ? std::optional<std::string_view>(*value) : std::nullopt;
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
            auto* table = h->table<int64_t, double>();
            auto value = with_namespace(ns, header, [&](const auto& identity) { return cached_get(h, table, int(group), identity, int64_t(key)); });
            if (!value) return JNI_FALSE;
            std::memcpy(&bits, &*value, sizeof(bits));
        } else {
            auto* table = h->table<int64_t, int64_t>();
            auto value = with_namespace(ns, header, [&](const auto& identity) { return cached_get(h, table, int(group), identity, int64_t(key)); });
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
            auto* table = h->table<int64_t, double>();
            double value; std::memcpy(&value, &bits, sizeof(value));
            with_namespace(ns, header, [&](const auto& identity) { return cached_put(h, table, int(group), identity, int64_t(key), value); });
        } else {
            auto* table = h->table<int64_t, int64_t>();
            with_namespace(ns, header, [&](const auto& identity) { return cached_put(h, table, int(group), identity, int64_t(key), int64_t(bits)); });
        }
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetBlobLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_blob(h, group, ns, header, key);
        return value ? string_to_jbytearray(env, *value) : nullptr;
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetBlobPtrLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        auto value = get_blob(h, group, ns, header, key);
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
            with_namespace(ns, header, [&](const auto& identity) { put_java_blob(env, h, h->table<int64_t, std::string>(), bytes, int(group), identity, int64_t(key)); });
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
            with_namespace(ns, header, [&](const auto& identity) {
            put_owned_blob(h, h->table<int64_t, std::string>(), size_t(length), [&](char* target) {
                env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(target));
            }, int(group), identity, int64_t(key));
        });
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
            with_namespace(ns, header, [&](const auto& identity) {
            put_owned_blob(h, h->table<int64_t, std::string>(), size_t(length), [&](char* target) {
                std::memmove(target, reinterpret_cast<const void*>(address), size_t(length));
            }, int(group), identity, int64_t(key));
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearLongNs(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong ns, jlong header) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        if (h->value_type == StateHandle::ValueType::FLOAT64) {
            with_namespace(ns, header, [&](const auto& identity) { return cached_remove(h, h->table<int64_t, double>(), int(group), identity, int64_t(key)); });
        } else if (h->value_type == StateHandle::ValueType::INT64 || h->value_type == StateHandle::ValueType::INT32) {
            with_namespace(ns, header, [&](const auto& identity) { return cached_remove(h, h->table<int64_t, int64_t>(), int(group), identity, int64_t(key)); });
        } else {
            with_namespace(ns, header, [&](const auto& identity) { return cached_remove(h, h->table<int64_t, std::string>(), int(group), identity, int64_t(key)); });
        }
    })
}
}
