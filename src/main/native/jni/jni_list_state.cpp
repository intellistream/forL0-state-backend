// jni_list_state.cpp — JNI entry points for ListState operations.
// Each namespace table stores the canonical ListSerializer bytes of every keyed list.

#include <jni.h>
#include "jni_utils.h"
#include "list_state_access.h"
#include "state_engine.h"
#include "flink_binary_format.h"

using namespace forl0;

namespace {
template<class Fn>
decltype(auto) with_window_list(JNIEnv* env, StateHandle* state, int64_t ns, WindowStateKey key,
                                jbyteArray bytes, jint length, Fn&& use) {
    if (!state->numeric_window_lists) throw std::invalid_argument("ListState does not use numeric namespaces");
    if (!state->namespace_serialized_keys) return use(state->table<WindowStateKey, ElementList>(), ns, key);
    if (!bytes && length < 0 && length >= -17) {
        // The two arguments carry the exact serialized bytes in little-endian order.
        // Both transports address the same NamespaceBlob key and checkpoint entry.
        char raw[16];
        for (size_t i = 0; i < 8; ++i) {
            raw[i] = static_cast<char>(static_cast<uint64_t>(key.number) >> (8 * i));
            raw[8 + i] = static_cast<char>(key.header >> (8 * i));
        }
        const NamespaceBlob user(raw, size_t(~length));
        return use(state->table<NamespaceBlob, ElementList>(), ns, user);
    }
    if (!bytes || length < 0 || length > env->GetArrayLength(bytes)) throw std::invalid_argument("invalid ListState key length");
    NamespaceBlob user(size_t(length), '\0');
    env->GetByteArrayRegion(bytes, 0, length, reinterpret_cast<jbyte*>(user.data()));
    return use(state->table<NamespaceBlob, ElementList>(), ns, user);
}

// Older JNI callers still supply the namespace and key in one serialized buffer.
template<class Fn>
decltype(auto) with_serialized_list(JNIEnv* env, StateHandle* state, jbyteArray encoded, Fn&& use) {
    auto bytes = jbytearray_to_string(env, encoded);
    if (!state->numeric_window_lists) return use(state->table<NamespaceBlob, ElementList>(), bytes);
    ReadBuffer in(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    const int64_t ns = in.read_long();
    if (state->namespace_serialized_keys) {
        NamespaceBlob user(reinterpret_cast<const char*>(in.current()), in.remaining());
        return use(state->table<NamespaceBlob, ElementList>(), ns, user);
    }
    if (in.remaining() != (state->row_data_key ? 20u : 8u)) throw std::invalid_argument("invalid numeric ListState key");
    return use(state->table<WindowStateKey, ElementList>(), ns, read_window_state_key(in, state->row_data_key));
}

void append_java_element(JNIEnv* env, ElementList& list, jbyteArray bytes, jint offset, jint length, bool row) {
    const size_t prefix = row ? 4 : 0;
    list.append_bytes(1, prefix + size_t(length), [&](char* out) {
        if (row) for (size_t i = 0; i < 4; ++i) out[i] = char(uint32_t(length) >> (24 - 8 * i));
        env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(out + prefix));
        if (env->ExceptionCheck()) throw std::runtime_error("ListState element copy failed");
    });
}
}  // namespace

extern "C" {

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUsesLongNamespaces(JNIEnv*, jclass, jlong state) {
    return from_handle<StateHandle>(state)->numeric_window_lists;
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listSupportsInlineWindowKeys(JNIEnv*, jclass, jlong state) {
    auto* handle = from_handle<StateHandle>(state);
    return handle->numeric_window_lists && handle->namespace_serialized_keys;
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGetLongNamespace(
        JNIEnv* env, jclass, jlong state, jint group, jlong ns, jlong key, jlong header,
        jbyteArray user, jint keyLength) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        return with_window_list(env, from_handle<StateHandle>(state), ns, {key, static_cast<uint64_t>(header)},
                user, keyLength, [&](auto* table, const auto&... args) { return list_get(env, table, group, args...); });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listChangeLongNamespace(
        JNIEnv* env, jclass, jlong state, jint group, jlong ns, jlong key, jlong header,
        jbyteArray user, jint keyLength, jint operation, jbyteArray bytes, jint offset, jint length) {
    JNI_ENTRY_VOID({
        if (operation != 3 && (!bytes || offset < 0 || length < 0 || offset > env->GetArrayLength(bytes) - length))
            throw std::invalid_argument("invalid ListState payload range");
        with_window_list(env, from_handle<StateHandle>(state), ns, {key, static_cast<uint64_t>(header)},
                user, keyLength, [&](auto* table, const auto&... args) {
            if (operation == 3) { table->remove(group, args...); return; }
            if (operation == 0 || operation == 4) {
                table->update_or_create(group, args..., [&](ElementList& list) {
                    append_java_element(env, list, bytes, offset, length, operation == 4);
                });
                return;
            }
            if (operation != 1 && operation != 2) throw std::invalid_argument("invalid ListState operation");
            NamespaceBlob packed(size_t(length), '\0');
            env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(packed.data()));
            if (env->ExceptionCheck()) throw std::runtime_error("ListState payload copy failed");
            if (operation == 1) {
                table->update_or_create(group, args..., [&](ElementList& list) { list.assign_serialized(packed); });
            } else {
                const auto count = ElementList::serialized_count(packed);
                if (count) table->update_or_create(group, args..., [&](ElementList& list) {
                    list.append_bytes(count, packed.size() - 4, [&](char* dst) {
                        std::memcpy(dst, packed.data() + 4, packed.size() - 4);
                    });
                });
            }
        });
    })
}


// ============================================================================
//  ListState: get — returns [count(4 bytes BE)][elem1_bytes][elem2_bytes]...
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGet(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        return list_get(env, table, keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        return with_serialized_list(env, handle, key, [&](auto* table, const auto&... args) {
            return list_get(env, table, keyGroup, args...);
        });
    })
}

// ============================================================================
//  ListState: add — append single element (O(1) amortized push_back)
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddLong(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlong element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        int64_t k = static_cast<int64_t>(key);
        // Serialize the long element as 8-byte big-endian
        int64_t elem = static_cast<int64_t>(element);
        uint8_t buf[8];
        for (int i = 7; i >= 0; --i) { buf[i] = static_cast<uint8_t>(elem & 0xFF); elem >>= 8; }
        NamespaceBlob elem_str(reinterpret_cast<const char*>(buf), 8);

        list_add(table, std::move(elem_str), keyGroup, k);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddRowRange(
        JNIEnv* env, jclass, jlong state, jlong key, jbyteArray genericKey, jint group,
        jlong nsStart, jlong nsEnd, jint mode, jbyteArray row, jint offset, jint length) {
    JNI_ENTRY_VOID({
        if (!row || offset < 0 || length < 0 || offset > env->GetArrayLength(row) - length)
            throw std::invalid_argument("invalid list row range");
        auto* h = from_handle<StateHandle>(state);
        auto append = [&](ElementList& list) { append_java_element(env, list, row, offset, length, true); };
        if (mode == 0) {
            auto* table = h->table<int64_t, ElementList>();
            table->update_or_create(int(group), int64_t(key), append);
        } else if (mode == 1) {
            auto* table = h->table<int64_t, ElementList>();
            table->update_or_create(int(group), TimeWindow(nsStart, nsEnd), int64_t(key), append);
        } else if (mode == 2 && genericKey) {
            with_serialized_list(env, h, genericKey, [&](auto* table, const auto&... args) {
                table->update_or_create(int(group), args..., append);
            });
        } else throw std::invalid_argument("invalid list row key mode");
    })
}

static ElementList encode_longs(const std::vector<jlong>& values) {
    ElementList result;
    if (!values.empty()) result.append_bytes(static_cast<uint32_t>(values.size()), values.size() * 8, [&](char* out) {
        for (const auto value : values) {
            const uint64_t bits = static_cast<uint64_t>(value);
            for (size_t i = 0; i < 8; ++i) *out++ = static_cast<char>(bits >> (56 - 8 * i));
        }
    });
    return result;
}

// Helper: decode 8-byte big-endian string to int64_t
static int64_t be_string_to_long(std::string_view s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) { v = (v << 8) | p[i]; }
    return static_cast<int64_t>(v);
}

// ============================================================================
//  ListState: get long elements — returns long[] directly (zero TypeSerializer)
// ============================================================================

JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGetLongElements(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jlongArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        ElementList* vec = table->get(keyGroup, static_cast<int64_t>(key));
        if (!vec || vec->empty()) return nullptr;
        jsize count = static_cast<jsize>(vec->size());
        const auto data = vec->payload();
        if (size_t(count) * 8 != data.size()) throw std::runtime_error("Invalid ListState long payload");
        std::vector<jlong> longs(count);
        for (jsize i = 0; i < count; ++i) longs[i] = be_string_to_long(data.substr(size_t(i) * 8, 8));
        jlongArray arr = env->NewLongArray(count);
        env->SetLongArrayRegion(arr, 0, count, longs.data());
        return arr;
    })
}

// ============================================================================
//  ListState: update with long[] — replace entire list (zero TypeSerializer)
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUpdateLongElements(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray elements) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        jsize count = env->GetArrayLength(elements);
        std::vector<jlong> longs(count);
        env->GetLongArrayRegion(elements, 0, count, longs.data());
        auto vec = encode_longs(longs);
        list_update(table, std::move(vec), keyGroup, static_cast<int64_t>(key));
    })
}

// ============================================================================
//  ListState: addAll with long[] — append multiple long elements (zero TypeSerializer)
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddAllLongElements(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jlongArray elements) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        int64_t k = static_cast<int64_t>(key);
        jsize count = env->GetArrayLength(elements);
        std::vector<jlong> longs(count);
        env->GetLongArrayRegion(elements, 0, count, longs.data());
        auto new_elems = encode_longs(longs);
        list_append(table, std::move(new_elems), keyGroup, k);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAdd(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        int64_t k = static_cast<int64_t>(key);
        NamespaceBlob elem = jbytearray_to_string(env, element);

        list_add(table, std::move(elem), keyGroup, k);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto elem = jbytearray_to_string(env, element);
        with_serialized_list(env, handle, key, [&](auto* table, const auto&... args) {
            list_add(table, std::move(elem), keyGroup, args...);
        });
    })
}

// ============================================================================
//  ListState: update — replace entire list
//  Input: [count(4 bytes BE)][elem1_bytes][elem2_bytes]...
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listWriteWithTW(
        JNIEnv* env, jclass, jlong stateHandle, jlong key, jint keyGroup,
        jlong nsStart, jlong nsEnd, jbyteArray serializedList, jboolean append) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        NamespaceBlob raw = jbytearray_to_string(env, serializedList);
        ElementList elements = ElementList::from_serialized(raw);
        TimeWindow ns(nsStart, nsEnd);
        if (append) list_append(table, std::move(elements), keyGroup, ns, static_cast<int64_t>(key));
        else list_update(table, std::move(elements), keyGroup, ns, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUpdate(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray serializedList) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        NamespaceBlob raw = jbytearray_to_string(env, serializedList);
        ElementList vec = ElementList::from_serialized(raw);
        list_update(table, std::move(vec), keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUpdateGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray serializedList) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto raw = jbytearray_to_string(env, serializedList);
        auto vec = ElementList::from_serialized(raw);
        with_serialized_list(env, handle, key, [&](auto* table, const auto&... args) {
            list_update(table, std::move(vec), keyGroup, args...);
        });
    })
}

// ============================================================================
//  ListState: addAll — append multiple elements
//  Input: [count(4 bytes BE)][elem1_bytes][elem2_bytes]...
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddAll(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray serializedElements) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        int64_t k = static_cast<int64_t>(key);
        NamespaceBlob raw = jbytearray_to_string(env, serializedElements);
        ElementList new_elems = ElementList::from_serialized(raw);
        if (new_elems.empty()) return;

        list_append(table, std::move(new_elems), keyGroup, k);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddAllGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray serializedElements) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto raw = jbytearray_to_string(env, serializedElements);
        auto elements = ElementList::from_serialized(raw);
        if (elements.empty()) return;
        with_serialized_list(env, handle, key, [&](auto* table, const auto&... args) {
            list_append(table, std::move(elements), keyGroup, args...);
        });
    })
}

// ============================================================================
//  ListState: clear
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listClear(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->table<int64_t, ElementList>();
        list_clear(table, keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        with_serialized_list(env, handle, key, [&](auto* table, const auto&... args) {
            list_clear(table, keyGroup, args...);
        });
    })
}

// ============================================================================
//  ListState: mergeNamespaces (for window triggers)
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listMergeNamespaces(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup,
        jlong targetNamespace, jlongArray sourceNamespaces) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        if (!sourceNamespaces) return;

        jsize numSources = env->GetArrayLength(sourceNamespaces);
        if (numSources == 0) return;

        std::vector<jlong> srcNs(numSources);
        env->GetLongArrayRegion(sourceNamespaces, 0, numSources, srcNs.data());

        std::vector<int64_t> sources;
        sources.reserve(numSources);
        for (jsize i = 0; i < numSources; ++i) {
            sources.push_back(static_cast<int64_t>(srcNs[i]));
        }

        // This legacy entry point merges every key in the namespaces.
        // Merge: concatenate element vectors
        auto merge_lists = [](const ElementList& a, ElementList&& b) -> ElementList {
            ElementList result = a;
            result.append_list(b);
            return result;
        };

        if (handle->numeric_window_lists) {
            if (handle->namespace_serialized_keys) handle->table<NamespaceBlob, ElementList>()->merge_namespaces(
                    keyGroup, int64_t(targetNamespace), sources, merge_lists);
            else handle->table<WindowStateKey, ElementList>()->merge_namespaces(
                    keyGroup, int64_t(targetNamespace), sources, merge_lists);
            return;
        }
        if (handle->key_type == StateHandle::KeyType::INT64) {
            auto* table = handle->table<int64_t, ElementList>();
            if (!table) return;
            table->merge_namespaces<int64_t>(
                keyGroup, static_cast<int64_t>(targetNamespace), sources, merge_lists);
        } else {
            auto* table = handle->table<NamespaceBlob, ElementList>();
            if (!table) return;
            table->merge_namespaces<int64_t>(
                keyGroup, static_cast<int64_t>(targetNamespace), sources, merge_lists);
        }
    })
}

}  // extern "C"
