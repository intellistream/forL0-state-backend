// jni_list_state.cpp — JNI entry points for ListState operations.
// ListState stores ElementList (std::vector<std::string>) in SwissTable<K, ElementList>.
// Each element in the vector is the raw serialized bytes from Java's TypeSerializer.
// Individual add() is a simple O(1) push_back; get() serializes the vector.

#include <jni.h>
#include "jni_utils.h"
#include "state_engine.h"
#include "flink_binary_format.h"

using namespace forl0;

extern "C" {

// ============================================================================
//  ListState: get — returns [count(4 bytes BE)][elem1_bytes][elem2_bytes]...
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGet(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        ElementList* vec = table->get(keyGroup, static_cast<int64_t>(key));
        if (!vec || vec->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(vec->size()));
        for (const auto& elem : *vec) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(elem.data()), elem.size());
        }
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listGetGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        ElementList* vec = table->get(keyGroup, k);
        if (!vec || vec->empty()) return nullptr;
        WriteBuffer buf;
        buf.write_int(static_cast<int32_t>(vec->size()));
        for (const auto& elem : *vec) {
            buf.write_raw(reinterpret_cast<const uint8_t*>(elem.data()), elem.size());
        }
        return string_to_jbytearray(env,
            std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        // Serialize the long element as 8-byte big-endian
        int64_t elem = static_cast<int64_t>(element);
        uint8_t buf[8];
        for (int i = 7; i >= 0; --i) { buf[i] = static_cast<uint8_t>(elem & 0xFF); elem >>= 8; }
        std::string elem_str(reinterpret_cast<const char*>(buf), 8);

        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->push_back(std::move(elem_str));
        } else {
            ElementList new_vec;
            new_vec.push_back(std::move(elem_str));
            table->put(keyGroup, k, std::move(new_vec));
        }
    })
}

// Helper: encode int64_t as 8-byte big-endian string
static std::string long_to_be_string(int64_t v) {
    uint8_t buf[8];
    for (int i = 7; i >= 0; --i) { buf[i] = static_cast<uint8_t>(v & 0xFF); v >>= 8; }
    return std::string(reinterpret_cast<const char*>(buf), 8);
}

// Helper: decode 8-byte big-endian string to int64_t
static int64_t be_string_to_long(const std::string& s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    int64_t v = 0;
    for (int i = 0; i < 8; ++i) { v = (v << 8) | p[i]; }
    return v;
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        ElementList* vec = table->get(keyGroup, static_cast<int64_t>(key));
        if (!vec || vec->empty()) return nullptr;
        jsize count = static_cast<jsize>(vec->size());
        std::vector<jlong> longs(count);
        for (jsize i = 0; i < count; ++i) {
            longs[i] = be_string_to_long((*vec)[i]);
        }
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        jsize count = env->GetArrayLength(elements);
        std::vector<jlong> longs(count);
        env->GetLongArrayRegion(elements, 0, count, longs.data());
        ElementList vec;
        vec.reserve(count);
        for (jsize i = 0; i < count; ++i) {
            vec.push_back(long_to_be_string(longs[i]));
        }
        table->put(keyGroup, static_cast<int64_t>(key), std::move(vec));
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        jsize count = env->GetArrayLength(elements);
        std::vector<jlong> longs(count);
        env->GetLongArrayRegion(elements, 0, count, longs.data());
        ElementList new_elems;
        new_elems.reserve(count);
        for (jsize i = 0; i < count; ++i) {
            new_elems.push_back(long_to_be_string(longs[i]));
        }
        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->insert(vec->end(),
                std::make_move_iterator(new_elems.begin()),
                std::make_move_iterator(new_elems.end()));
        } else {
            table->put(keyGroup, k, std::move(new_elems));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAdd(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string elem = jbytearray_to_string(env, element);

        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->push_back(std::move(elem));
        } else {
            ElementList new_vec;
            new_vec.push_back(std::move(elem));
            table->put(keyGroup, k, std::move(new_vec));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray element) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string elem = jbytearray_to_string(env, element);

        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->push_back(std::move(elem));
        } else {
            ElementList new_vec;
            new_vec.push_back(std::move(elem));
            table->put(keyGroup, k, std::move(new_vec));
        }
    })
}

// ============================================================================
//  ListState: update — replace entire list
//  Input: [count(4 bytes BE)][elem1_bytes][elem2_bytes]...
// ============================================================================

// Helper: parse serialized elements into a vector using element_layout to determine boundaries.
static ElementList parse_serialized_list(const uint8_t* data, size_t len,
                                         const TypeLayout* elem_layout) {
    ElementList result;
    if (len < 4) return result;
    ReadBuffer reader(data, len);
    int32_t count = reader.read_int();
    result.reserve(count);
    for (int32_t i = 0; i < count && reader.remaining() > 0; ++i) {
        if (elem_layout) {
            result.push_back(reader.read_element_bytes(*elem_layout));
        } else {
            // No layout info — store remaining bytes as single element (fallback)
            size_t rem = reader.remaining();
            std::string s(reinterpret_cast<const char*>(data + (len - rem)), rem);
            result.push_back(std::move(s));
            break;
        }
    }
    return result;
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUpdate(
        JNIEnv* env, jclass,
        jlong stateHandle, jlong key, jint keyGroup, jbyteArray serializedList) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        std::string raw = jbytearray_to_string(env, serializedList);
        ElementList vec = parse_serialized_list(
            reinterpret_cast<const uint8_t*>(raw.data()), raw.size(),
            handle->get_element_layout());
        table->put(keyGroup, static_cast<int64_t>(key), std::move(vec));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listUpdateGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray serializedList) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string raw = jbytearray_to_string(env, serializedList);
        ElementList vec = parse_serialized_list(
            reinterpret_cast<const uint8_t*>(raw.data()), raw.size(),
            handle->get_element_layout());
        table->put(keyGroup, k, std::move(vec));
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        int64_t k = static_cast<int64_t>(key);
        std::string raw = jbytearray_to_string(env, serializedElements);
        ElementList new_elems = parse_serialized_list(
            reinterpret_cast<const uint8_t*>(raw.data()), raw.size(),
            handle->get_element_layout());
        if (new_elems.empty()) return;

        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->insert(vec->end(),
                std::make_move_iterator(new_elems.begin()),
                std::make_move_iterator(new_elems.end()));
        } else {
            table->put(keyGroup, k, std::move(new_elems));
        }
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listAddAllGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup, jbyteArray serializedElements) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        std::string raw = jbytearray_to_string(env, serializedElements);
        ElementList new_elems = parse_serialized_list(
            reinterpret_cast<const uint8_t*>(raw.data()), raw.size(),
            handle->get_element_layout());
        if (new_elems.empty()) return;

        ElementList* vec = table->get(keyGroup, k);
        if (vec) {
            vec->insert(vec->end(),
                std::make_move_iterator(new_elems.begin()),
                std::make_move_iterator(new_elems.end()));
        } else {
            table->put(keyGroup, k, std::move(new_elems));
        }
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
        auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
        table->remove(keyGroup, static_cast<int64_t>(key));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_listClearGeneric(
        JNIEnv* env, jclass,
        jlong stateHandle, jbyteArray key, jint keyGroup) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
        std::string k = jbytearray_to_string(env, key);
        table->remove(keyGroup, k);
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

        // Merge: concatenate element vectors
        auto merge_lists = [](const ElementList& a, ElementList&& b) -> ElementList {
            ElementList result = a;
            result.insert(result.end(),
                std::make_move_iterator(b.begin()),
                std::make_move_iterator(b.end()));
            return result;
        };

        if (handle->key_type == StateHandle::KeyType::INT64) {
            auto* table = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
            if (!table) return;
            table->merge_namespaces<int64_t>(
                keyGroup, static_cast<int64_t>(targetNamespace), sources, merge_lists);
        } else {
            auto* table = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
            if (!table) return;
            table->merge_namespaces<int64_t>(
                keyGroup, static_cast<int64_t>(targetNamespace), sources, merge_lists);
        }
    })
}

}  // extern "C"
