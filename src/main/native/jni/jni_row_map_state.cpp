// RowData user keys arrive as a range of their existing BinaryRowData array.
// Keep the same [length BE][row bytes] identity and canonical map encoding.
#include "state_cache_access.h"
using namespace forl0;

namespace {
std::string row_key(JNIEnv* env, jbyteArray array, jint offset, jint size) {
    if (!array || offset < 0 || size < 0 || offset > env->GetArrayLength(array) - size)
        throw std::invalid_argument("invalid map row key range");
    std::string key(size_t(size) + 4, '\0');
    uint32_t length = static_cast<uint32_t>(size);
    for (int i = 0; i < 4; ++i) key[i] = char(length >> (24 - 8 * i));
    env->GetByteArrayRegion(array, offset, size, reinterpret_cast<jbyte*>(key.data() + 4));
    return key;
}

std::optional<std::string_view> get_row_value(StateHandle* h, int group, int64_t key, const std::string& user) {
    MapCallCache cache(h, group, key);
    std::string_view bytes;
    if (cache.get(user, &bytes)) return bytes;
    auto* map = h->engine->get_state_table<int64_t, InnerMap>(h->table_id)->get(group, key);
    if (!map) return std::nullopt;
    auto found = map->find(user);
    if (found == map->end()) return std::nullopt;
    cache.put(user, found->second);
    return std::string_view(found->second);
}

template<class Copy>
void put_row_value(StateHandle* h, int group, int64_t key, std::string&& user, size_t size, Copy&& copy) {
    MapCallCache cache(h, group, key);
    cache.prepare(user);
    auto* table = h->engine->get_state_table<int64_t, InnerMap>(h->table_id);
    std::string* stored = nullptr;
    auto update = [&](InnerMap& map) {
        auto& bytes = map[std::move(user)];
        bytes.resize(size);
        copy(bytes.data());
        stored = &bytes;
    };
    if (!table->modify_in_place(group, key, update)) {
        InnerMap map; update(map);
        table->put(group, key, std::move(map));
    }
    cache.put_prepared(*stored);
}
}

extern "C" {
JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyInt(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* h = from_handle<StateHandle>(state);
        std::string user = row_key(env, array, offset, size);
        auto bytes = get_row_value(h, group, key, user);
        if (!bytes) return 0;
        if (bytes->size() != 4) throw std::runtime_error("invalid integer map value width");
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<uint8_t>((*bytes)[i]);
        // Bit 32 records presence; all signed int values remain representable.
        return static_cast<jlong>((uint64_t(1) << 32) | value);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyInt(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size, jint value) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        std::string user = row_key(env, array, offset, size);
        put_row_value(h, group, key, std::move(user), 4, [&](char* bytes) {
            uint32_t bits = static_cast<uint32_t>(value);
            for (int i = 0; i < 4; ++i) bytes[i] = char(bits >> (24 - 8 * i));
        });
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyBytes(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* h = from_handle<StateHandle>(state);
        std::string user = row_key(env, array, offset, size);
        auto bytes = get_row_value(h, group, key, user);
        return bytes ? string_to_jbytearray(env, *bytes) : nullptr;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyBytes(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size, jbyteArray value) {
    JNI_ENTRY_VOID({
        if (!value) throw std::invalid_argument("null serialized map value");
        auto* h = from_handle<StateHandle>(state);
        std::string user = row_key(env, array, offset, size);
        jsize length = env->GetArrayLength(value);
        put_row_value(h, group, key, std::move(user), size_t(length), [&](char* bytes) {
            env->GetByteArrayRegion(value, 0, length, reinterpret_cast<jbyte*>(bytes));
        });
    })
}
} // extern "C"
