// Numeric arguments for SQL windows, retaining the original serialized table
// and checkpoint encodings. Cache hits do not allocate temporary string keys.
#include "state_cache_access.h"
#include "window_map_storage.h"
using namespace forl0;

namespace {
void write_be(char* out, int64_t number) {
    uint64_t bits = static_cast<uint64_t>(number);
    for (int i = 0; i < 8; ++i) out[i] = char(bits >> (56 - i * 8));
}
int64_t read_be(const char* in) {
    uint64_t bits = 0;
    for (int i = 0; i < 8; ++i) bits = (bits << 8) | static_cast<unsigned char>(in[i]);
    return static_cast<int64_t>(bits);
}
struct WindowBytes {
    char data[16];
    WindowBytes(int64_t start, int64_t end) { write_be(data, start); write_be(data + 8, end); }
    std::string_view view() const { return {data, sizeof(data)}; }
};
struct SqlWindowKey {
    char data[36];
    size_t size;
    SqlWindowKey(StateHandle* h, int64_t key, int64_t start, int64_t end, uint64_t header) {
        write_be(data, start); write_be(data + 8, end);
        if (h->row_data_key) {
            data[16] = data[17] = data[18] = 0; data[19] = 16;
            for (int i = 0; i < 8; ++i) {
                data[20 + i] = char(header >> (8 * i));
                data[28 + i] = char(static_cast<uint64_t>(key) >> (8 * i));
            }
            size = 36;
        } else { write_be(data + 16, key); size = 24; }
    }
    std::string_view view() const { return {data, size}; }
    std::string owned() const { return {data, size}; }
};
std::optional<std::string_view> sql_window_get(StateHandle* h, int group, const SqlWindowKey& key) {
    if (h->serialized_cache) {
        StateCacheKey identity(group, key.view());
        std::string_view value;
        if (h->serialized_cache->get_view(identity.view(), &value)) return value;
    }
    auto* table = h->table<std::string, std::string>();
    auto* value = table->get_string_view(group, key.view());
    // Do not evict borrowed positive views while servicing another read.
    return value ? std::optional<std::string_view>(*value) : std::nullopt;
}
template<class Copy>
void sql_window_put(StateHandle* h, int group, const SqlWindowKey& key, size_t length, Copy&& copy) {
    auto* table = h->table<std::string, std::string>();
    std::string* stored = nullptr;
    auto update = [&](std::string& value) {
        value.resize(length);
        copy(value.data());
        stored = &value;
    };
    try {
        if (!table->modify_string_view(group, key.view(), update)) {
            std::string value;
            update(value);
            stored = table->put(group, key.owned(), std::move(value));
        }
        if (h->serialized_cache) {
            StateCacheKey identity(group, key.view());
            h->serialized_cache->put(identity.view(), *stored);
        }
    } catch (...) {
        if (h->serialized_cache) {
            StateCacheKey identity(group, key.view());
            h->serialized_cache->invalidate(identity.view());
        }
        throw;
    }
}
}

extern "C" {
JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueGetSqlWindowPtr(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jlong start, jlong end, jlong header) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        auto value = sql_window_get(h, group, SqlWindowKey(h, key, start, end, header));
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
        auto value = sql_window_get(h, group, SqlWindowKey(h, key, start, end, header));
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
        sql_window_put(h, group, SqlWindowKey(h, key, start, end, header), size_t(length), [&](char* target) {
            env->GetByteArrayRegion(bytes, offset, length, reinterpret_cast<jbyte*>(target));
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valuePutSqlWindowAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header,
        jobject owner, jlong address, jint length) {
    JNI_ENTRY_VOID({
        if (!owner || !address || length < 0) throw std::invalid_argument("invalid SQL window native row");
        auto* h = from_handle<StateHandle>(state);
        // owner pins the borrowed MemorySegment throughout this synchronous copy.
        sql_window_put(h, group, SqlWindowKey(h, key, start, end, header), size_t(length), [&](char* target) {
            std::memmove(target, reinterpret_cast<const void*>(address), size_t(length));
        });
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_valueClearSqlWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end, jlong header) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        auto* table = h->table<std::string, std::string>();
        std::string encoded = SqlWindowKey(h, key, start, end, header).owned();
        cached_remove(h, table, int(group), encoded);
    })
}

JNIEXPORT jboolean JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end) {
    JNI_ENTRY_RETURN(jboolean, JNI_FALSE, {
        auto* h = from_handle<StateHandle>(state);
        WindowBytes user(start, end);
        MapCallCache cache(h, int(group), int64_t(key));
        std::string_view value;
        if (!cache.get(user.view(), &value)) {
            auto* table = h->table<int64_t, InnerMapTimeWindow>();
            auto* map = table->get(group, int64_t(key));
            if (!map) return JNI_FALSE;
            auto it = map->find(TimeWindow(start, end));
            if (it == map->end()) return JNI_FALSE;
            WindowBytes encoded(it->second.first, it->second.second);
            cache.put(user.view(), encoded.view());
            h->read_result[0] = it->second.first;
            h->read_result[1] = it->second.second;
            return JNI_TRUE;
        }
        if (value.size() != 16) throw std::runtime_error("invalid TimeWindow map value width");
        h->read_result[0] = read_be(value.data());
        h->read_result[1] = read_be(value.data() + 8);
        return JNI_TRUE;
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jlong user_start, jlong user_end, jlong value_start, jlong value_end) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        WindowBytes user(user_start, user_end), value(value_start, value_end);
        MapCallCache cache(h, int(group), int64_t(key));
        cache.prepare(user.view());
        TimeWindow uk(user_start, user_end), uv(value_start, value_end);
        auto* table = h->table<int64_t, InnerMapTimeWindow>();
        put_window_entry(table, h->window_scratch, group, key, uk, uv);
        cache.put_prepared(value.view());
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapRemoveTimeWindow(
        JNIEnv* env, jclass, jlong state, jlong key, jint group, jlong start, jlong end) {
    JNI_ENTRY_VOID({
        auto* h = from_handle<StateHandle>(state);
        WindowBytes user(start, end);
        MapCallCache cache(h, int(group), int64_t(key));
        cache.remove(user.view());
        auto* table = h->table<int64_t, InnerMapTimeWindow>();
        remove_window_entry(table, h->window_scratch, group, key, TimeWindow(start, end));
    })
}
}
