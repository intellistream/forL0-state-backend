// A serialized Flink map key is its big-endian length followed by its BinaryRowData bytes.
#include "state_cache_access.h"
using namespace forl0;

namespace {
class RowInput {
public:
    RowInput(JNIEnv* env, jbyteArray array, jint offset, jint size)
        : size(size), env_(env), array_(array), offset_(offset) {
        if (!array || offset < 0 || size < 0 || offset > env->GetArrayLength(array) - size)
            throw std::invalid_argument("invalid map row key range");
    }
    RowInput(jobject owner, jlong address, jint size)
        : size(size), direct(reinterpret_cast<const char*>(address)) {
        if (!owner || !address || size < 0) throw std::invalid_argument("invalid direct map row key");
    }
    void copy(char* target) const {
        if (direct) std::memcpy(target, direct, size);
        else env_->GetByteArrayRegion(array_, offset_, size, reinterpret_cast<jbyte*>(target));
    }
    jint size;
    const char* direct = nullptr;
private:
    JNIEnv* env_ = nullptr;
    jbyteArray array_ = nullptr;
    jint offset_ = 0;
};

class RowKey {
public:
    explicit RowKey(const RowInput& input) {
        value_.resize(size_t(input.size) + 4);
        const auto length = static_cast<uint32_t>(input.size);
        for (int i = 0; i < 4; ++i) value_[i] = char(length >> (24 - 8 * i));
        input.copy(value_.data() + 4);
    }
    const NamespaceBlob& value() const { return value_; }
private:
    char buffer_[256];
    std::pmr::monotonic_buffer_resource storage_{buffer_, sizeof(buffer_)};
    NamespaceBlob value_{&storage_};
};

template<class Map>
std::optional<std::string_view> find_row(const Map& map, const RowInput& input) {
    char scratch[1024];
    if (map.supports_compact_lookup() && (input.direct || size_t(input.size) <= sizeof(scratch))) {
        const char* raw = input.direct;
        auto bytes = [&]() {
            if (!raw) { input.copy(scratch); raw = scratch; }
            return std::string_view(raw, input.size);
        };
        const auto size = static_cast<uint32_t>(input.size);
        const uint8_t prefix[] = {uint8_t(size >> 24), uint8_t(size >> 16), uint8_t(size >> 8), uint8_t(size)};
        const size_t stored_length = size_t(size) + sizeof(prefix);
        const auto* value = map.find_compact_value([&] { return packed_byte_key_tag(bytes(), stored_length); },
                [&](const auto& key) {
                    return key.size() == stored_length && std::memcmp(key.data(), prefix, sizeof(prefix)) == 0
                            && std::memcmp(key.data() + sizeof(prefix), bytes().data(), size) == 0;
                });
        return value ? std::optional<std::string_view>({value->data(), value->size()}) : std::nullopt;
    }
    const RowKey encoded(input);
    const auto* found = map.find_value(encoded.value());
    return found ? std::optional<std::string_view>({found->data(), found->size()}) : std::nullopt;
}

std::optional<std::string_view> get_row_value(StateHandle* h, int group, int64_t key, const RowInput& input) {
    return visit_serialized_map(h, [&](auto& table) -> std::optional<std::string_view> {
        const auto* map = table.get(group, key);
        return map ? find_row(*map, input) : std::nullopt;
    });
}

template<class Copy>
void put_row_value(StateHandle* h, int group, int64_t key, const RowInput& input, size_t size, Copy&& copy) {
    check_inline_map_width(*h, size);
    // A direct input may point into another namespace in this very pool. Own the key before any allocation or move.
    const RowKey user(input);
    visit_serialized_map(h, [&](auto& table) {
        table.update_or_create(group, key, [&](auto& map) {
            auto entry = map.try_emplace(user.value());
            auto& bytes = entry.first->second;
            bytes.resize(size);
            copy(bytes.data());
        });
    });
}

jlong get_int(StateHandle* h, jint group, jlong key, const RowInput& input) {
    auto bytes = get_row_value(h, group, key, input);
    if (!bytes) return 0;
    if (bytes->size() != 4) throw std::runtime_error("invalid integer map value width");
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) value = (value << 8) | static_cast<uint8_t>((*bytes)[i]);
    return static_cast<jlong>((uint64_t(1) << 32) | value);
}

void put_int(StateHandle* h, jint group, jlong key, const RowInput& input, jint value) {
    put_row_value(h, group, key, input, 4, [&](char* bytes) {
        const auto bits = static_cast<uint32_t>(value);
        for (int i = 0; i < 4; ++i) bytes[i] = char(bits >> (24 - 8 * i));
    });
}

jbyteArray get_bytes(JNIEnv* env, StateHandle* h, jint group, jlong key, const RowInput& input) {
    auto bytes = get_row_value(h, group, key, input);
    return bytes ? string_to_jbytearray(env, *bytes) : nullptr;
}

void put_bytes(JNIEnv* env, StateHandle* h, jint group, jlong key, const RowInput& input, jbyteArray value) {
    if (!value) throw std::invalid_argument("null serialized map value");
    const auto size = env->GetArrayLength(value);
    put_row_value(h, group, key, input, size, [&](char* bytes) {
        env->GetByteArrayRegion(value, 0, size, reinterpret_cast<jbyte*>(bytes));
    });
}
}  // namespace

extern "C" {
JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyInt(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size) {
    JNI_ENTRY_RETURN(jlong, 0, {
        return get_int(from_handle<StateHandle>(state), group, key, RowInput(env, array, offset, size));
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyIntAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jobject owner, jlong address, jint size) {
    JNI_ENTRY_RETURN(jlong, 0, {
        return get_int(from_handle<StateHandle>(state), group, key, RowInput(owner, address, size));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyInt(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size, jint value) {
    JNI_ENTRY_VOID({
        put_int(from_handle<StateHandle>(state), group, key, RowInput(env, array, offset, size), value);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyIntAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jobject owner, jlong address, jint size, jint value) {
    JNI_ENTRY_VOID({
        put_int(from_handle<StateHandle>(state), group, key, RowInput(owner, address, size), value);
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyBytes(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        return get_bytes(env, from_handle<StateHandle>(state), group, key, RowInput(env, array, offset, size));
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapGetRowKeyBytesAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jobject owner, jlong address, jint size) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        return get_bytes(env, from_handle<StateHandle>(state), group, key, RowInput(owner, address, size));
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyBytes(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jbyteArray array, jint offset, jint size, jbyteArray value) {
    JNI_ENTRY_VOID({
        put_bytes(env, from_handle<StateHandle>(state), group, key, RowInput(env, array, offset, size), value);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_mapPutRowKeyBytesAddress(
        JNIEnv* env, jclass, jlong state, jlong key, jint group,
        jobject owner, jlong address, jint size, jbyteArray value) {
    JNI_ENTRY_VOID({
        put_bytes(env, from_handle<StateHandle>(state), group, key, RowInput(owner, address, size), value);
    })
}

}  // extern "C"
