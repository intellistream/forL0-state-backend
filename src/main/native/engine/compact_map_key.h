#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <string_view>

namespace forl0 {

// Immutable map keys need their resource only when they own an external allocation.
// The complete serialized three- or four-field fixed BinaryRow fits beside its value.
template<size_t InlineBytes>
class SerializedMapKey {
public:
    using allocator_type = std::pmr::polymorphic_allocator<char>;
    static constexpr size_t inline_bytes = InlineBytes;

    explicit SerializedMapKey(allocator_type = {}) noexcept {}
    SerializedMapKey(const std::pmr::string& key, allocator_type allocator = {})
            : SerializedMapKey(std::string_view(key), allocator.resource(), false) {}
    SerializedMapKey(const SerializedMapKey& key, allocator_type allocator = {})
            : SerializedMapKey(key, allocator.resource(), false) {}
    SerializedMapKey(SerializedMapKey&& key) noexcept { take(key); }
    SerializedMapKey(SerializedMapKey&& key, allocator_type allocator) {
        if (key.size() <= inline_bytes || key.external().resource == allocator.resource()) take(key);
        else {
            SerializedMapKey copy(key, allocator);
            take(copy);
        }
    }
    SerializedMapKey& operator=(SerializedMapKey&& key) noexcept {
        if (this != &key) { release(); take(key); }
        return *this;
    }
    ~SerializedMapKey() { release(); }

    // Only for synchronous lookups. Node and packed-entry constructors always
    // receive their table allocator and own the stored key.
    static SerializedMapKey probe(std::string_view bytes) { return SerializedMapKey(bytes, nullptr, true); }
    const char* data() const noexcept {
        return size() <= inline_bytes ? storage_ : external().data;
    }
    size_t size() const noexcept { return length_; }
    static constexpr size_t length_offset() noexcept { return offsetof(SerializedMapKey, length_); }
    operator std::string_view() const noexcept { return {data(), size()}; }
    bool operator==(const SerializedMapKey& other) const noexcept {
        return std::string_view(*this) == std::string_view(other);
    }

private:
    struct External { const char* data; std::pmr::memory_resource* resource; };
    SerializedMapKey(std::string_view key, std::pmr::memory_resource* resource, bool borrowed) {
        if (key.size() > UINT32_MAX) throw std::length_error("serialized map key is too large");
        length_ = static_cast<uint32_t>(key.size());
        if (size() <= inline_bytes) {
            if (size()) std::memcpy(storage_, key.data(), size());
        } else {
            const char* data = key.data();
            if (!borrowed) {
                auto* copy = static_cast<char*>(resource->allocate(size(), alignof(char)));
                std::memcpy(copy, key.data(), size());
                data = copy;
            }
            External ext{data, resource};
            std::memcpy(storage_, &ext, sizeof(ext));
        }
    }
    External external() const noexcept {
        External ext;
        std::memcpy(&ext, storage_, sizeof(ext));
        return ext;
    }
    void release() noexcept {
        if (size() > inline_bytes) {
            const auto ext = external();
            if (ext.resource) ext.resource->deallocate(const_cast<char*>(ext.data), size(), alignof(char));
        }
    }
    void take(SerializedMapKey& key) noexcept {
        length_ = key.length_;
        std::memcpy(storage_, key.storage_, size() <= inline_bytes ? size() : sizeof(External));
        key.length_ = 0;
    }

    alignas(void*) char storage_[inline_bytes];
    uint32_t length_ = 0;
};
using CompactMapKey = SerializedMapKey<44>;
// Five-field and wider BinaryRows always need external bytes. Keep their complete key
// contiguous and use the smaller header for the stored key and four-byte value.
using WideMapKey = SerializedMapKey<20>;
template<class T> inline constexpr bool is_serialized_map_key = false;
template<size_t N> inline constexpr bool is_serialized_map_key<SerializedMapKey<N>> = true;
static_assert(sizeof(WideMapKey) == 24);
static_assert(sizeof(CompactMapKey) == 48);

} // namespace forl0

template<size_t N> struct std::hash<forl0::SerializedMapKey<N>> {
    size_t operator()(const forl0::SerializedMapKey<N>& key) const noexcept {
        return std::hash<std::string_view>{}(key);
    }
};
