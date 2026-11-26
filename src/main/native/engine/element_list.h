#pragma once

#include "allocator.h"
#include "namespace_row.h"

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace forl0 {

// Flink serializes a list as a big-endian count followed by consecutive
// serialized elements. Store those same bytes in the namespace's allocation.
class ElementList {
public:
    using allocator_type = NamespaceBlob::allocator_type;

    ElementList() = default;
    explicit ElementList(allocator_type alloc) : bytes_(alloc) {}
    ElementList(std::initializer_list<std::string_view> elements, allocator_type alloc = {})
        : ElementList(alloc) {
        for (auto element : elements) append(element);
    }
    ElementList(const ElementList&) = default;
    ElementList(ElementList&&) = default;
    ElementList& operator=(const ElementList&) = default;
    ElementList& operator=(ElementList&&) = default;
    ElementList(const ElementList& source, allocator_type alloc) : bytes_(source.bytes_, alloc) {}
    ElementList(ElementList&& source, allocator_type alloc) : bytes_(std::move(source.bytes_), alloc) {}

    bool empty() const noexcept { return bytes_.empty(); }
    uint32_t size() const noexcept { return empty() ? 0 : read_count(bytes_.data()); }
    const NamespaceRow& storage() const noexcept { return bytes_; }
    std::string_view serialized() const noexcept {
        return empty() ? std::string_view("\0\0\0\0", 4) : std::string_view(bytes_);
    }
    std::string_view payload() const noexcept { return serialized().substr(4); }

    static uint32_t serialized_count(std::string_view bytes) {
        if (bytes.size() < 4 || bytes.size() > limit)
            throw std::invalid_argument("Invalid serialized ListState length");
        const uint32_t count = read_count(bytes.data());
        if (count > limit || (!count && bytes.size() != 4))
            throw std::invalid_argument("Invalid serialized ListState count");
        return count;
    }

    void assign_serialized(std::string_view bytes) {
        const auto count = serialized_count(bytes);
        bytes_.assign(bytes.data(), count ? bytes.size() : 0);
    }

    static ElementList from_serialized(std::string_view bytes) {
        ElementList result;
        result.assign_serialized(bytes);
        return result;
    }

    void append(std::string_view element) {
        const auto source = reinterpret_cast<uintptr_t>(element.data());
        const auto base = reinterpret_cast<uintptr_t>(bytes_.data());
        const bool internal = source >= base && source - base < bytes_.size();
        append_bytes(1, element.size(), [&](char* out) {
            const char* data = internal ? bytes_.data() + (source - base) : element.data();
            if (!element.empty()) std::memmove(out, data, element.size());
        });
    }
    void append_list(const ElementList& other) {
        if (other.empty()) return;
        if (this == &other) {
            ElementList copy(other);
            append_list(copy);
            return;
        }
        const auto data = other.payload();
        append_bytes(other.size(), data.size(), [&](char* out) {
            if (!data.empty()) std::memcpy(out, data.data(), data.size());
        });
    }

    template<class Fill>
    void append_bytes(uint32_t count, size_t length, Fill fill) {
        const size_t before = bytes_.size();
        const size_t start = before ? before : 4;
        const uint32_t old_count = size();
        if (count > limit - old_count || length > limit - start)
            throw std::length_error("Serialized ListState exceeds Flink length limit");
        bytes_.resize(start + length, true);
        try {
            fill(bytes_.data() + start);
        } catch (...) {
            bytes_.resize(before);
            throw;
        }
        const uint32_t next = old_count + count;
        for (size_t i = 0; i < 4; ++i) bytes_.data()[i] = static_cast<char>(next >> (24 - 8 * i));
    }

private:
    static constexpr uint32_t limit = std::numeric_limits<int32_t>::max();
    static uint32_t read_count(const char* data) noexcept {
        const auto* p = reinterpret_cast<const uint8_t*>(data);
        return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3];
    }
    NamespaceRow bytes_;
};

}  // namespace forl0
