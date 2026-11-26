#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <string_view>
#include <type_traits>
#include <utility>

namespace forl0 {

// The memory resource owns both small and external values. Its alignment leaves
// two bits for the representation, keeping a full 24-byte row in a 32-byte slot.
class NamespaceRow {
public:
    using allocator_type = std::pmr::polymorphic_allocator<char>;
    static constexpr size_t inline_bytes = 24;
    static constexpr unsigned slot_format = 252;

    explicit NamespaceRow(allocator_type allocator = {}) noexcept
        : owner_and_kind_(reinterpret_cast<uintptr_t>(allocator.resource())) {}
    NamespaceRow(const char* bytes, size_t length, allocator_type allocator = {})
        : NamespaceRow(allocator) { assign(bytes, length); }
    template<class Text, std::enable_if_t<std::is_convertible_v<const Text&, std::string_view>
            && !std::is_same_v<std::decay_t<Text>, NamespaceRow>, int> = 0>
    NamespaceRow(const Text& bytes, allocator_type allocator = {}) : NamespaceRow(allocator) {
        const std::string_view view(bytes);
        assign(view.data(), view.size());
    }
    NamespaceRow(size_t count, char value, allocator_type allocator = {})
        : NamespaceRow(allocator) {
        overwrite(count, [&](char* out) noexcept { if (count) std::memset(out, value, count); });
    }
    NamespaceRow(const NamespaceRow& other, allocator_type allocator = {})
        : NamespaceRow(other.data(), other.size(), allocator) {}
    NamespaceRow(NamespaceRow&& other) noexcept : NamespaceRow(other.allocator()) { take(other); }
    NamespaceRow(NamespaceRow&& other, allocator_type target) : NamespaceRow(target) {
        if (target == other.allocator()) take(other);
        else assign(other.data(), other.size());
    }
    ~NamespaceRow() { release(); }

    template<class Text, std::enable_if_t<std::is_convertible_v<const Text&, std::string_view>
            && !std::is_same_v<std::decay_t<Text>, NamespaceRow>, int> = 0>
    NamespaceRow& operator=(const Text& bytes) {
        const std::string_view view(bytes);
        assign(view.data(), view.size());
        return *this;
    }
    bool operator==(std::string_view other) const noexcept { return std::string_view(*this) == other; }

    NamespaceRow& operator=(const NamespaceRow& other) {
        if (this != &other) assign(other.data(), other.size());
        return *this;
    }
    NamespaceRow& operator=(NamespaceRow&& other) {
        if (this == &other) return *this;
        if (allocator() == other.allocator()) {
            release();
            take(other);
        } else {
            assign(other.data(), other.size());
        }
        return *this;
    }

    size_t size() const noexcept {
        const auto code = kind();
        if (code >= two_words) return code * 8;
        return code == external ? storage_.remote.length : static_cast<unsigned char>(storage_.local[inline_bytes - 1]);
    }
    const uintptr_t* representation_address() const noexcept { return &owner_and_kind_; }
    static constexpr size_t representation_offset() { return offsetof(NamespaceRow, owner_and_kind_); }
    static constexpr size_t storage_offset() { return offsetof(NamespaceRow, storage_); }
    const char* storage_address() const noexcept { return reinterpret_cast<const char*>(&storage_); }
    bool empty() const noexcept { return size() == 0; }
    char front() const noexcept { return data()[0]; }
    char* data() noexcept { return kind() == external ? storage_.remote.ptr : storage_.local; }
    const char* data() const noexcept { return kind() == external ? storage_.remote.ptr : storage_.local; }
    operator std::string_view() const noexcept { return {data(), size()}; }

    void assign(const char* bytes, size_t length) {
        replace(length, [&](char* out) { if (length) std::memmove(out, bytes, length); });
    }
    template<class Fill>
    void overwrite(size_t length, Fill fill) {
        static_assert(std::is_nothrow_invocable_v<Fill, char*>);
        replace(length, fill);
    }
    void resize(size_t length, bool grow = false) {
        const auto previous = size();
        if (length == previous) return;
        replace(length, [&](char* out) {
            const size_t keep = std::min(previous, length);
            if (out != data() && keep) std::memcpy(out, data(), keep);
            if (length > keep) std::memset(out + keep, 0, length - keep);
        }, grow);
    }

private:
    static constexpr uintptr_t kind_mask = 3;
    enum Kind : uintptr_t { short_bytes, external, two_words, three_words };
    static_assert(alignof(std::pmr::memory_resource) > kind_mask);
    struct Remote { char* ptr; size_t length; size_t capacity; };
    static_assert(offsetof(Remote, length) == sizeof(void*));
    union Storage {
        char local[inline_bytes];
        Remote remote;
        Storage() : local{} {}
    };

    uintptr_t kind() const noexcept { return owner_and_kind_ & kind_mask; }
    void set_kind(uintptr_t code) noexcept { owner_and_kind_ = (owner_and_kind_ & ~kind_mask) | code; }
    allocator_type allocator() const noexcept {
        return allocator_type(reinterpret_cast<std::pmr::memory_resource*>(owner_and_kind_ & ~kind_mask));
    }

    template<class Fill>
    void replace(size_t length, Fill fill, bool grow = false) {
        const bool remote = kind() == external;
        if (length <= inline_bytes) {
            if (remote) {
                // The input can refer to a suffix of the old value.
                Storage next;
                fill(next.local);
                release();
                storage_ = next;
            } else {
                fill(storage_.local);
            }
            if (length == 16 || length == 24) set_kind(length / 8);
            else {
                storage_.local[inline_bytes - 1] = static_cast<char>(length);
                set_kind(short_bytes);
            }
        } else if (remote && length <= storage_.remote.capacity) {
            fill(storage_.remote.ptr);
            storage_.remote.length = length;
        } else {
            size_t capacity = length;
            if (grow && remote && storage_.remote.capacity <= SIZE_MAX / 2)
                capacity = std::max(length, storage_.remote.capacity * 2);
            char* next = allocator().allocate(capacity);
            fill(next);
            release();
            storage_.remote = {next, length, capacity};
            set_kind(external);
        }
    }
    void release() noexcept {
        if (kind() == external) allocator().deallocate(storage_.remote.ptr, storage_.remote.capacity);
    }
    void take(NamespaceRow& source) noexcept {
        storage_ = source.storage_;
        set_kind(source.kind());
        source.storage_ = Storage{};
        source.set_kind(short_bytes);
    }

    uintptr_t owner_and_kind_;
    Storage storage_;
};
static_assert(sizeof(uintptr_t) != 8 || sizeof(NamespaceRow) == 32);

// For fixed-width BinaryRowData, store the complete canonical payload in its
// namespace slot. Non-canonical lengths from serializers or old checkpoints
// still round-trip byte for byte through an allocation owned by the same arena.
template<size_t Width>
class FixedWindowRow {
public:
    static_assert(Width == 8 || Width == 16);
    using allocator_type = std::pmr::polymorphic_allocator<char>;
    static constexpr unsigned slot_format = Width == 8 ? 247 : 248;
    explicit FixedWindowRow(allocator_type a = {}) noexcept
        : owner_kind_(reinterpret_cast<uintptr_t>(a.resource())) {}
    FixedWindowRow(const char* bytes, size_t length, allocator_type a = {}) : FixedWindowRow(a) {
        assign(bytes, length);
    }
    FixedWindowRow(const FixedWindowRow& other, allocator_type a = {})
        : FixedWindowRow(other.data(), other.size(), a) {}
    FixedWindowRow(FixedWindowRow&& other) noexcept : FixedWindowRow(other.allocator()) { take(other); }
    FixedWindowRow(FixedWindowRow&& other, allocator_type a) : FixedWindowRow(a) {
        if (a == other.allocator()) take(other);
        else assign(other.data(), other.size());
    }
    ~FixedWindowRow() { release(); }
    FixedWindowRow& operator=(const FixedWindowRow& other) {
        if (this != &other) assign(other.data(), other.size());
        return *this;
    }
    FixedWindowRow& operator=(FixedWindowRow&& other) {
        if (this != &other) {
            if (allocator() == other.allocator()) {
                release();
                take(other);
            } else assign(other.data(), other.size());
        }
        return *this;
    }
    const uintptr_t* representation_address() const noexcept { return &owner_kind_; }
    const char* storage_address() const noexcept { return reinterpret_cast<const char*>(&storage_); }
    size_t size() const noexcept {
        if (kind() == inline_value) return Width;
        if (kind() == empty_value) return 0;
        size_t length;
        std::memcpy(&length, storage_.external, sizeof(length));
        return length;
    }
    const char* data() const noexcept { return kind() == external_value ? storage_.external + sizeof(size_t) : storage_.local; }
    char* data() noexcept { return const_cast<char*>(std::as_const(*this).data()); }
    operator std::string_view() const noexcept { return {data(), size()}; }
    size_t external_bytes() const noexcept { return kind() == external_value ? sizeof(size_t) + size() : 0; }

    void assign(const char* input, size_t length) {
        if (length == 0) {
            release();
            set_kind(empty_value);
        } else if (length == Width) {
            // Input may alias the allocation that is about to be released.
            if (kind() == external_value) {
                char copy[Width];
                std::memcpy(copy, input, Width);
                release();
                std::memcpy(storage_.local, copy, Width);
            } else std::memmove(storage_.local, input, Width);
            set_kind(inline_value);
        } else if (kind() == external_value && length == size()) {
            std::memmove(data(), input, length);
        } else {
            char* next = make_external(length);
            std::memcpy(next + sizeof(size_t), input, length);
            release();
            storage_.external = next;
            set_kind(external_value);
        }
    }
    // JNI fills the complete buffer before returning to Flink.
    template<class Copy> void overwrite(size_t length, Copy&& copy) {
        if (length == size()) {
            copy(data());
        } else if (length == 0) {
            release();
            set_kind(empty_value);
        } else if (length == Width) {
            char next[Width];
            copy(next);
            release();
            std::memcpy(storage_.local, next, Width);
            set_kind(inline_value);
        } else {
            auto a = allocator();
            char* next = make_external(length);
            try { copy(next + sizeof(size_t)); }
            catch (...) { a.deallocate(next, length + sizeof(size_t)); throw; }
            release();
            storage_.external = next;
            set_kind(external_value);
        }
    }
private:
    enum Kind : uintptr_t { empty_value, inline_value, external_value };
    static_assert(alignof(std::pmr::memory_resource) > 3);
    uintptr_t kind() const noexcept { return owner_kind_ & 3; }
    void set_kind(Kind k) noexcept { owner_kind_ = (owner_kind_ & ~uintptr_t{3}) | k; }
    allocator_type allocator() const noexcept {
        return allocator_type(reinterpret_cast<std::pmr::memory_resource*>(owner_kind_ & ~uintptr_t{3}));
    }
    char* make_external(size_t length) {
        if (length > SIZE_MAX - sizeof(size_t)) throw std::bad_alloc();
        char* result = allocator().allocate(sizeof(size_t) + length);
        std::memcpy(result, &length, sizeof(length));
        return result;
    }
    void release() noexcept {
        if (kind() == external_value) allocator().deallocate(storage_.external, external_bytes());
    }
    void take(FixedWindowRow& other) noexcept {
        storage_ = other.storage_;
        set_kind(static_cast<Kind>(other.kind()));
        other.set_kind(empty_value);
    }
    uintptr_t owner_kind_;
    union Storage {
        char local[Width];
        char* external;
        Storage() : local{} {}
    } storage_;
};
static_assert(sizeof(uintptr_t) != 8 || (sizeof(FixedWindowRow<8>) == 16 && sizeof(FixedWindowRow<16>) == 24));
template<class T>
inline constexpr bool is_fixed_window_row = std::is_same_v<T, FixedWindowRow<8>>
                                         || std::is_same_v<T, FixedWindowRow<16>>;

} // namespace forl0
