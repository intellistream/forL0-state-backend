#pragma once

#include "simd.h"
#include "compact_map_key.h"
#include "namespace_cache.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>
#include <memory_resource>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace forl0 {

// Small maps compare complete keys after filtering. Sampling a few words makes
// the cost of filtering independent of the length of a serialized Flink row.
inline int8_t packed_byte_key_tag(std::string_view payload, size_t encoded_size) noexcept {
    uint64_t first = 0, middle = 0, last = 0;
    if (payload.size() >= sizeof(uint64_t)) {
        const size_t tail = payload.size() - sizeof(uint64_t);
        std::memcpy(&first, payload.data() + std::min(size_t{8}, tail), sizeof(first));
        std::memcpy(&middle, payload.data() + tail / 2, sizeof(middle));
        std::memcpy(&last, payload.data() + tail, sizeof(last));
    } else if (!payload.empty()) {
        std::memcpy(&first, payload.data(), payload.size());
    }
    uint64_t bits = first ^ ((middle << 17) | (middle >> 47)) ^ ((last << 37) | (last >> 27));
    bits ^= encoded_size * UINT64_C(0x9e3779b97f4a7c15);
    bits ^= bits >> 33;
    return static_cast<int8_t>((bits * UINT64_C(0xff51afd7ed558ccd)) >> 57);
}

// An empty or single-entry map lives in its outer slot. Small multi-entry
// maps keep their key/value objects in one allocation before switching to hashing.
template<class K, class V, class Hash = std::hash<K>>
class StateMap {
    using Entry = std::pair<K, V>;
    static constexpr size_t linear_limit = 4;
    static constexpr size_t resident_packed_limit = 4 * kGroupWidth;
    static_assert(kGroupWidth == 16 && linear_limit == 4 && alignof(Entry) <= kGroupWidth,
            "Resident packed groups share their layout with NamespaceReadIndex");
    // A byte-string hash is expensive to recompute when every node is
    // copied across memory tiers. libstdc++ retains it when the callable
    // is not declared noexcept, so copying a map can reuse node hashes.
    struct RetainedHash {
        size_t operator()(const K& key) const { return Hash{}(key); }
    };
    static constexpr bool string_key = std::is_same_v<K, std::pmr::string> || is_serialized_map_key<K>;
    using MapHash = std::conditional_t<string_key, RetainedHash, Hash>;
    using HashMap = std::pmr::unordered_map<K, V, MapHash>;

public:
    using value_type = std::pair<const K, V>;
    using allocator_type = typename HashMap::allocator_type;

    template<bool IsConst>
    class Iterator {
        using Pointer = std::conditional_t<IsConst, const Entry*, Entry*>;
        using HashIterator = std::conditional_t<IsConst,
            typename HashMap::const_iterator, typename HashMap::iterator>;
        using ValueRef = std::conditional_t<IsConst, const V&, V&>;
        friend class StateMap;

        explicit Iterator(Pointer entry) : position_(entry) {}
        explicit Iterator(HashIterator entry) : position_(entry) {}

    public:
        struct Reference {
            const K& first;
            ValueRef second;
        };
        struct Arrow {
            Reference ref;
            const Reference* operator->() const { return &ref; }
        };

        Iterator() = default;
        Reference operator*() const {
            if (const auto* single = std::get_if<Pointer>(&position_)) {
                return {(**single).first, (**single).second};
            }
            auto& entry = *std::get<HashIterator>(position_);
            return {entry.first, entry.second};
        }
        Arrow operator->() const { return {**this}; }
        Iterator& operator++() {
            if (auto* position = std::get_if<Pointer>(&position_)) ++*position;
            else ++std::get<HashIterator>(position_);
            return *this;
        }
        Iterator operator++(int) {
            Iterator previous(*this);
            ++*this;
            return previous;
        }
        bool operator==(const Iterator& other) const { return position_ == other.position_; }
        bool operator!=(const Iterator& other) const { return !(*this == other); }

    private:
        std::variant<Pointer, HashIterator> position_;
    };

    using iterator = Iterator<false>;
    using const_iterator = Iterator<true>;

    StateMap() : StateMap(allocator_type()) {}
    explicit StateMap(allocator_type allocator) : allocator_(allocator) {}
    StateMap(std::initializer_list<value_type> entries) : StateMap() {
        reserve(entries.size());
        for (const auto& [key, value] : entries) try_emplace(key, value);
    }
    StateMap(const StateMap& other) : StateMap(other, allocator_type()) {}
    StateMap(const StateMap& other, allocator_type allocator) : StateMap(allocator) {
        const size_t count = other.size();
        if (other.hashed() && !resident_pack(count)) {
            storage_.hash = allocate_hash(*other.storage_.hash);
            extent_.count = -1;
            return;
        }
        // A source map already has unique keys. Keep its contiguous layout when
        // a whole namespace moves, including a return to ordinary memory.
        if (count > 1) {
            const bool returning_to_table = count > kGroupWidth && !other.hashed()
                    && dynamic_cast<NamespaceDramAllocator*>(allocator_.resource()) != nullptr;
            if (returning_to_table) reserve_packed(count);
            else reserve(count);
        }
        if (hashed()) {
            for (const auto& [key, value] : other) storage_.hash->try_emplace(key, value);
            return;
        }
        Entry* rows = data();
        int8_t* tags = count > linear_limit ? tag_data(rows, extent_.capacity) : nullptr;
        if (tags && !other.hashed()) {
            std::memcpy(tags, tag_data(other.data(), other.extent_.capacity), count);
            storage_.packed.tags_present = other.storage_.packed.tags_present;
            tags = nullptr;
        }
        for (const auto& [key, value] : other) {
            if (tags) {
                const int8_t tag = key_tag(key);
                tags[extent_.count] = tag;
                storage_.packed.tags_present |= tag_bit(tag);
            }
            new (rows + extent_.count) Entry(owned<K>(key), owned<V>(value));
            ++extent_.count;
        }
    }
    StateMap(StateMap&& other) noexcept(std::is_nothrow_move_constructible_v<Entry>)
        : StateMap(other.allocator_) { take(other); }
    StateMap(StateMap&& other, allocator_type allocator) : StateMap(allocator) {
        if (allocator_ == other.allocator_) take(other);
        else *this = static_cast<const StateMap&>(other);
    }
    StateMap& operator=(const StateMap& other) {
        if (this != &other) {
            StateMap replacement(other, allocator_);
            release();
            take(replacement);
        }
        return *this;
    }
    StateMap& operator=(StateMap&& other) {
        if (this != &other) {
            if (allocator_ != other.allocator_) return *this = static_cast<const StateMap&>(other);
            release();
            take(other);
        }
        return *this;
    }
    ~StateMap() { release(); }

    allocator_type get_allocator() const { return allocator_; }
    void relocate_storage(allocator_type allocator) {
        static_assert(std::is_nothrow_move_constructible_v<Entry>);
        StateMap replacement(*this, allocator);
        // Publish only after the allocator-aware copy completes. The outer
        // table retains the same mapped-value address and slot.
        this->~StateMap();
        new (this) StateMap(std::move(replacement));
    }

    size_t size() const { return hashed() ? storage_.hash->size() : size_t(extent_.count); }
    bool empty() const { return size() == 0; }
    size_t copy_storage_bytes() const {
        // The caller estimates the next complete copy into its namespace arena.
        if (size() <= resident_packed_limit) return size() > 1 ? block_bytes(size()) : 0;
        if (hashed()) {
            const size_t node = sizeof(value_type) + sizeof(void*) + (string_key ? sizeof(size_t) : 0);
            return sizeof(HashMap) + size() * node + storage_.hash->bucket_count() * sizeof(void*);
        }
        return block_bytes(size());
    }

    // The task thread can inspect the same entries the native table owns.
    // The signed count selects the representation. Capacity one means inline.
    const void* extent_address() const noexcept { return &extent_; }
    const void* storage_address() const noexcept { return &storage_; }

    iterator begin() { return begin_at<false>(*this); }
    const_iterator begin() const { return begin_at<true>(*this); }
    const_iterator cbegin() const { return begin(); }
    iterator end() { return end_at<false>(*this); }
    const_iterator end() const { return end_at<true>(*this); }
    const_iterator cend() const { return end(); }
    iterator find(const K& key) { return find_at<false>(*this, key); }
    const_iterator find(const K& key) const { return find_at<true>(*this, key); }

    const V* find_value(const K& key) const {
        if (hashed()) {
            const auto it = storage_.hash->find(key);
            return it == storage_.hash->end() ? nullptr : &it->second;
        }
        return find_compact_value([&] { return key_tag(key); }, [&](const auto& stored) { return stored == key; });
    }
    template<class Bytes, std::enable_if_t<is_serialized_map_key<K>
            && std::is_same_v<Bytes, std::pmr::string>, int> = 0>
    const V* find_value(const Bytes& key) const { return find_value(K::probe(key)); }

    template<class Bytes, std::enable_if_t<is_serialized_map_key<K>
            && std::is_same_v<Bytes, std::pmr::string>, int> = 0>
    size_t erase(const Bytes& key) { return erase(K::probe(key)); }

    size_t count(const K& key) const { return find_value(key) != nullptr; }
    bool supports_compact_lookup() const noexcept { return !hashed(); }

    template<class Tag, class Equal>
    const V* find_compact_value(Tag tag, Equal equal) const {
        const size_t i = match_index(tag, equal);
        return i < size_t(extent_.count) ? &data()[i].second : nullptr;
    }
    size_t erase(const K& key) {
        if (hashed()) return storage_.hash->erase(key);
        const size_t count = size();
        const size_t i = key_index(key);
        if (i == count) return 0;
        if (count == 1) {
            release();
            return 1;
        }
        auto* rows = data();
        std::move(rows + i + 1, rows + count, rows + i);
        std::destroy_at(rows + count - 1);
        if (count > linear_limit) {
            auto* tags = tag_data(rows, extent_.capacity);
            std::move(tags + i + 1, tags + count, tags + i);
            tags[count - 1] = kEmpty;
        }
        --extent_.count;
        return 1;
    }
    void reserve(size_t entries) {
        if (hashed()) { storage_.hash->reserve(entries); return; }
        if (entries <= extent_.capacity) return;
        if (entries > kGroupWidth && !resident_pack(entries)) {
            auto* map = allocate_hash();
            try {
                map->reserve(entries);
                for (const auto& [key, value] : *this) map->try_emplace(key, value);
            } catch (...) {
                free_hash(map);
                throw;
            }
            release();
            storage_.hash = map;
            extent_.count = -1;
            return;
        }
        reserve_packed(entries);
    }

    V& operator[](const K& key) { return try_emplace(key).first->second; }
    V& operator[](K&& key) { return try_emplace(std::move(key)).first->second; }
    V& at(const K& key) { return lookup(*this, key); }
    const V& at(const K& key) const { return lookup(*this, key); }

    template<class Key, class... Args>
    std::pair<iterator, bool> try_emplace(Key&& key, Args&&... args) {
        if constexpr (is_serialized_map_key<K> && std::is_same_v<std::decay_t<Key>, std::pmr::string>) {
            return try_emplace(K::probe(key), std::forward<Args>(args)...);
        }
        if (hashed()) {
            auto entry = storage_.hash->try_emplace(std::forward<Key>(key), std::forward<Args>(args)...);
            return {iterator(entry.first), entry.second};
        }
        const size_t count = size();
        int8_t tag = 0;
        const size_t existing = match_index([&] { return tag = key_tag(key); },
                [&](const auto& stored) { return stored == key; });
        if (existing < count) return {iterator(data() + existing), false};
        // Materialize input before reallocation. A caller may have borrowed the
        // key or the value from another entry in this same map.
        Entry entry(owned<K>(std::forward<Key>(key)), owned<V>(std::forward<Args>(args)...));
        if (count == extent_.capacity) {
            if (count >= kGroupWidth && !resident_pack(count + 1)) {
                reserve(count + 1);
                auto inserted = storage_.hash->try_emplace(std::move(entry.first), std::move(entry.second));
                return {iterator(inserted.first), inserted.second};
            }
            const size_t limit = count < kGroupWidth ? kGroupWidth : resident_packed_limit;
            reserve(std::min(limit, 2 * count));
        }
        if (count >= linear_limit) {
            auto* tags = tag_data(storage_.packed.rows, extent_.capacity);
            if (count == linear_limit) {
                tag = key_tag(entry.first);
                storage_.packed.tags_present = 0;
                for (size_t i = 0; i < count; ++i) {
                    tags[i] = key_tag(storage_.packed.rows[i].first);
                    storage_.packed.tags_present |= tag_bit(tags[i]);
                }
            }
            tags[count] = tag;
            storage_.packed.tags_present |= tag_bit(tag);
        }
        auto* target = extent_.capacity == 1 ? &storage_.one : storage_.packed.rows + count;
        new (target) Entry(std::move(entry));
        ++extent_.count;
        return {iterator(target), true};
    }
    template<class Key, class... Args>
    std::pair<iterator, bool> emplace(Key&& key, Args&&... value) {
        return try_emplace(std::forward<Key>(key), std::forward<Args>(value)...);
    }

private:
    struct Extent { int32_t count = 0; uint32_t capacity = 1; };
    static_assert(offsetof(Extent, capacity) == sizeof(int32_t));
    union Storage {
        Entry one;
        struct Packed { Entry* rows; uint64_t tags_present; } packed;
        static_assert(offsetof(Packed, tags_present) == sizeof(uint64_t),
                "The task reads the tag mask immediately after the packed pointer");
        HashMap* hash;
        Storage() : packed{} {}
        ~Storage() {}
    };
    bool hashed() const noexcept { return extent_.count < 0; }
    Entry* data() { return extent_.capacity == 1 ? &storage_.one : storage_.packed.rows; }
    const Entry* data() const { return extent_.capacity == 1 ? &storage_.one : storage_.packed.rows; }
    void reserve_packed(size_t entries) {
        auto* resource = allocator_.resource();
        auto* memory = static_cast<int8_t*>(resource->allocate(block_bytes(entries), alignof(Entry)));
        const size_t prefix = tag_bytes(entries);
        std::fill_n(memory, prefix, kEmpty);
        auto* target = reinterpret_cast<Entry*>(memory + prefix);
        const size_t count = size();
        if (count > linear_limit) std::memcpy(memory, tag_data(data(), extent_.capacity), count);
        const auto previous = extent_;
        const uint64_t present = count > linear_limit ? storage_.packed.tags_present : 0;
        size_t copied = 0;
        try {
            for (; copied < count; ++copied) {
                auto& row = data()[copied];
                if constexpr (std::is_nothrow_move_constructible_v<Entry>) new (target + copied) Entry(std::move(row));
                else new (target + copied) Entry(owned<K>(row.first), owned<V>(row.second));
            }
        } catch (...) {
            std::destroy_n(target, copied);
            resource->deallocate(memory, block_bytes(entries), alignof(Entry));
            throw;
        }
        release();
        storage_.packed = {target, present};
        extent_ = {previous.count, static_cast<uint32_t>(entries)};
    }
    bool resident_pack(size_t entries) const noexcept {
        return entries <= resident_packed_limit && dynamic_cast<L0PayloadAllocator*>(allocator_.resource()) != nullptr;
    }
    static size_t tag_bytes(size_t capacity) { return (capacity + kGroupWidth - 1) & ~(kGroupWidth - 1); }
    static size_t block_bytes(size_t capacity) { return tag_bytes(capacity) + capacity * sizeof(Entry); }
    static const int8_t* tag_data(const Entry* rows, size_t capacity) {
        return reinterpret_cast<const int8_t*>(rows) - tag_bytes(capacity);
    }
    static int8_t* tag_data(Entry* rows, size_t capacity) {
        return const_cast<int8_t*>(tag_data(static_cast<const Entry*>(rows), capacity));
    }
    template<class... Args>
    HashMap* allocate_hash(Args&&... args) {
        auto* res = allocator_.resource();
        void* raw = res->allocate(sizeof(HashMap), alignof(HashMap));
        try { return new (raw) HashMap(std::forward<Args>(args)..., allocator_); }
        catch (...) { res->deallocate(raw, sizeof(HashMap), alignof(HashMap)); throw; }
    }
    void free_hash(HashMap* map) {
        std::destroy_at(map);
        allocator_.resource()->deallocate(map, sizeof(HashMap), alignof(HashMap));
    }
    void release() noexcept {
        if (hashed()) free_hash(storage_.hash);
        else if (extent_.capacity > 1) {
            auto* rows = storage_.packed.rows;
            std::destroy_n(rows, size_t(extent_.count));
            allocator_.resource()->deallocate(tag_data(rows, extent_.capacity), block_bytes(extent_.capacity), alignof(Entry));
        } else if (extent_.count == 1) std::destroy_at(&storage_.one);
        storage_.packed = {};
        extent_ = {};
    }
    void take(StateMap& other) {
        if (other.hashed()) storage_.hash = other.storage_.hash;
        else if (other.extent_.capacity > 1) storage_.packed = other.storage_.packed;
        else if (other.extent_.count == 1) {
            new (&storage_.one) Entry(std::move(other.storage_.one));
            std::destroy_at(&other.storage_.one);
        }
        extent_ = other.extent_;
        other.storage_.packed = {};
        other.extent_ = {};
    }
    template<class T, class... Args>
    T owned(Args&&... values) const {
        if constexpr (std::uses_allocator_v<T, allocator_type>) return T(std::forward<Args>(values)..., allocator_);
        else return T(std::forward<Args>(values)...);
    }
    static uint64_t tag_bit(int8_t tag) { return uint64_t{1} << (uint8_t(tag) & 63); }

    static int8_t key_tag(const K& key) {
        if constexpr (string_key && std::is_same_v<Hash, std::hash<K>>) {
            const std::string_view bytes(key.data(), key.size());
            // BinaryRowData keys start with a four-byte size. Other serializers
            // can share this filter because every candidate is checked in full.
            return packed_byte_key_tag(bytes.substr(std::min(size_t{4}, bytes.size())), bytes.size());
        }
        if constexpr (std::is_same_v<K, int64_t> && std::is_same_v<Hash, std::hash<K>>) {
            const uint64_t bits = static_cast<uint64_t>(key);
            return static_cast<int8_t>((bits ^ (bits >> 16)) & 0x7f);
        }
        const size_t hash = Hash{}(key);
        return static_cast<int8_t>((hash ^ (hash >> 16)) & 0x7f);
    }


    template<class Tag, class Equal>
    size_t match_index(Tag&& tag, Equal&& equal) const {
        const size_t count = size_t(extent_.count);
        const Entry* rows = data();
        if (count <= linear_limit) {
            for (size_t i = 0; i < count; ++i) if (equal(rows[i].first)) return i;
        } else {
            const auto fingerprint = tag();
            // The pointer and this mask share the outer slot. A clear bit rejects
            // the key before accessing any inner allocation. Erasures may leave bits set.
            if (!(storage_.packed.tags_present & tag_bit(fingerprint))) return count;
            const auto* tags = tag_data(rows, extent_.capacity);
            for (size_t first = 0; first < count; first += kGroupWidth) {
                auto matches = Group(tags + first).match(fingerprint);
                while (matches.has_next()) {
                    const size_t i = first + matches.next();
                    if (i < count && equal(rows[i].first)) return i;
                }
            }
        }
        return count;
    }
    size_t key_index(const K& key) const {
        return match_index([&] { return key_tag(key); }, [&](const auto& stored) { return stored == key; });
    }
    template<bool C, class Self>
    static Iterator<C> begin_at(Self& self) {
        if (self.hashed()) return Iterator<C>(self.storage_.hash->begin());
        return Iterator<C>(self.data());
    }
    template<bool C, class Self>
    static Iterator<C> end_at(Self& self) {
        if (self.hashed()) return Iterator<C>(self.storage_.hash->end());
        auto* first = self.data();
        return Iterator<C>(self.extent_.count == 0 ? first : first + self.extent_.count);
    }
    template<bool C, class Self>
    static Iterator<C> find_at(Self& self, const K& key) {
        if (self.hashed()) return Iterator<C>(self.storage_.hash->find(key));
        size_t i = self.key_index(key);
        return i ? Iterator<C>(self.data() + i) : Iterator<C>(self.data());
    }
    template<class Self>
    static decltype(auto) lookup(Self& self, const K& key) {
        auto it = self.find(key);
        if (it == self.end()) throw std::out_of_range("StateMap key");
        return (it->second);
    }
    allocator_type allocator_;
    Extent extent_;
    Storage storage_;
};

template<class T> inline constexpr bool is_state_map = false;
template<class K, class V, class Hash> inline constexpr bool is_state_map<StateMap<K, V, Hash>> = true;

}  // namespace forl0
