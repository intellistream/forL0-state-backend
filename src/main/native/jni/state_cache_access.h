#pragma once
#include "jni_utils.h"
#include <array>
#include <algorithm>
#include <optional>
#include <type_traits>

namespace forl0 {
// Byte-identical to StateCacheKey(group, int64 namespace, int64 key), without
// constructing the generic key's string storage on this frequent read path.
class LongNamespaceCacheKey {
public:
    LongNamespaceCacheKey(int32_t group, int64_t ns, int64_t key) {
        std::memcpy(bytes_.data(), &group, 4);
        bytes_[4] = 1;
        std::memcpy(bytes_.data() + 5, &ns, 8);
        std::memcpy(bytes_.data() + 13, &key, 8);
    }
    std::string_view view() const { return {bytes_.data(), bytes_.size()}; }
private:
    std::array<char, 21> bytes_;
};

// Common numeric/FixedRow identities fit on the stack. Generic paths reuse the
// bytes already received by JNI, without another Java serializer invocation.
class StateCacheKey {
public:
    StateCacheKey(const StateCacheKey& other) { copy_from(other); }
    StateCacheKey& operator=(const StateCacheKey& other) {
        if (this != &other) copy_from(other);
        return *this;
    }
    template<class K> StateCacheKey(int group, const K& key) {
        append(group); append(uint8_t(0)); append(key);
    }
    template<class N, class K> StateCacheKey(int group, const N& ns, const K& key) {
        append(group); append(uint8_t(1)); append(ns); append(key);
    }
    template<class T> void append(const T& value) { bytes(&value, sizeof(value)); }
    // JNI byte-array identities have 32-bit lengths.
    void append(const std::string& value) { append(uint32_t(value.size())); bytes(value.data(), value.size()); }
    void append(std::string_view value) { append(uint32_t(value.size())); bytes(value.data(), value.size()); }
    void append(const FixedRow& row) { append(row.arity); bytes(row.f, size_t(row.arity) * sizeof(int64_t)); }
    void append(const TimeWindow& ns) { append(ns.first); append(ns.second); }
    std::string_view view() const { return {large_.empty() ? small_.data() : large_.data(), size_}; }
private:
    std::array<char, 256> small_;
    std::string large_;
    size_t size_ = 0;
    void copy_from(const StateCacheKey& other) {
        size_ = other.size_;
        if (other.large_.empty()) {
            large_.clear();
            std::memcpy(small_.data(), other.small_.data(), size_);
        } else large_ = other.large_;
    }
    void bytes(const void* data, size_t n) {
        if (large_.empty() && size_ + n <= small_.size()) std::memcpy(small_.data() + size_, data, n);
        else {
            if (large_.empty()) large_.assign(small_.data(), size_);
            large_.append(static_cast<const char*>(data), n);
        }
        size_ += n;
    }
};

template<class V> std::string_view cache_value_bytes(const V& value) {
    if constexpr (std::is_same_v<V, std::string> || std::is_same_v<V, std::string_view>) return value;
    else return {reinterpret_cast<const char*>(&value), sizeof(value)};
}

template<class K, class V, class... Args>
auto cached_get(StateHandle* handle, StateTable<K, V>* table, Args&&... args) {
    using R = std::conditional_t<std::is_same_v<V, std::string>, std::string_view, V>;
    if (handle->serialized_cache) {
        StateCacheKey key(args...);
        std::string_view bytes;
        if (handle->serialized_cache->get_view(key.view(), &bytes)) {
            if constexpr (std::is_same_v<V, std::string>) return std::optional<R>(bytes);
            else { V value; std::memcpy(&value, bytes.data(), sizeof(value)); return std::optional<R>(value); }
        }
    }
    V* value = table->get(std::forward<Args>(args)...);
    if (!value) return std::optional<R>();
    // Blob views can be retained until the next state mutation. Do not admit
    // read misses: eviction during another read would invalidate such a view.
    if constexpr (std::is_same_v<V, std::string>) return std::optional<R>(std::string_view(*value));
    else return std::optional<R>(*value);
}

template<class K, class V, class W>
V* cached_put(StateHandle* handle, StateTable<K, V>* table, int group, const K& key, W&& value) {
    if (!handle->serialized_cache) return table->put(group, key, std::forward<W>(value));
    StateCacheKey identity(group, key);
    try {
        V* stored = table->put(group, key, std::forward<W>(value));
        handle->serialized_cache->put(identity.view(), cache_value_bytes(*stored));
        return stored;
    } catch (...) { handle->serialized_cache->invalidate(identity.view()); throw; }
}
template<class K, class V, class N, class W>
V* cached_put(StateHandle* handle, StateTable<K, V>* table, int group, const N& ns, const K& key, W&& value) {
    if (!handle->serialized_cache) return table->put(group, ns, key, std::forward<W>(value));
    StateCacheKey identity(group, ns, key);
    try {
        V* stored = table->put(group, ns, key, std::forward<W>(value));
        handle->serialized_cache->put(identity.view(), cache_value_bytes(*stored));
        return stored;
    } catch (...) { handle->serialized_cache->invalidate(identity.view()); throw; }
}
template<class K, class V, class... Args>
bool cached_remove(StateHandle* handle, StateTable<K, V>* table, Args&&... args) {
    if (handle->serialized_cache) { StateCacheKey key(args...); handle->serialized_cache->invalidate(key.view()); }
    return table->remove(std::forward<Args>(args)...);
}
template<class K, class V, class F>
bool cached_modify(StateHandle* handle, StateTable<K, V>* table, int group, const K& key, F&& fn) {
    if (handle->serialized_cache) { StateCacheKey id(group, key); handle->serialized_cache->invalidate(id.view()); }
    return table->modify_in_place(group, key, std::forward<F>(fn));
}
template<class K, class V, class N, class F>
bool cached_modify(StateHandle* handle, StateTable<K, V>* table, int group, const N& ns, const K& key, F&& fn) {
    if (handle->serialized_cache) { StateCacheKey id(group, ns, key); handle->serialized_cache->invalidate(id.view()); }
    return table->modify_in_place(group, ns, key, std::forward<F>(fn));
}

// Reuse the canonical blob allocation for window accumulator updates. JNI
// copies directly into it; no temporary std::string allocation/copy is needed.
template<class K, class F, class... Args>
void put_owned_blob(StateHandle* h, StateTable<K, std::string>* table,
                    size_t length, F&& copy, Args&&... args) {
    std::string* stored = nullptr;
    auto update = [&](std::string& value) {
        value.resize(static_cast<size_t>(length));
        copy(value.data());
        stored = &value;
    };
    try {
        if (!table->modify_in_place(args..., update)) {
            std::string value;
            update(value);
            stored = table->put(args..., std::move(value));
        }
        if (h->serialized_cache) {
            StateCacheKey key(args...);
            h->serialized_cache->put(key.view(), *stored);
        }
    } catch (...) {
        if (h->serialized_cache) { StateCacheKey key(args...); h->serialized_cache->invalidate(key.view()); }
        throw;
    }
}

template<class K, class... Args>
void put_java_blob(JNIEnv* env, StateHandle* h, StateTable<K, std::string>* table,
                   jbyteArray bytes, Args&&... args) {
    const jsize length = env->GetArrayLength(bytes);
    put_owned_blob(h, table, length, [&](char* target) {
        env->GetByteArrayRegion(bytes, 0, length, reinterpret_cast<jbyte*>(target));
    }, std::forward<Args>(args)...);
}

class MapCallCache {
public:
    template<class... Args> MapCallCache(StateHandle* h, Args&&... args) : cache_(h->serialized_cache.get()) {
        if (cache_) parent_.emplace(std::forward<Args>(args)...);
    }
    MapCallCache(const MapCallCache&) = delete;
    MapCallCache& operator=(const MapCallCache&) = delete;
    ~MapCallCache() {
        // A failed canonical mutation must not leave the old entry readable.
        if (pending_) cache_->invalidate(child_->view());
    }
    void invalidate() {
        if (cache_) cache_->invalidate_prefix(parent_->view());
    }
    template<class U> void prepare(const U& user) {
        if (!cache_) return;
        if (pending_) cache_->invalidate(child_->view());
        child_ = *parent_; child_->append(uint8_t(1)); child_->append(user);
        pending_ = true;
        auto bulk = *parent_; bulk.append(uint8_t(2));
        cache_->invalidate(bulk.view());
    }
    template<class U> void remove(const U& user) { prepare(user); }
    template<class V> void put_prepared(const V& value) {
        if (cache_) {
            // Updating a present entry can overwrite its existing cache span.
            // Defer invalidation to failure/removal instead of evicting it first.
            cache_->put(child_->view(), cache_value_bytes(value), parent_->view().size());
            pending_ = false;
        }
    }
    template<class U> bool get(const U& user, std::string_view* value) {
        if (!cache_) return false;
        auto key = *parent_; key.append(uint8_t(1)); key.append(user);
        return cache_->get_view(key.view(), value);
    }
    template<class U> bool get_number(const U& user, int64_t* value) {
        std::string_view bytes;
        if (!get(user, &bytes)) return false;
        std::memcpy(value, bytes.data(), sizeof(*value));
        return true;
    }
    template<class U, class V> void put(const U& user, const V& value) {
        if (!cache_) return;
        auto key = *parent_; key.append(uint8_t(1)); key.append(user);
        cache_->put(key.view(), cache_value_bytes(value), parent_->view().size());
    }
    bool get_bulk(std::string_view* value) {
        if (!cache_) return false;
        auto key = *parent_; key.append(uint8_t(2));
        return cache_->get_view(key.view(), value);
    }
    void put_bulk(const uint8_t* data, size_t size) {
        if (!cache_) return;
        auto key = *parent_; key.append(uint8_t(2));
        cache_->put(key.view(), {reinterpret_cast<const char*>(data), size}, parent_->view().size());
    }
    bool has_entries() const { return cache_ && cache_->has_prefix(parent_->view()); }
    size_t list_entry_bytes(size_t value_size) const {
        return cache_ ? SerializedHotCache::entry_bytes(parent_->view().size() + 1 + sizeof(uint64_t), value_size) : 0;
    }
    bool admit_list_read(size_t footprint) const { return cache_ && footprint <= cache_->capacity() / 2; }
    void record_misses(size_t count) { if (cache_) cache_->record_misses(count); }
    void record_hits(size_t count) { if (cache_) cache_->record_hits(count); }
    using ListEntry = std::pair<size_t, std::string_view>;
    std::vector<ListEntry> list_entries(size_t count) const {
        std::vector<ListEntry> entries;
        if (!cache_) return entries;
        const size_t prefix_size = parent_->view().size();
        cache_->for_each_prefix(parent_->view(), [&](std::string_view key, std::string_view value) {
            if (key.size() != prefix_size + 1 + sizeof(uint64_t) || key[prefix_size] != 1) return;
            uint64_t index;
            std::memcpy(&index, key.data() + prefix_size + 1, sizeof(index));
            if (index < count) entries.emplace_back(size_t(index), value);
        });
        std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
        return entries;
    }
private:
    SerializedHotCache* cache_;
    std::optional<StateCacheKey> parent_;
    std::optional<StateCacheKey> child_;
    bool pending_ = false;
};
} // namespace forl0
