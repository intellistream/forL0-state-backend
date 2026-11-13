// Exact serialized-key cache. Keys and values live in the SDK L0 allocation.
// The fixed directory holds only hashes, offsets and links in DRAM. All
// metadata is allocated at attachment; state accesses never allocate nodes.
#pragma once

#include "hot_cache.h"
#include <functional>
#include <string_view>
#include <vector>

namespace forl0 {
class SerializedHotCache {
public:
    using Hasher = uint64_t (*)(std::string_view);
    explicit SerializedHotCache(HotCacheLL* backing, Hasher hasher = &hash)
        : hasher_(hasher), backing_(backing), data_(reinterpret_cast<char*>(backing->sets())),
          capacity_(size_t(backing->num_sets()) * sizeof(HotSet)),
          entries_(capacity_ / line_size), buckets_(bucket_count(entries_.size()), none),
          prefixes_(buckets_.size(), none) {
        backing_->set_serialized_payload();
    }
    HotCacheLL* backing() const { return backing_; }
    const char* data() const { return data_; }
    size_t capacity() const { return capacity_; }

    bool get(std::string_view key, std::string* out) {
        std::string_view view;
        if (!get_view(key, &view)) return false;
        out->assign(view.data(), view.size());
        return true;
    }
    bool get_view(std::string_view key, std::string_view* out) {
        uint32_t slot = find(key, hasher_(key));
        backing_->record_serialized_lookup(slot != none);
        if (slot == none) return false;
        const Entry& e = entries_[slot];
        *out = std::string_view(data_ + size_t(slot) * line_size + e.key_size, e.value_size);
        return true;
    }
    void put(std::string_view key, std::string_view value, size_t prefix_size = 0) {
        if (prefix_size > key.size()) throw std::invalid_argument("cache prefix exceeds key length");
        backing_->record_serialized_write();
        const uint64_t key_hash = hasher_(key);
        uint32_t previous = find(key, key_hash);
        if (previous != none) {
            Entry& e = entries_[previous];
            if (e.prefix_size == prefix_size && value.size() <= size_t(e.lines) * line_size - e.key_size) {
                std::memmove(data_ + size_t(previous) * line_size + e.key_size, value.data(), value.size());
                e.value_size = static_cast<uint32_t>(value.size());
                return;
            }
            remove_index(previous);
        }
        if (key.size() > capacity_ || value.size() > capacity_ - key.size()) {
            backing_->record_serialized_bypass();
            return;
        }
        const uint32_t lines = static_cast<uint32_t>(std::max(size_t(1), (key.size() + value.size() + line_size - 1) / line_size));
        if (lines > entries_.size()) { backing_->record_serialized_bypass(); return; }
        if (size_t(tail_) + lines > entries_.size()) {
            // Account for the unused end of the ring as a FIFO padding record.
            // This keeps the occupied region contiguous, including tombstones.
            if (!used_) head_ = tail_ = 0;
            else {
                const uint32_t padding = static_cast<uint32_t>(entries_.size() - tail_);
                make_room(padding);
                if (!used_) head_ = tail_;
                entries_[tail_] = Entry{};
                entries_[tail_].lines = padding;
                used_ += padding;
                tail_ = 0;
            }
        }
        make_room(lines);
        if (!used_) head_ = tail_;
        const uint32_t slot = tail_;
        Entry& e = entries_[slot];
        e = Entry{};
        e.lines = lines;
        e.hash = key_hash;
        e.key_size = static_cast<uint32_t>(key.size());
        e.value_size = static_cast<uint32_t>(value.size());
        e.prefix_size = static_cast<uint32_t>(prefix_size);
        e.valid = true;
        const size_t offset = size_t(slot) * line_size;
        std::memcpy(data_ + offset, key.data(), key.size());
        std::memcpy(data_ + offset + key.size(), value.data(), value.size());
        const size_t bucket = key_hash & (buckets_.size() - 1);
        e.next = buckets_[bucket];
        if (e.next != none) entries_[e.next].previous = slot;
        buckets_[bucket] = slot;
        if (prefix_size) {
            e.prefix_hash = hasher_(key.substr(0, prefix_size));
            const size_t bucket = e.prefix_hash & (prefixes_.size() - 1);
            e.prefix_next = prefixes_[bucket];
            if (e.prefix_next != none) entries_[e.prefix_next].prefix_previous = slot;
            prefixes_[bucket] = slot;
        }
        used_ += lines;
        tail_ = static_cast<uint32_t>((size_t(tail_) + lines) % entries_.size());
    }
    void invalidate(std::string_view key) {
        uint32_t slot = find(key, hasher_(key));
        if (slot != none) remove_index(slot);
    }
    void invalidate_prefix(std::string_view prefix) {
        const uint64_t h = hasher_(prefix);
        uint32_t slot = prefixes_[h & (prefixes_.size() - 1)];
        while (slot != none) {
            const Entry& e = entries_[slot];
            uint32_t next = e.prefix_next;
            if (e.prefix_hash == h && e.prefix_size == prefix.size()
                && std::memcmp(data_ + size_t(slot) * line_size, prefix.data(), prefix.size()) == 0) remove_index(slot);
            slot = next;
        }
    }
    void clear() {
        std::fill(entries_.begin(), entries_.end(), Entry{});
        std::fill(buckets_.begin(), buckets_.end(), none);
        std::fill(prefixes_.begin(), prefixes_.end(), none);
        head_ = tail_ = used_ = 0;
    }

private:
    static constexpr uint32_t none = UINT32_MAX;
    static constexpr size_t line_size = 64;
    struct Entry {
        uint64_t hash = 0, prefix_hash = 0;
        uint32_t key_size = 0, value_size = 0, prefix_size = 0, lines = 0;
        uint32_t next = none, previous = none, prefix_next = none, prefix_previous = none;
        bool valid = false;
    };
    Hasher hasher_;
    HotCacheLL* backing_;
    char* data_;
    size_t capacity_;
    std::vector<Entry> entries_;
    std::vector<uint32_t> buckets_, prefixes_;
    uint32_t head_ = 0, tail_ = 0, used_ = 0;
    static size_t bucket_count(size_t entries) {
        size_t count = 2;
        while (count < entries * 2) count *= 2;
        return count;
    }
    static uint64_t hash(std::string_view key) {
        return std::hash<std::string_view>{}(key);
    }
    uint32_t find(std::string_view key, uint64_t h) const {
        uint32_t slot = buckets_[h & (buckets_.size() - 1)];
        while (slot != none) {
            const Entry& e = entries_[slot];
            if (e.hash == h && e.key_size == key.size()
                && std::memcmp(data_ + size_t(slot) * line_size, key.data(), key.size()) == 0) return slot;
            slot = e.next;
        }
        return none;
    }
    void remove_index(uint32_t slot) {
        Entry& e = entries_[slot];
        if (e.previous == none) buckets_[e.hash & (buckets_.size() - 1)] = e.next;
        else entries_[e.previous].next = e.next;
        if (e.next != none) entries_[e.next].previous = e.previous;
        if (e.prefix_size) {
            if (e.prefix_previous == none) prefixes_[e.prefix_hash & (prefixes_.size() - 1)] = e.prefix_next;
            else entries_[e.prefix_previous].prefix_next = e.prefix_next;
            if (e.prefix_next != none) entries_[e.prefix_next].prefix_previous = e.prefix_previous;
        }
        e.valid = false;
        backing_->record_serialized_invalidation();
    }
    void make_room(uint32_t lines) {
        while (size_t(used_) + lines > entries_.size()) {
            Entry& e = entries_[head_];
            if (e.valid) remove_index(head_);
            const uint32_t span = e.lines;
            head_ = static_cast<uint32_t>((size_t(head_) + span) % entries_.size());
            used_ -= span;
            e.lines = 0;
        }
    }
};
} // namespace forl0
