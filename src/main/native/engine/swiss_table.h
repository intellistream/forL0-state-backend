// SwissTable implementation aligned with abseil-cpp's raw_hash_set.
//
// Memory layout (single contiguous allocation):
//   [ctrl bytes (capacity) | sentinel (1) | cloned ctrl (kGroupWidth-1) | padding | slots (capacity)]
//
// Key design decisions matching abseil:
//   - Triangular probing: probe(i) offset = i*(i+1)/2
//   - H1 = hash >> 7 for probe start, H2 = hash & 0x7F in ctrl byte
//   - EMPTY=0x80, DELETED=0xFE, SENTINEL=0xFF, FULL=H2 (0x00..0x7F)
//   - 7/8 load factor (87.5%)
//   - Objects stored inline in slots via placement new (flat_hash_map style)
//   - Capacity is always a power of 2

#pragma once

#include "allocator.h"
#include "simd.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <new>
#include <type_traits>
#include <utility>

namespace forl0 {

// ============================================================================
//  Layout helpers
// ============================================================================

// Total ctrl bytes = capacity + 1 (sentinel) + kGroupWidth - 1 (cloned)
//                  = capacity + kGroupWidth
inline size_t ctrl_bytes(size_t capacity) {
    return capacity + kGroupWidth;
}

// Alignment for slot array within the block.
template <typename SlotType>
inline size_t slot_align() {
    return alignof(SlotType) > 16 ? alignof(SlotType) : 16;
}

// Offset from start of allocation to the first slot.
// ctrl array comes first: ctrl_bytes(capacity) bytes.
// Then padding to align slots.
template <typename SlotType>
inline size_t slot_offset(size_t capacity) {
    size_t cb = ctrl_bytes(capacity);
    size_t align = slot_align<SlotType>();
    return (cb + align - 1) & ~(align - 1);
}

// Total allocation size for ctrl + slots.
template <typename SlotType>
inline size_t alloc_size(size_t capacity) {
    return slot_offset<SlotType>(capacity) + capacity * sizeof(SlotType);
}

// ============================================================================
//  SwissTable
// ============================================================================

template <typename K, typename V,
          typename Hash = std::hash<K>,
          typename KeyEqual = std::equal_to<K>>
class SwissTable {
public:
    using key_type = K;
    using mapped_type = V;
    using slot_type = std::pair<K, V>;

    // --- Construction / destruction ---

    explicit SwissTable(size_t initial_capacity = 16,
                        Allocator* alloc = &DefaultAllocator::instance())
        : alloc_(alloc), size_(0), capacity_(0), growth_left_(0),
          ctrl_(nullptr), slots_(nullptr), alloc_ptr_(nullptr) {
        // Ensure power of 2 and >= kGroupWidth
        size_t cap = kGroupWidth;
        while (cap < initial_capacity) cap <<= 1;
        allocate_and_init(cap);
    }

    ~SwissTable() {
        destroy_all_slots();
        free_backing();
    }

    // Move construction
    SwissTable(SwissTable&& other) noexcept
        : alloc_(other.alloc_), size_(other.size_), capacity_(other.capacity_),
          growth_left_(other.growth_left_), ctrl_(other.ctrl_),
          slots_(other.slots_), alloc_ptr_(other.alloc_ptr_) {
        other.size_ = 0;
        other.capacity_ = 0;
        other.growth_left_ = 0;
        other.ctrl_ = nullptr;
        other.slots_ = nullptr;
        other.alloc_ptr_ = nullptr;
    }

    SwissTable& operator=(SwissTable&& other) noexcept {
        if (this != &other) {
            destroy_all_slots();
            free_backing();
            alloc_ = other.alloc_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            growth_left_ = other.growth_left_;
            ctrl_ = other.ctrl_;
            slots_ = other.slots_;
            alloc_ptr_ = other.alloc_ptr_;
            other.size_ = 0;
            other.capacity_ = 0;
            other.ctrl_ = nullptr;
            other.slots_ = nullptr;
            other.alloc_ptr_ = nullptr;
        }
        return *this;
    }

    SwissTable(const SwissTable&) = delete;
    SwissTable& operator=(const SwissTable&) = delete;

    // --- Accessors ---

    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
    bool empty() const { return size_ == 0; }

    // --- find ---
    // Returns pointer to the value if found, nullptr otherwise.
    V* find(const K& key) {
        size_t hash = hash_(key);
        int8_t h2 = H2(hash);
        ProbeSeq seq(H1(hash), capacity_ - 1);

        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask matches = g.match(h2);
            while (matches.has_next()) {
                uint32_t i = matches.next();
                size_t idx = seq.pos() + i;
                // Wrap around (idx may be >= capacity due to cloned ctrl reads,
                // but slot access must be < capacity).
                if (idx >= capacity_) idx -= capacity_;
                slot_type& slot = slots_[idx];
                if (eq_(slot.first, key)) {
                    return &slot.second;
                }
            }
            if (g.match_empty()) {
                return nullptr;  // Probe sequence ends at empty slot.
            }
            seq.next();
        }
    }

    const V* find(const K& key) const {
        return const_cast<SwissTable*>(this)->find(key);
    }

    // --- insert_or_assign ---
    // Inserts or overwrites. Returns (pointer to value, true if new insertion).
    std::pair<V*, bool> insert_or_assign(const K& key, const V& value) {
        return insert_or_assign_impl(key, value);
    }

    std::pair<V*, bool> insert_or_assign(const K& key, V&& value) {
        return insert_or_assign_impl(key, std::move(value));
    }

    // --- emplace ---
    // Insert if not present. Returns (pointer to value, true if inserted).
    template <typename... Args>
    std::pair<V*, bool> emplace(const K& key, Args&&... args) {
        size_t hash = hash_(key);
        int8_t h2 = H2(hash);
        ProbeSeq seq(H1(hash), capacity_ - 1);

        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask matches = g.match(h2);
            while (matches.has_next()) {
                uint32_t i = matches.next();
                size_t idx = wrap(seq.pos() + i);
                if (eq_(slots_[idx].first, key)) {
                    return {&slots_[idx].second, false};
                }
            }
            if (g.match_empty()) {
                break;
            }
            seq.next();
        }

        // Not found — insert
        if (growth_left_ == 0) {
            rehash_and_grow_if_necessary();
            // Retry after rehash
            return emplace(key, std::forward<Args>(args)...);
        }

        size_t target = find_first_non_full(hash);
        if (ctrl_[target] == kEmpty) {
            --growth_left_;
        }
        ++size_;
        set_ctrl(ctrl_, capacity_, target, h2);
        new (&slots_[target]) slot_type(
            std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(std::forward<Args>(args)...));
        return {&slots_[target].second, true};
    }

    // --- erase ---
    // Returns true if key was found and erased.
    bool erase(const K& key) {
        size_t hash = hash_(key);
        int8_t h2 = H2(hash);
        ProbeSeq seq(H1(hash), capacity_ - 1);

        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask matches = g.match(h2);
            while (matches.has_next()) {
                uint32_t i = matches.next();
                size_t idx = wrap(seq.pos() + i);
                if (eq_(slots_[idx].first, key)) {
                    erase_slot(idx);
                    return true;
                }
            }
            if (g.match_empty()) {
                return false;
            }
            seq.next();
        }
    }

    // --- clear ---
    void clear() {
        destroy_all_slots();
        init_ctrl(ctrl_, capacity_);
        size_ = 0;
        growth_left_ = growth_budget(capacity_);
    }

    // --- iteration ---
    // Calls fn(const K& key, V& value) for each entry.
    template <typename Fn>
    void for_each(Fn&& fn) {
        for (size_t i = 0; i < capacity_; ++i) {
            if (is_full(ctrl_[i])) {
                fn(slots_[i].first, slots_[i].second);
            }
        }
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for (size_t i = 0; i < capacity_; ++i) {
            if (is_full(ctrl_[i])) {
                fn(slots_[i].first, slots_[i].second);
            }
        }
    }

    // --- Direct slot access for checkpoint ---
    int8_t* ctrl_data() { return ctrl_; }
    const int8_t* ctrl_data() const { return ctrl_; }
    slot_type* slot_data() { return slots_; }
    const slot_type* slot_data() const { return slots_; }

private:
    // --- helpers ---

    static bool is_full(int8_t c) { return c >= 0; }  // H2 values are 0x00..0x7F

    static size_t growth_budget(size_t capacity) {
        // 7/8 load factor = capacity - capacity/8
        return capacity - capacity / 8;
    }

    size_t wrap(size_t idx) const {
        return idx >= capacity_ ? idx - capacity_ : idx;
    }

    // Find the first empty or deleted slot suitable for insertion.
    size_t find_first_non_full(size_t hash) const {
        ProbeSeq seq(H1(hash), capacity_ - 1);
        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask mask = g.match_empty_or_deleted();
            if (mask) {
                size_t idx = wrap(seq.pos() + mask.lowest_bit_set());
                return idx;
            }
            seq.next();
        }
    }

    void erase_slot(size_t idx) {
        slots_[idx].~slot_type();
        --size_;

        // Determine whether to set DELETED or EMPTY.
        // Following abseil: if the slot's group has no empty slots after it,
        // we must set DELETED to not break probe chains. Otherwise set EMPTY.
        // Simplified: if the next slot in the probe chain is EMPTY, we can set EMPTY.
        // Full abseil logic: check if the group containing idx+1 has any empty slots.
        // Here we use abseil's was_never_full heuristic:
        size_t next_idx = (idx + 1) & (capacity_ - 1);
        if (ctrl_[next_idx] == kEmpty) {
            set_ctrl(ctrl_, capacity_, idx, kEmpty);
            ++growth_left_;
        } else {
            set_ctrl(ctrl_, capacity_, idx, kDeleted);
        }
    }

    template <typename VV>
    std::pair<V*, bool> insert_or_assign_impl(const K& key, VV&& value) {
        size_t hash = hash_(key);
        int8_t h2 = H2(hash);
        ProbeSeq seq(H1(hash), capacity_ - 1);

        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask matches = g.match(h2);
            while (matches.has_next()) {
                uint32_t i = matches.next();
                size_t idx = wrap(seq.pos() + i);
                if (eq_(slots_[idx].first, key)) {
                    slots_[idx].second = std::forward<VV>(value);
                    return {&slots_[idx].second, false};
                }
            }
            if (g.match_empty()) {
                break;
            }
            seq.next();
        }

        // Not found — insert
        if (growth_left_ == 0) {
            rehash_and_grow_if_necessary();
            return insert_or_assign_impl(key, std::forward<VV>(value));
        }

        size_t target = find_first_non_full(hash);
        if (ctrl_[target] == kEmpty) {
            --growth_left_;
        }
        ++size_;
        set_ctrl(ctrl_, capacity_, target, h2);
        new (&slots_[target]) slot_type(key, std::forward<VV>(value));
        return {&slots_[target].second, true};
    }

    void rehash_and_grow_if_necessary() {
        // If too many tombstones, rehash at same capacity; otherwise grow 2x.
        // abseil: grow if size+1 > growth_budget, rehash if tombstones > size.
        size_t tombstones = growth_budget(capacity_) - growth_left_ - size_;
        if (tombstones > capacity_ / 4) {
            // Too many tombstones — rehash at same capacity
            rehash(capacity_);
        } else {
            // Grow 2x
            rehash(capacity_ * 2);
        }
    }

    void rehash(size_t new_capacity) {
        int8_t* old_ctrl = ctrl_;
        slot_type* old_slots = slots_;
        void* old_alloc = alloc_ptr_;
        size_t old_capacity = capacity_;

        allocate_and_init(new_capacity);
        size_ = 0;

        // Move all FULL entries from old to new
        for (size_t i = 0; i < old_capacity; ++i) {
            if (is_full(old_ctrl[i])) {
                // Re-insert with move
                size_t hash = hash_(old_slots[i].first);
                int8_t h2 = H2(hash);
                size_t target = find_first_non_full(hash);
                if (ctrl_[target] == kEmpty) {
                    --growth_left_;
                }
                ++size_;
                set_ctrl(ctrl_, capacity_, target, h2);
                new (&slots_[target]) slot_type(std::move(old_slots[i]));
                old_slots[i].~slot_type();
            }
        }

        // Free old allocation
        if (old_alloc) {
            alloc_->deallocate(old_alloc, alloc_size<slot_type>(old_capacity));
        }
    }

    void allocate_and_init(size_t cap) {
        capacity_ = cap;
        growth_left_ = growth_budget(cap);

        size_t total = alloc_size<slot_type>(cap);
        size_t alignment = slot_align<slot_type>();
        if (alignment < 64) alignment = 64;  // Cache-line aligned

        alloc_ptr_ = alloc_->allocate(total, alignment);
        std::memset(alloc_ptr_, 0, total);

        ctrl_ = reinterpret_cast<int8_t*>(alloc_ptr_);
        init_ctrl(ctrl_, cap);

        slots_ = reinterpret_cast<slot_type*>(
            static_cast<char*>(alloc_ptr_) + slot_offset<slot_type>(cap));
    }

    void destroy_all_slots() {
        if (!ctrl_ || !slots_) return;
        if constexpr (!std::is_trivially_destructible_v<slot_type>) {
            for (size_t i = 0; i < capacity_; ++i) {
                if (is_full(ctrl_[i])) {
                    slots_[i].~slot_type();
                }
            }
        }
    }

    void free_backing() {
        if (alloc_ptr_) {
            alloc_->deallocate(alloc_ptr_, alloc_size<slot_type>(capacity_));
            alloc_ptr_ = nullptr;
            ctrl_ = nullptr;
            slots_ = nullptr;
        }
    }

    // --- members ---
    Allocator* alloc_;
    size_t size_;
    size_t capacity_;
    size_t growth_left_;
    int8_t* ctrl_;
    slot_type* slots_;
    void* alloc_ptr_;   // raw allocation pointer (for deallocation)
    Hash hash_;
    KeyEqual eq_;
};

}  // namespace forl0
