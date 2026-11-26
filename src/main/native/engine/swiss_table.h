// SwissTable implementation aligned with abseil-cpp's raw_hash_set.
//
// Memory layout:
//   Unified mode (small tables):
//     [ctrl bytes (capacity+kGroupWidth) | padding | slots (capacity)]  — single allocation
//   Split mode uses separate allocations when the state allocator accounts
//   for controls and slots independently.
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
#include <memory>
#include <vector>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace forl0 {

// ============================================================================
//  Layout helpers
// ============================================================================

// Total ctrl bytes = capacity + 1 (sentinel) + kGroupWidth - 1 (cloned)
//                  = capacity + kGroupWidth
inline size_t ctrl_bytes(size_t capacity) {
    return capacity == 1 ? size_t(kGroupWidth) : capacity + kGroupWidth;
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

// One task-thread descriptor per VoidNamespace table. Java indexes into the
// engine's existing bounded pool segment. Zero disables the optional filter.
struct RootMembershipView {
    // Bitmap word indexes use at most 40 bits. Old readers mask an unsigned
    // 40-bit index and therefore ignore these two publication flags.
    static constexpr uint64_t supports_repeated_absence = uint64_t{1} << 63;
    static constexpr uint64_t valid_absence = uint64_t{1} << 62;
    uint64_t bitmap_offset_plus_one = 0;
    uint64_t word_mask = 0;
    uint64_t last_absent_hash = 0;
    uint64_t skipped_reads = 0;
    uint64_t count() const { return __atomic_load_n(&skipped_reads, __ATOMIC_ACQUIRE); }
};
static_assert(sizeof(RootMembershipView) == 4 * sizeof(uint64_t));

// Views contain no entries. The Flink task thread reads the authoritative
// control bytes in the pool and withdraws the view before backing changes.
struct alignas(64) ResidentControls {
    static constexpr uint64_t root_kind = 1;
    static constexpr size_t per_group(bool root) { return root ? 1 : 4; }
    static uint64_t window_hash(int64_t first, int64_t second) {
        const uint64_t end = static_cast<uint64_t>(second);
        const uint64_t key = static_cast<uint64_t>(first) ^ ((end << 21) | (end >> 43));
        return (key ^ (key >> 32)) * UINT64_C(0x9e3779b97f4a7c15);
    }
    static size_t window_bucket(int64_t first, int64_t second) {
        return window_hash(first, second) >> 62;
    }
    uint64_t address = 0, mask = 0, kind = 0;
    // A DRAM root has no namespace bounds and uses second for its last confirmed slot.
    int64_t first = 0, second = 0;
    uint64_t skipped = 0;
    uint64_t slots = 0, slot_layout = 0, found = 0;
    // A positive position names an exact row. A negative position names an
    // insertion slot only when a complete first group had no matching H2.
    int64_t edit_position = 0;
    uint64_t absent_hash = 0, paired_updates = 0, paired_inserts = 0;
    uint64_t updated = 0, missing_reads = 0;
    // Large tables publish a fingerprint and a one-based slot. For tables below
    // SIMD width the low word is zero and the high word holds the exact live slots.
    static constexpr uint64_t compact_slots_tag = uint64_t{1} << 40;
    uint64_t last_root_row = 0;
    static constexpr uint64_t missing_bit = 8;
    uint64_t namespace_misses() const { return __atomic_load_n(&missing_reads, __ATOMIC_ACQUIRE); }
    static constexpr uint64_t writable_bit = uint64_t{1} << 23;
    uint64_t writes() const { return __atomic_load_n(&updated, __ATOMIC_ACQUIRE); }
    uint64_t hits() const { return __atomic_load_n(&found, __ATOMIC_ACQUIRE); }
    uint64_t count() const { return __atomic_load_n(&skipped, __ATOMIC_ACQUIRE); }
};
static_assert(sizeof(ResidentControls) == 128 && offsetof(ResidentControls, updated) == 104
        && offsetof(ResidentControls, missing_reads) == 112 && offsetof(ResidentControls, last_root_row) == 120);

// Attached to a complete namespace's control-byte copy. The table's slots,
// payloads and original controls always stay together in their owning tier.
struct ProbeIndexUsage {
    size_t bytes = 0, installs = 0, releases = 0;
};

// H1 and H2 need entropy across the entire hash. std::hash on integers is
// commonly identity: shifting it for H1 clusters nearby keys, while high-bit
// keys can all land in the same bucket with the same fingerprint.
template <typename K>
struct SwissKeyHash {
    size_t operator()(const K& key) const {
        if constexpr (std::is_integral_v<K>) {
            uint64_t mixed = static_cast<uint64_t>(key);
            mixed ^= mixed >> 30;
            mixed *= UINT64_C(0xbf58476d1ce4e5b9);
            mixed ^= mixed >> 27;
            mixed *= UINT64_C(0x94d049bb133111eb);
            return static_cast<size_t>(mixed ^ (mixed >> 31));
        }
        return std::hash<K>{}(key);
    }
};

template <typename K, typename V,
          typename Hash = SwissKeyHash<K>,
          typename KeyEqual = std::equal_to<K>>
class SwissTable {
public:
    using key_type = K;
    using mapped_type = V;
    using slot_type = std::pair<K, V>;

    // --- Construction / destruction ---

    explicit SwissTable(size_t initial_capacity = 16,
                        Allocator* alloc = &DefaultAllocator::instance(),
                        size_t max_capacity = 0,
                        double max_load_factor = 0.875,
                        bool compact = false)
        : alloc_(alloc), size_(0), capacity_(0), growth_left_(0),
          ctrl_(nullptr), slots_(nullptr), split_{nullptr, nullptr, false},
          max_capacity_(max_capacity), max_load_factor_(max_load_factor) {
        // Compact tables below SIMD width use the first complete group.
        const size_t cap = rounded_capacity(initial_capacity, compact);
        if (max_capacity_ > 0 && cap > max_capacity_) {
            throw std::invalid_argument("initial SwissTable capacity exceeds configured maximum");
        }
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
          slots_(other.slots_), split_(other.split_),
          max_capacity_(other.max_capacity_), max_load_factor_(other.max_load_factor_),
          membership_(std::move(other.membership_)), compact_live_(other.compact_live_) {
        other.withdraw_controls();
        index_owner_ = std::exchange(other.index_owner_, nullptr);
        index_usage_ = std::exchange(other.index_usage_, nullptr);
        other.size_ = 0;
        other.compact_live_ = 0;
        other.capacity_ = 0;
        other.growth_left_ = 0;
        other.ctrl_ = nullptr;
        other.slots_ = nullptr;
        other.split_ = {nullptr, nullptr, false};
    }

    SwissTable& operator=(SwissTable&& other) noexcept {
        if (this != &other) {
            other.withdraw_controls();
            destroy_all_slots();
            free_backing();
            membership_ = std::move(other.membership_);
            compact_live_ = other.compact_live_;
            alloc_ = other.alloc_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            growth_left_ = other.growth_left_;
            ctrl_ = other.ctrl_;
            index_owner_ = std::exchange(other.index_owner_, nullptr);
            index_usage_ = std::exchange(other.index_usage_, nullptr);
            slots_ = other.slots_;
            split_ = other.split_;
            max_capacity_ = other.max_capacity_;
            max_load_factor_ = other.max_load_factor_;
            other.size_ = 0;
            other.compact_live_ = 0;
            other.capacity_ = 0;
            other.ctrl_ = nullptr;
            other.slots_ = nullptr;
            other.split_ = {nullptr, nullptr, false};
        }
        return *this;
    }

    SwissTable(const SwissTable&) = delete;
    SwissTable& operator=(const SwissTable&) = delete;

    // --- Accessors ---

    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
    bool empty() const { return size_ == 0; }
    size_t packed_backing_bytes() const { return packed_backing_bytes_for(capacity_); }
    static size_t initial_backing_bytes(size_t requested, bool compact) {
        return packed_backing_bytes_for(rounded_capacity(requested, compact));
    }
    static size_t initial_backing_alignment(size_t requested, bool compact) {
        return backing_slot_alignment(rounded_capacity(requested, compact));
    }
    size_t packed_backing_alignment() const { return backing_slot_alignment(capacity_); }

    // Preserve the table address and probe layout. Publish the new storage
    // only after all live objects, including their allocator-owned bytes,
    // have been copied successfully.
    void relocate(Allocator* target) {
        if (target == alloc_) return;
        SwissTable replacement(capacity_, target, max_capacity_, max_load_factor_, capacity_ < kGroupWidth);
        size_t constructed_end = 0;
        try {
            for_each_live_slot(0, capacity_, [&](size_t i) {
                replacement.construct_slot(i, slots_[i].first, slots_[i].second);
                constructed_end = i + 1;
            });
        } catch (...) {
            // The destination controls are still empty. Occupied source slots
            // before the failing row identify exactly which objects to destroy.
            if constexpr (!std::is_trivially_destructible_v<slot_type>) {
                for (size_t i = 0; i < constructed_end; ++i)
                    if (is_full(ctrl_[i])) replacement.slots_[i].~slot_type();
            }
            throw;
        }
        std::memcpy(replacement.ctrl_, ctrl_, ctrl_bytes(capacity_));
        replacement.size_ = size_;
        replacement.growth_left_ = growth_left_;
        replacement.compact_live_ = compact_live_;
        *this = std::move(replacement);
    }

    bool has_probe_index() const noexcept { return index_owner_ != nullptr; }

    // Move the active controls into the pool. The original backing remains
    // reserved so reclaiming the index can return the controls without allocating.
    void attach_probe_index(void* storage, Allocator& owner, ProbeIndexUsage& usage) noexcept {
        assert(storage && !index_owner_ && storage != ctrl_);
        withdraw_controls();
        const size_t bytes = ctrl_bytes(capacity_);
        std::memcpy(storage, ctrl_, bytes);
        ctrl_ = static_cast<int8_t*>(storage);
        index_owner_ = &owner;
        index_usage_ = &usage;
        usage.bytes += bytes;
        ++usage.installs;
    }

    void drop_probe_index() noexcept {
        if (!index_owner_) return;
        withdraw_controls();
        const size_t bytes = ctrl_bytes(capacity_);
        std::memcpy(split_.ctrl_ptr, ctrl_, bytes);
        release_probe_index(ctrl_, bytes);
        ctrl_ = static_cast<int8_t*>(split_.ctrl_ptr);
    }

    // A namespace can use spare cache space for a one-sided membership
    // summary. It contains bits only. The complete table stays at its tier.
    bool has_membership() const { return bool(membership_); }

    size_t membership_bytes() const {
        return membership_ ? membership_->words.capacity() * sizeof(uint64_t) : 0;
    }

    size_t membership_write_skips() const { return membership_ ? membership_->write_skips : 0; }
    size_t shared_absence_inserts() const { return membership_ ? membership_->shared_absence_inserts : 0; }
    size_t prepared_writes() const { return membership_ ? membership_->prepared_writes : 0; }
    void share_membership(RootMembershipView& view, const void* pool_base) {
        if (!membership_) return;
        const auto base = reinterpret_cast<uintptr_t>(pool_base);
        auto& m = *membership_;
        view.word_mask = (m.words.size() - 1) | RootMembershipView::supports_repeated_absence;
        m.view = &view;
        m.offset_plus_one = reinterpret_cast<uintptr_t>(m.words.data()) - base + 1;
        m.consumed_reads = view.count();
        view.bitmap_offset_plus_one = m.offset_plus_one;
    }

    void enable_shared_membership(bool enabled) noexcept {
        if (membership_ && membership_->view) {
            membership_->view->bitmap_offset_plus_one = enabled ? membership_->offset_plus_one : 0;
        }
    }

    void drop_membership() noexcept { membership_.reset(); }

    bool install_membership(Allocator& resource, size_t budget) {
        if (membership_ || budget < 64) return false;
        size_t words = 1;
        while (words <= budget / sizeof(uint64_t) / 2) words *= 2;
        auto fresh = std::make_unique<Membership>(&resource, words);
        for (size_t i = 0; i < capacity_; ++i) {
            if (is_full(ctrl_[i])) fresh->add(hash_(slots_[i].first));
        }
        membership_ = std::move(fresh);
        return true;
    }

    V* find_with_membership(const K& key, bool& rejected) {
        assert(membership_);
        const size_t hash = hash_(key);
        rejected = !membership_->may_contain(hash);
        if (!rejected) return find_hashed(key, hash, eq_);
        if (membership_->prepare_writes && capacity_ >= 256 && growth_left_) {
            const size_t position = find_first_non_full(hash);
            membership_->pending_slot = position;
            __builtin_prefetch(slots_ + position, 1, 1);
        }
        return nullptr;
    }

    // --- find ---
    // Returns pointer to the value if found, nullptr otherwise.
    V* find(const K& key) { return find_impl(key, hash_, eq_); }

    const V* find(const K& key) const {
        return const_cast<SwissTable*>(this)->find(key);
    }

    // Lookup serialized keys without allocating an owning string. The
    // standard string and string_view hash specializations are equivalent.
    V* find_string_view(std::string_view key) {
        static_assert(std::is_same_v<K, std::string>
                && std::is_same_v<Hash, SwissKeyHash<std::string>>
                && std::is_same_v<KeyEqual, std::equal_to<std::string>>);
        return find_impl(key, std::hash<std::string_view>{}, std::equal_to<std::string_view>{});
    }

    // The pointer comes from a successful foreground lookup in this table.
    // The value subobject is inside its slot, so integer division recovers
    // the index without assuming an ABI-specific offsetof(std::pair, second).
    size_t position_of(const V& value) const {
        return (reinterpret_cast<uintptr_t>(&value) - reinterpret_cast<uintptr_t>(slots_)) / sizeof(slot_type);
    }

    V* value_at_if_equal(size_t index, const K& key) {
        if (index >= capacity_ || !is_full(ctrl_[index]) || !eq_(slots_[index].first, key)) return nullptr;
        return &slots_[index].second;
    }

    void remember_root_position(ResidentControls& view, uint64_t position) noexcept {
        if (observer_ != &view) {
            withdraw_controls();
            assert(!view.address);
            observer_ = &view;
            view.kind = ResidentControls::root_kind;
        }
        view.last_root_row = position;
        std::memcpy(&view.second, &position, sizeof(position));
    }

    V* value_at_root_position(const ResidentControls& view, uint64_t position, const K& key) {
        const size_t index = static_cast<uint32_t>(position) - 1;
        if (observer_ != &view || static_cast<uint64_t>(view.second) != position) {
            return value_at_if_equal(index, key);
        }
        // Only a confirmed native lookup records a live slot here. Erase,
        // clear and backing moves revoke it before the slot can be reused.
        assert(index < capacity_);
        return eq_(slots_[index].first, key) ? &slots_[index].second : nullptr;
    }

    V* take_read_slot(const K& key) {
        auto* view = current_controls();
        if (!view || view->edit_position <= 0) return nullptr;
        const auto index = static_cast<size_t>(std::exchange(view->edit_position, 0) - 1);
        auto* value = value_at_if_equal(index, key);
        if (value) ++view->paired_updates;
        return value;
    }

    template<class Input>
    static void assign_existing(V& target, Input&& value) {
        if constexpr (std::is_same_v<V, NamespaceBlob>) target.assign(value.data(), value.size());
        else target = std::forward<Input>(value);
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
        size_t hash, position = capacity_;
        if (auto* existing = lookup_for_write(key, hash, position)) return {existing, false};

        // Not found — insert
        if (growth_left_ == 0) {
            rehash_and_grow_if_necessary();
            // Retry after rehash
            return emplace(key, std::forward<Args>(args)...);
        }

        size_t target = position != capacity_ ? position : slot_for_write(hash);
        new (&slots_[target]) slot_type(std::piecewise_construct, slot_args<K>(key),
                                      slot_args<V>(std::forward<Args>(args)...));
        publish_slot(target, hash);
        return {&slots_[target].second, true};
    }

    // --- erase ---
    // Returns true if key was found and erased.
    bool erase(const K& key) {
        if (capacity_ < kGroupWidth && size_ <= 2) {
            if (auto* live = find_compact(key, eq_)) {
                erase_slot(position_of(*live));
                return true;
            }
            return false;
        }
        size_t hash = hash_(key);
        int8_t h2 = H2(hash);
        ProbeSeq seq(capacity_ < kGroupWidth ? 0 : H1(hash), capacity_ - 1);

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

    // The caller has just verified the full key in this table and has not
    // changed its structure since obtaining the value.
    void erase_at_value(const V& value) { erase_slot(position_of(value)); }

    // --- clear ---
    void clear() {
        if (membership_) {
            std::fill(membership_->words.begin(), membership_->words.end(), 0);
            membership_->pending = false;
        }
        if (size_) destroy_all_slots();
        init_ctrl(ctrl_, capacity_);
        compact_live_ = 0;
        invalidate_read_slot();
        size_ = 0;
        growth_left_ = growth_budget(capacity_);
    }

    // --- iteration ---
    template <typename Fn>
    void for_each(Fn&& fn) {
        for_each_live_slot(0, capacity_, [&](size_t i) { fn(slots_[i].first, slots_[i].second); });
    }

    template <typename Fn>
    void for_each(Fn&& fn) const {
        for_each_live_slot(0, capacity_, [&](size_t i) { fn(slots_[i].first, slots_[i].second); });
    }

    // Wrapped COW marks are conservative. Only marked occupied slots need
    // a frozen-key lookup and every unmarked occupied slot is current.
    template<class Current, class Changed>
    size_t for_each_marked(const uint64_t* words, size_t word_mask, Current& current, Changed& changed) const {
        size_t checked = 0;
        for (size_t base = 0; base < capacity_; base += 64) {
            const uint64_t bits = words[(base / 64) & word_mask];
            const size_t end = std::min(base + 64, capacity_);
            if (!bits) {
                for_each_live_slot(base, end, [&](size_t i) { current(slots_[i].first, slots_[i].second); });
            } else {
                for_each_live_slot(base, end, [&](size_t i) {
                    if (bits & (uint64_t{1} << (i - base))) {
                        ++checked;
                        changed(slots_[i].first, slots_[i].second);
                    } else {
                        current(slots_[i].first, slots_[i].second);
                    }
                });
            }
        }
        return checked;
    }

    // --- Direct slot access for checkpoint ---
    int8_t* ctrl_data() { return ctrl_; }
    const int8_t* ctrl_data() const { return ctrl_; }
    void publish_controls(ResidentControls& view, uint64_t kind, int64_t first, int64_t second) {
        const auto address = reinterpret_cast<uintptr_t>(ctrl_);
        if (observer_ && observer_ != &view) withdraw_controls();
        if (observer_ == &view && view.address == address && view.kind == kind
                && (kind == ResidentControls::root_kind || (view.first == first && view.second == second))) return;
        observer_ = &view;
        if (capacity_ < kGroupWidth) view.last_root_row = compact_slots_word();
        view.edit_position = 0;
        view.slots = 0;
        view.slot_layout = 0;
        view.mask = capacity_ - 1;
        view.first = first;
        view.second = second;
        view.kind = kind;
        view.address = address;
    }

    void withdraw_controls() noexcept {
        if (observer_ && observer_->kind == ResidentControls::root_kind) observer_->second = 0;
        if (auto* view = current_controls()) {
            view->edit_position = 0;
            // The task tests slots before trying the ordinary root's negative index.
            // A retired backing must no longer advertise its values or write permission.
            view->slots = 0;
            view->slot_layout = 0;
            view->address = 0;
        }
        observer_ = nullptr;
    }
    slot_type* slot_data() { return slots_; }
    const slot_type* slot_data() const { return slots_; }

private:
    template <typename T, typename... Args>
    auto slot_args(Args&&... args) {
        if constexpr (std::uses_allocator_v<T, std::pmr::polymorphic_allocator<char>>) {
            return std::forward_as_tuple(std::forward<Args>(args)..., alloc_);
        } else {
            return std::forward_as_tuple(std::forward<Args>(args)...);
        }
    }

    template <typename VV>
    void construct_slot(size_t position, const K& key, VV&& value) {
        if constexpr (std::is_same_v<V, NamespaceBlob>) {
            const std::string_view bytes(value);
            new (&slots_[position]) slot_type(std::piecewise_construct, slot_args<K>(key), slot_args<V>(bytes));
        } else if constexpr (std::uses_allocator_v<K, std::pmr::polymorphic_allocator<char>>
                          || std::uses_allocator_v<V, std::pmr::polymorphic_allocator<char>>) {
            new (&slots_[position]) slot_type(std::piecewise_construct, slot_args<K>(key),
                                            slot_args<V>(std::forward<VV>(value)));
        } else {
            new (&slots_[position]) slot_type(key, std::forward<VV>(value));
        }
    }

    struct Membership {
        explicit Membership(Allocator* allocator, size_t count) : words(count, uint64_t{}, allocator) {}
        ~Membership() {
            if (view) view->bitmap_offset_plus_one = 0;
        }
        std::pmr::vector<uint64_t> words;
        RootMembershipView* view = nullptr;
        uint64_t offset_plus_one = 0;
        uint64_t consumed_reads = 0;
        struct Bits { size_t word; uint64_t mask; };
        Bits pending_bits{};
        static constexpr size_t no_slot = static_cast<size_t>(-1);
        size_t pending_slot = no_slot;
        size_t prepared_writes = 0;
        uint8_t negative_reads = 0;
        uint8_t paired_inserts = 0;
        bool prepare_writes = false;
        size_t pending_hash = 0;
        size_t write_skips = 0;
        size_t shared_absence_inserts = 0;
        bool pending = false;
        Bits bits(size_t hash) const {
            // A single aligned word load tests all three bits. Mix before
            // partitioning to support even poorly distributed user hashes.
            uint64_t h = SwissKeyHash<uint64_t>{}(hash);
            return {size_t(h >> 24) & (words.size() - 1),
                    (UINT64_C(1) << (h & 63)) | (UINT64_C(1) << ((h >> 6) & 63))
                    | (UINT64_C(1) << ((h >> 12) & 63))};
        }
        bool may_contain(size_t hash) {
            const auto b = bits(hash);
            const bool possible = (words[b.word] & b.mask) == b.mask;
            if (!possible) {
                if (++negative_reads == 64) {
                    prepare_writes = paired_inserts >= 48;
                    negative_reads = paired_inserts = 0;
                }
                pending_slot = no_slot;
                pending_hash = hash;
                pending_bits = b;
                pending = true;
            }
            return possible;
        }
        bool known_absent_for_write(size_t hash) {
            if (view && consumed_reads != view->count()) {
                consumed_reads = view->count();
                if (hash == view->last_absent_hash) {
                    ++shared_absence_inserts;
                    pending_hash = hash;
                    pending_bits = bits(hash);
                    pending_slot = no_slot;
                    pending = true;
                }
            }
            if (!pending) return false;
            if (pending_hash != hash) {
                pending = false;
                return false;
            }
            // A missing bit rules out every key with this complete hash.
            // Publishing any insertion below consumes the proof.
            ++write_skips;
            return true;
        }
        void add(size_t hash) {
            const bool paired = pending && pending_hash == hash;
            if (paired && paired_inserts < 64) ++paired_inserts;
            const auto b = paired ? pending_bits : bits(hash);
            words[b.word] |= b.mask;
            if (view) {
                consumed_reads = view->count();
                // Inserting a colliding full hash also revokes the negative result.
                if (view->last_absent_hash == hash) view->word_mask &= ~RootMembershipView::valid_absence;
            }
            pending = false;
        }
    };

    template<class Query, class QueryEqual>
    V* find_compact(const Query& key, const QueryEqual& equal) {
        for (unsigned bits = compact_live_; bits; bits &= bits - 1) {
            auto& row = slots_[__builtin_ctz(bits)];
            if (equal(row.first, key)) return &row.second;
        }
        return nullptr;
    }

    template<class Fn>
    void for_each_live_slot(size_t begin, size_t end, Fn&& visit) const {
        if (capacity_ < kGroupWidth) {
            if (capacity_ == 1) {
                if (size_) visit(0);
                return;
            }
            for (unsigned live = compact_live_; live; live &= live - 1) visit(__builtin_ctz(live));
            return;
        }
        // Callers pass whole groups. A DRAM table with a probe index
        // reads the same exact controls in the pool that its owner updates.
        for (size_t group = begin; group < end; group += kGroupWidth) {
            auto live = Group(ctrl_ + group).match_full();
            while (live) visit(group + live.next());
        }
    }

    ResidentControls* current_controls() const noexcept {
        return observer_ && observer_->address == reinterpret_cast<uintptr_t>(ctrl_) ? observer_ : nullptr;
    }

    uint64_t compact_slots_word() const noexcept {
        return ResidentControls::compact_slots_tag | (uint64_t{compact_live_} << 32);
    }

    void invalidate_read_slot() noexcept {
        if (observer_ && observer_->kind == ResidentControls::root_kind) observer_->second = 0;
        if (auto* view = current_controls()) {
            view->edit_position = 0;
            if (capacity_ < kGroupWidth) view->last_root_row = compact_slots_word();
        }
    }

    V* lookup_for_write(const K& key, size_t& hash, size_t& position) {
        if (capacity_ < kGroupWidth && size_ <= 2 && !membership_) {
            if (auto* live = find_compact(key, eq_)) return live;
            hash = hash_(key);
            return nullptr;
        }
        hash = hash_(key);
        if (auto* view = current_controls(); view && view->edit_position < 0) {
            const size_t next = static_cast<size_t>(~std::exchange(view->edit_position, 0));
            if (view->absent_hash == hash && growth_left_ && next < capacity_ && ctrl_[next] < kSentinel) {
                position = next;
                ++view->paired_inserts;
                return nullptr;
            }
        }
        if (membership_ && membership_->known_absent_for_write(hash)) return nullptr;
        return find_hashed(key, hash, eq_);
    }

    void write_control(size_t slot, int8_t value) {
        invalidate_read_slot();
        set_ctrl(ctrl_, capacity_, slot, value);
    }

    void publish_slot(size_t slot, size_t hash) {
        if (membership_) membership_->add(hash);
        const int8_t h2 = H2(hash);
        if (ctrl_[slot] == kEmpty) --growth_left_;
        ++size_;
        if (capacity_ < kGroupWidth) compact_live_ |= uint8_t(1u << slot);
        write_control(slot, h2);
    }

    template<class Query, class QueryHash, class QueryEqual>
    V* find_impl(const Query& key, const QueryHash& query_hash, const QueryEqual& query_equal) {
        if (capacity_ < kGroupWidth && size_ <= 2) return find_compact(key, query_equal);
        return find_hashed(key, query_hash(key), query_equal);
    }

    template<class Query, class QueryEqual>
    V* find_hashed(const Query& key, size_t hash, const QueryEqual& query_equal) {
        if (capacity_ == 1) return find_compact(key, query_equal);
        const int8_t h2 = H2(hash);
        ProbeSeq seq(capacity_ < kGroupWidth ? 0 : H1(hash), capacity_ - 1);

        while (true) {
            Group g(ctrl_ + seq.pos());
            BitMask matches = g.match(h2);
            while (matches.has_next()) {
                size_t idx = wrap(seq.pos() + matches.next());
                slot_type& slot = slots_[idx];
                if (query_equal(slot.first, key)) {
                    return &slot.second;
                }
            }
            if (g.match_empty()) {
                return nullptr;  // Probe sequence ends at empty slot.
            }
            seq.next();
        }
    }

    // --- helpers ---

    static bool is_full(int8_t c) { return c >= 0; }  // H2 values are 0x00..0x7F

    size_t growth_budget(size_t capacity) const {
        if (capacity == 1) return 1; // A singleton performs an exact comparison and grows on a second key.
        size_t budget = static_cast<size_t>(static_cast<double>(capacity) * max_load_factor_);
        if (budget == 0) budget = 1;
        if (budget >= capacity) budget = capacity - 1;
        return budget;
    }

    size_t wrap(size_t idx) const {
        return idx >= capacity_ ? idx - capacity_ : idx;
    }

    // Find the first empty or deleted slot suitable for insertion.
    size_t slot_for_write(size_t hash) {
        if (membership_ && membership_->pending && membership_->pending_hash == hash
                && membership_->pending_slot != Membership::no_slot) {
            ++membership_->prepared_writes;
            return membership_->pending_slot;
        }
        return find_first_non_full(hash);
    }

    size_t find_first_non_full(size_t hash) const {
        ProbeSeq seq(capacity_ < kGroupWidth ? 0 : H1(hash), capacity_ - 1);
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

        if (capacity_ <= kGroupWidth) {
            if (capacity_ < kGroupWidth) compact_live_ &= uint8_t(~(1u << idx));
            // Every query examines this entire group before checking for an
            // empty slot. A deletion cannot terminate a probe to a later group.
            write_control(idx, kEmpty);
            ++growth_left_;
            return;
        }
        // Multi-group tables keep a tombstone to preserve longer probe chains.
        write_control(idx, kDeleted);
    }

    template <typename VV>
    std::pair<V*, bool> insert_or_assign_impl(const K& key, VV&& value) {
        size_t hash, position = capacity_;
        if (auto* existing = lookup_for_write(key, hash, position)) {
            assign_existing(*existing, std::forward<VV>(value));
            return {existing, false};
        }

        // Not found — insert
        if (growth_left_ == 0) {
            rehash_and_grow_if_necessary();
            return insert_or_assign_impl(key, std::forward<VV>(value));
        }

        size_t target = position != capacity_ ? position : slot_for_write(hash);
        construct_slot(target, key, std::forward<VV>(value));
        publish_slot(target, hash);
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
            if (max_capacity_ > 0 && capacity_ >= max_capacity_) {
                throw std::length_error("ForL0 SwissTable reached configured max-table-capacity");
            }
            size_t next_capacity = capacity_ == 1 ? 4 : capacity_ * 2;
            if (max_capacity_ > 0 && next_capacity > max_capacity_) {
                next_capacity = max_capacity_;
            }
            rehash(next_capacity);
        }
    }

    void rehash(size_t new_capacity) {
        withdraw_controls();
        slot_type* old_slots = slots_;
        SplitResult old_split = split_;
        size_t old_capacity = capacity_;

        auto* old_ctrl = allocate_and_init(new_capacity);
        if (membership_) membership_->pending_slot = Membership::no_slot;
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
                if (capacity_ < kGroupWidth) compact_live_ |= uint8_t(1u << target);
                set_ctrl(ctrl_, capacity_, target, h2);
                new (&slots_[target]) slot_type(std::move(old_slots[i]));
                old_slots[i].~slot_type();
            }
        }

        // Growth consumed the old controls directly. Their reserved backing
        // can be released without copying the controls out of the pool.
        release_probe_index(old_ctrl, ctrl_bytes(old_capacity));
        if (old_split.ctrl_ptr) {
            alloc_->deallocate_split(old_split,
                ctrl_bytes(old_capacity), old_capacity * sizeof(slot_type));
        }
        if constexpr (std::is_nothrow_move_constructible_v<slot_type>) {
            if (split_.temporary) recover_resize();
        }
    }

    void recover_resize() {
        const size_t controls = ctrl_bytes(capacity_);
        const size_t bytes = capacity_ * sizeof(slot_type);
        const size_t align = backing_slot_alignment(capacity_);
        const auto destination = alloc_->recover_split(split_, controls, align, bytes, align);
        if (!destination.ctrl_ptr) return;
        auto* slots = static_cast<slot_type*>(destination.slots_ptr);
        std::memcpy(destination.ctrl_ptr, ctrl_, controls);
        for (size_t i = 0; i < capacity_; ++i) {
            if (!is_full(ctrl_[i])) continue;
            new (&slots[i]) slot_type(std::move(slots_[i]));
            slots_[i].~slot_type();
        }
        alloc_->deallocate_split(split_, controls, bytes);
        split_ = destination;
        ctrl_ = static_cast<int8_t*>(destination.ctrl_ptr);
        slots_ = slots;
    }

    static size_t backing_slot_alignment(size_t capacity) {
        // A compact table's probe controls and first slots can share the
        // first cache line. Larger slot arrays retain cache-line alignment.
        const size_t alignment = slot_align<slot_type>();
        return capacity < kGroupWidth ? alignment : std::max(size_t(64), alignment);
    }

    static size_t rounded_capacity(size_t requested, bool compact) {
        if (compact && requested == 1) return 1;
        size_t capacity = compact ? 4 : kGroupWidth;
        while (capacity < requested) capacity <<= 1;
        return capacity;
    }

    static size_t packed_backing_bytes_for(size_t capacity) {
        const size_t alignment = backing_slot_alignment(capacity);
        return ((ctrl_bytes(capacity) + alignment - 1) & ~(alignment - 1))
            + capacity * sizeof(slot_type);
    }

    int8_t* allocate_and_init(size_t cap) {
        const size_t cb = ctrl_bytes(cap);
        const size_t sa = backing_slot_alignment(cap);
        const size_t slots_bytes = cap * sizeof(slot_type);

        // Allocate into locals first. If a configured memory limit rejects a
        // growth allocation, the existing table must remain fully usable.
        SplitResult new_split = alloc_->allocate_split(cb, sa, slots_bytes, sa);
        int8_t* new_ctrl = reinterpret_cast<int8_t*>(new_split.ctrl_ptr);
        slot_type* new_slots = reinterpret_cast<slot_type*>(new_split.slots_ptr);
        init_ctrl(new_ctrl, cap);

        split_ = new_split;
        slots_ = new_slots;
        capacity_ = cap;
        compact_live_ = 0;
        growth_left_ = growth_budget(cap);
        return std::exchange(ctrl_, new_ctrl);
    }

    void destroy_all_slots() {
        if (!ctrl_ || !slots_) return;
        if constexpr (!std::is_trivially_destructible_v<slot_type>) {
            for_each_live_slot(0, capacity_, [&](size_t i) { slots_[i].~slot_type(); });
        }
    }

    void free_backing() {
        withdraw_controls();
        release_probe_index(ctrl_, ctrl_bytes(capacity_));
        if (split_.ctrl_ptr) {
            alloc_->deallocate_split(split_,
                ctrl_bytes(capacity_), capacity_ * sizeof(slot_type));
            split_ = {nullptr, nullptr, false};
            ctrl_ = nullptr;
            slots_ = nullptr;
        }
    }

    void release_probe_index(void* storage, size_t bytes) noexcept {
        if (!index_owner_) return;
        index_usage_->bytes -= bytes;
        ++index_usage_->releases;
        index_owner_->deallocate(storage, bytes);
        index_owner_ = nullptr;
        index_usage_ = nullptr;
    }

    // --- members ---
    Allocator* alloc_;
    size_t size_;
    size_t capacity_;
    size_t growth_left_;
    int8_t* ctrl_;
    Allocator* index_owner_ = nullptr;
    ProbeIndexUsage* index_usage_ = nullptr;
    slot_type* slots_;
    SplitResult split_;   // tracks allocation for deallocation
    size_t max_capacity_; // 0 = unlimited
    double max_load_factor_;
    std::unique_ptr<Membership> membership_;
    ResidentControls* observer_ = nullptr;
    // Four- and eight-slot namespace tables can address one or two live rows
    // from the header without first fetching the array of hash fingerprints.
    uint8_t compact_live_ = 0;
    Hash hash_;
    KeyEqual eq_;
};

}  // namespace forl0
