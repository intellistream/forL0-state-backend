// StateEngine: Core state storage component.
// Replaces the Java ForL0StateStore hierarchy.
//
// Organization: KeyGroup → Namespace → SwissTable<K, V>
//   - VoidNamespace mode: tables[keyGroup] directly
//   - General Namespace mode: namespace_maps[keyGroup] → SwissTable per namespace
//
// Manages multiple named states (one StateTable per state descriptor).

#pragma once

#include "hot_cache.h"

#include "swiss_table.h"
#include "type_layout.h"
#include "allocator.h"
#include "namespace_cache.h"
#include "namespace_row.h"
#include "../checkpoint/flink_binary_format.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>
#include <mutex>
#include <atomic>
#include <limits>

namespace forl0 {

struct WindowStateKey {
    int64_t number;
    uint64_t header;
    bool operator==(const WindowStateKey& other) const noexcept {
        return number == other.number && header == other.header;
    }
};

template <>
struct SwissKeyHash<WindowStateKey> {
    size_t operator()(const WindowStateKey& key) const noexcept {
        const size_t hash = SwissKeyHash<int64_t>{}(key.number);
        return key.header ? hash ^ SwissKeyHash<uint64_t>{}(key.header) : hash;
    }
};

// Forward declaration
class StateTableBase;

// ============================================================================
//  StateTable: per-state-descriptor storage, templated on K, V
// ============================================================================

template <typename K, typename V>
class StateTable {
public:
    using Table = SwissTable<K, V>;

private:
    template<class T>
    static constexpr bool owns_tier_allocations = std::is_trivially_copyable_v<T>
            || std::uses_allocator_v<T, std::pmr::polymorphic_allocator<char>>;
    static constexpr bool namespace_placeable = owns_tier_allocations<K> && owns_tier_allocations<V>;
    struct RootMapPayloads {
        struct Slot {
            int64_t key = 0;
            uint64_t last_edit = 0;
        };
        RootMapPayloads(NamespaceCacheArena& arena, Allocator& dram, size_t& resident_bytes)
            : allocator(arena, dram, resident_bytes) {}
        RootMapPayloadAllocator allocator;
        std::array<Slot, 4> slots{};
        std::array<Slot, 4> candidates{};
    };
    struct DramStorage {
        explicit DramStorage(Allocator* backing) : dram(backing) {}
        NamespaceDramAllocator dram;
    };
    struct NamespaceTable;
    struct RootEditHints {
        // The root view already holds the latest position. Keep three older
        // positions for interleaved edits, with no copied keys or values.
        std::array<uint64_t, 3> positions{};
        unsigned probes = 0;
        unsigned hits = 0;
    };
    struct NamespaceUsage {
        RootEditHints* root_edit_hints = nullptr;
        int group_index = 0;
        uint16_t writes_before_admission = 1;
        bool partition = false;
        bool first_root_write = false;
        bool started_with_miss = false;
        bool backing_exceeds_quota = false;
        bool was_evicted = false;
        bool spilled = false;
        bool resident = false;
        uint8_t sampled_reads = 0;
        uint8_t sampled_empty = 0;
        bool request_membership = false;
        uint8_t membership_sample = 0;
        uint8_t sample_skips = 0;
        uint16_t ordinary_reads_left = 0;
        uint16_t probe_index_delay = 255;
        uint64_t last_shared_sample = 0;
        uint8_t dram_reads = 0;
        uint8_t resident_reads = 0;
        uint8_t admission_failures = 0;
        uint32_t reuse = 0;
        uint32_t partition_reads = 0;
        uint32_t partition_updates = 0;
        uint64_t reuse_at_probe = 0;
        uint64_t spent_dram_copy_credit = 0;

        uint64_t partition_reuse() const {
            return uint64_t{partition_reads} + partition_updates / 2;
        }
        size_t rows_at_probe = 0;
        size_t peak_window_rows = 0;
        // In L0 this is the admitted footprint. After a growth demotion it is
        // the most recent ordinary footprint sampled while considering readmission.
        size_t transfer_bytes = 0;
        uint64_t last_reference = 0;
        uint64_t negative_at_promotion = 0;
        uint64_t root_edit_retry_at = 32;
        NamespaceTable* older = nullptr;
        NamespaceTable* newer = nullptr;
    };
    struct NamespaceTable : private DramStorage, Table, NamespaceUsage {
        NamespaceTable(size_t capacity, Allocator* backing, size_t max_capacity,
                       double max_load_factor, NamespaceCacheArena* arena = nullptr)
            : DramStorage(backing),
              Table(capacity, arena ? static_cast<Allocator*>(arena) : &this->dram,
                    max_capacity, max_load_factor, arena && capacity < kGroupWidth) {}

        void return_to_dram() {
            if constexpr (std::uses_allocator_v<K, std::pmr::polymorphic_allocator<char>>
                    || std::uses_allocator_v<V, std::pmr::polymorphic_allocator<char>>) {
                if (this->size() >= 64) {
                    this->dram.pack_copy([&] { this->relocate(&this->dram); });
                    return;
                }
            }
            this->relocate(&this->dram);
        }

        void reset_empty_on_dram(size_t initial, size_t maximum, double load_factor) {
            Table fresh(initial, &this->dram, maximum, load_factor);
            static_cast<Table&>(*this) = std::move(fresh);
        }
        size_t dram_bytes() const { return this->dram.bytes(); }
        Allocator* dram_resource() { return &this->dram; }
    };
    using IntegerDirectory = std::unordered_map<int64_t, std::unique_ptr<NamespaceTable>>;
    using WindowDirectory = std::unordered_map<TimeWindow, std::unique_ptr<NamespaceTable>, TimeWindowHash>;
    using StringDirectory = std::unordered_map<std::string, std::unique_ptr<NamespaceTable>>;
    // Directory nodes have no remaining row or namespace binding after extraction.
    // Keep the common immediate successor allocation-free, and allocate the other
    // handles only when several windows close before their successors open.
    template<class Directory>
    class RetiredDirectoryNodes {
        using Node = typename Directory::node_type;
        using Batch = std::array<Node, 15>;
        Node first_;
        std::unique_ptr<Batch> batch_;
        size_t count_ = 0;
    public:
        bool empty() const { return first_.empty(); }
        Node take() {
            return count_ ? std::move((*batch_)[--count_]) : std::move(first_);
        }
        bool retain(Directory& directory, typename Directory::iterator found) {
            Node* destination = &first_;
            if (!first_.empty()) {
                if (!batch_) batch_.reset(new (std::nothrow) Batch{});
                if (!batch_ || count_ == batch_->size()) return false;
                destination = &(*batch_)[count_++];
            }
            *destination = directory.extract(found);
            destination->mapped().reset();
            return true;
        }
    };


    struct WindowReuse {
        size_t live_namespaces = 0;
        size_t capacity = 0;
        size_t last_fitting_capacity = 0;
        uint8_t small_lifetimes = 0;
        uint8_t singleton_lifetimes = 0;
        uint8_t small_openings = 0;
        uint8_t matching_lifetimes = 0;
        std::unique_ptr<NamespaceTable> empty_table;
    };
    struct NamespaceLookup {
        NamespaceTable* table = nullptr;
        enum class Kind : uint8_t { None, Long, Window } kind = Kind::None;
        int64_t first = 0, second = 0;
    };
    using NamespaceHints = std::array<NamespaceLookup, 4>;
    struct EmptyWindowBatch {
        std::array<std::unique_ptr<NamespaceTable>, 16> tables;
        size_t count = 0;
    };
    using WindowBatches = std::array<EmptyWindowBatch, 6>;
    static constexpr size_t window_reuse_bucket(size_t capacity) {
        switch (capacity) {
            case 1: return 0;
            case 4: return 1;
            case 8: return 2;
            case 16: return 3;
            case 32: return 4;
            case 64: return 5;
            default: return std::tuple_size<WindowBatches>::value;
        }
    }

    static bool has_reusable_medium_backing(const NamespaceTable& table) {
        return table.empty() && table.peak_window_rows > 3
                && table.capacity() >= 8 && table.capacity() <= 64;
    }

    struct ReadPosition {
        uintptr_t table_id = 0;
        size_t slot = 0;
    };
    // One checked cursor for successive changes to the same outer MapState row.
    // Its numeric key lets unrelated writes reject the hint before touching a data slot.
    struct MapEditPosition {
        const NamespaceTable* table = nullptr;
        int64_t key = 0;
        size_t slot = 0;
        uint8_t samples = 0, matches = 0;
    };
    static constexpr bool numeric_map = std::is_same_v<K, int64_t> && is_state_map<V>;

public:
    StateTable(int start_key_group, int num_key_groups, bool void_namespace,
               Allocator* alloc = &DefaultAllocator::instance(),
               size_t initial_table_capacity = 16,
               size_t max_table_capacity = 0,
               double max_load_factor = 0.875)
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          void_namespace_(void_namespace),
          alloc_(alloc),
          initial_table_capacity_(initial_table_capacity),
          max_table_capacity_(max_table_capacity),
          max_load_factor_(max_load_factor),
          cow_states_(num_key_groups) {
        // Always allocate flat tables — void-namespace API uses them directly,
        // and they serve as fallback for non-void-namespace states when Java
        // code calls void-namespace JNI functions (e.g., window operators).
        tables_.resize(num_key_groups);
        for (int i = 0; i < num_key_groups; ++i) {
            tables_[i] = std::make_unique<NamespaceTable>(initial_table_capacity_, alloc_,
                                                 max_table_capacity_, max_load_factor_);
            tables_[i]->group_index = i;
            tables_[i]->partition = true;
        }
        // Per-key-group mutexes for COW thread safety (async snapshot vs main thread)
        cow_mutexes_.resize(num_key_groups);
        for (int i = 0; i < num_key_groups; ++i) {
            cow_mutexes_[i] = std::make_unique<std::recursive_mutex>();
        }
        // OPT-4: Pre-size namespace and COW vectors for direct indexing
        int_namespace_maps_.resize(num_key_groups);
        str_namespace_maps_.resize(num_key_groups);
        tw_namespace_maps_.resize(num_key_groups);
        int_ns_cow_.resize(num_key_groups);
        str_ns_cow_.resize(num_key_groups);
        tw_ns_cow_.resize(num_key_groups);
    }

    ~StateTable() {
        if constexpr (numeric_map) {
            if (namespace_stats_.root_edit_hint_bytes) {
                for (auto& table : tables_) release_root_edit_hints(*table);
            }
        }
        release_map_edit();
        release_read_position();
    }

    StateTable(const StateTable&) = delete;
    StateTable& operator=(const StateTable&) = delete;

    void enable_namespace_storage(std::shared_ptr<HotCacheManager> manager, size_t bytes) {
        if constexpr (namespace_placeable) {
            if (namespace_arena_) return;
            if (!void_namespace_) namespace_lookup_.resize(num_key_groups_);
            if (!manager) return;
            try {
                auto arena = std::make_unique<NamespaceCacheArena>(std::move(manager), bytes, alloc_);
                if (arena->backing()) {
                    if (!void_namespace_) {
                        windows_.resize(num_key_groups_);
                        for (size_t i = 0; i < windows_.size(); ++i) {
                            auto& window = windows_[i];
                            window.live_namespaces = int_namespace_maps_[i].size()
                                    + str_namespace_maps_[i].size() + tw_namespace_maps_[i].size();
                            live_window_count_ += window.live_namespaces;
                            window.capacity = initial_table_capacity_ > kGroupWidth
                                ? initial_table_capacity_ / 2 : initial_table_capacity_;
                        }
                    }
                    root_copy_limit_ = 2 * arena->maximum_table_capacity();
                    arena->set_index_reclaimer(this, [](void* context, size_t bytes, size_t alignment) {
                        return static_cast<StateTable*>(context)->make_room_for_growth(bytes, alignment);
                    });
                    namespace_arena_ = std::move(arena);
                    if (void_namespace_ && !tables_.empty()) {
                        // Empty groups consume no cache space. Spend the first-write
                        // budget only as their namespace tables actually become active.
                        initial_root_bytes_left_ = namespace_arena_->primary_capacity() / 8;
                        for (auto& root : tables_) {
                            root->first_root_write = root->empty()
                                    && root->packed_backing_bytes() <= initial_root_bytes_left_;
                        }
                    }
                }
            } catch (const std::bad_alloc&) {
                // A cache allocation is optional; the namespace tables remain usable.
            }
        }
    }

    void set_serialized_namespace(std::unique_ptr<TypeLayout> ns) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) serialized_namespace_ = std::move(ns);
    }

    const NamespaceCacheArena* namespace_arena() const { return namespace_arena_.get(); }
    ResidentControls* resident_control_views() {
        if (!namespace_arena_ || sizeof(size_t) != 8) return nullptr;
        if (resident_controls_.empty()) {
            control_resource_.bind(namespace_arena_.get());
            try { resident_controls_.resize(tables_.size() * ResidentControls::per_group(void_namespace_)); }
            catch (const std::bad_alloc&) { return nullptr; }
        }
        return resident_controls_.data();
    }
    RootMembershipView* root_membership_views() {
        if constexpr (sizeof(size_t) != 8) return nullptr;
        if (!namespace_arena_ || !void_namespace_) return nullptr;
        if (root_memberships_.empty()) {
            control_resource_.bind(namespace_arena_.get());
            try {
                root_memberships_.resize(tables_.size());
            } catch (const std::bad_alloc&) { return nullptr; }
            for (auto& table : tables_) share_root_membership(*table);
        }
        return root_memberships_.data();
    }

    uint64_t membership_skips() const {
        uint64_t total = 0;
        for (const auto& view : root_memberships_) total += view.count();
        return total;
    }

    uint64_t shared_slot_reads() const {
        uint64_t count = 0;
        for (const auto& view : resident_controls_) count += view.hits();
        return count;
    }

    uint64_t shared_slot_writes() const {
        uint64_t total = 0;
        for (const auto& view : resident_controls_) total += view.writes();
        return total;
    }

    uint64_t shared_missing_namespaces() const {
        uint64_t total = 0;
        for (const auto& view : resident_controls_) total += view.namespace_misses();
        return total;
    }

    uint64_t pending_shared_writes() const {
        return shared_slot_writes() - namespace_stats_.resident_slot_writes;
    }

    uint64_t pending_shared_hits() const {
        return shared_slot_reads() - namespace_stats_.resident_slot_reads;
    }

    uint64_t pending_shared_reads() const {
        return membership_skips() - namespace_stats_.shared_negative_reads
                + control_skips() - namespace_stats_.resident_control_skips + pending_shared_hits()
                + shared_missing_namespaces() - namespace_stats_.missing_namespace_skips;
    }

    void flush_shared_reads() {
        const auto membership = membership_skips(), controls = control_skips(), hits = shared_slot_reads();
        const auto absent = shared_missing_namespaces();
        const auto misses = absent - namespace_stats_.missing_namespace_skips + membership - namespace_stats_.shared_negative_reads
                + controls - namespace_stats_.resident_control_skips;
        if (misses) namespace_arena_->backing()->record_serialized_misses(misses);
        if (hits > namespace_stats_.resident_slot_reads)
            namespace_arena_->backing()->record_serialized_hits(hits - namespace_stats_.resident_slot_reads);
        namespace_stats_.shared_negative_reads = membership;
        namespace_stats_.missing_namespace_skips = absent;
        namespace_stats_.resident_control_skips = controls;
        namespace_stats_.resident_slot_reads = hits;
        const auto writes = pending_shared_writes();
        if (writes) namespace_arena_->backing()->record_serialized_write(writes);
        namespace_stats_.resident_slot_writes += writes;
    }

    NamespaceCacheStats namespace_cache_stats() const {
        auto stats = namespace_stats_;
        stats.missing_namespace_skips = shared_missing_namespaces();
        stats.resident_control_skips = control_skips();
        stats.resident_slot_reads = shared_slot_reads();
        stats.resident_slot_writes = shared_slot_writes();
        for (const auto& view : resident_controls_) {
            stats.paired_slot_updates += view.paired_updates;
            stats.paired_slot_inserts += view.paired_inserts;
        }
        stats.probe_index_bytes = probe_indexes_.bytes;
        stats.probe_indexes_built = probe_indexes_.installs;
        stats.probe_indexes_released = probe_indexes_.releases;
        stats.resident_lookups += stats.resident_control_skips + stats.resident_slot_reads;
        stats.snapshot_index_scans = snapshot_marks_.scans();
        stats.snapshot_lookup_skips = snapshot_marks_.skips();
        stats.shared_negative_reads = membership_skips();
        stats.membership_probes += stats.shared_negative_reads;
        stats.membership_skips += stats.shared_negative_reads;
        stats.resident_tables = resident_count_;
        stats.root_map_payload_bytes = root_map_payload_bytes_;
        if (namespace_arena_) {
            stats.peak_table_loans = namespace_arena_->peak_table_loans();
            stats.recovered_resizes = namespace_arena_->recovered_resizes();
            stats.payload_run_slack = namespace_arena_->payload_run_slack();
            stats.payload_run_allocations = namespace_arena_->payload_run_allocations();
            stats.payload_runs_reserved = namespace_arena_->payload_runs_reserved();
            for (const auto& root : tables_) {
                stats.membership_write_skips += root->membership_write_skips();
                stats.prepared_writes += root->prepared_writes();
            }
            stats.control_view_bytes = control_resource_.pooled_bytes();
            stats.resident_bytes = namespace_arena_->capacity() - namespace_arena_->free_bytes() - stats.control_view_bytes
                    - stats.membership_bytes - stats.directory_bytes - stats.idle_window_bytes
                    - stats.read_position_bytes - stats.snapshot_index_bytes - stats.payload_run_slack;
        }
        return stats;
    }

    // ---- COW Snapshot support ----

    // OPT-5: Merged COW tracking — single map instead of overwritten + added_after.
    // cow_entries: optional<V> with value = overwritten (old value), nullopt = added_after.
    using CowHash = std::conditional_t<std::is_same_v<K, WindowStateKey>, SwissKeyHash<K>, std::hash<K>>;
    using FrozenRows = std::unordered_map<K, std::optional<V>, CowHash>;
    struct COWState {
        std::atomic<bool> active{false};
        FrozenRows cow_entries;
        std::unordered_map<K, V, CowHash> deleted;       // old values of deleted entries
    };

    // Per-namespace COW tracking (used by namespace put/remove)
    struct NsCowEntry {
        FrozenRows cow_entries;
        std::unordered_map<K, V, CowHash> deleted;
        void clear() { cow_entries.clear(); deleted.clear(); }
    };

    void prepare_snapshot() {
        if (namespace_arena_ && !namespace_stats_.snapshot_index_bytes) {
            namespace_stats_.snapshot_index_bytes = snapshot_marks_.reserve(*namespace_arena_);
        }
        for (int i = 0; i < num_key_groups_; ++i) {
            std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[i]);
            // Snapshot preparation runs on the task thread before its next state access.
            // The checkpoint reader may now run concurrently. Only native COW updates are valid.
            if (!resident_controls_.empty()) {
                const size_t width = ResidentControls::per_group(void_namespace_);
                for (size_t slot = 0; slot < width; ++slot) {
                    resident_controls_[i * width + slot].slot_layout &= ~ResidentControls::writable_bit;
                }
            }
            cow_states_[i].active = true;
            cow_states_[i].cow_entries.clear();
            cow_states_[i].deleted.clear();
            int_ns_cow_[i].clear();
            str_ns_cow_[i].clear();
            tw_ns_cow_[i].clear();
        }
    }

    void release_snapshot() {
        for (int i = 0; i < num_key_groups_; ++i)
            release_snapshot_group(start_key_group_ + i);
    }

    void release_snapshot_group(int group) {
        const int i = group - start_key_group_;
        if (!cow_states_[i].active.load(std::memory_order_acquire)) return;
        std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[i]);
        cow_states_[i].cow_entries.clear();
        cow_states_[i].deleted.clear();
        int_ns_cow_[i].clear();
        str_ns_cow_[i].clear();
        tw_ns_cow_[i].clear();
        // The task can now mutate this group without the COW lock. It republishes
        // writable resident slots itself at its next native table access.
        cow_states_[i].active.store(false, std::memory_order_release);
    }

    // Check if a snapshot is active for a given key group.
    // When true, in-place modifications to values obtained via get() are unsafe
    // and callers must use copy-and-replace through put() instead.
    bool is_snapshot_active(int key_group) const {
        return cow_states_[key_group - start_key_group_].active;
    }

    // ---- VoidNamespace operations ----

    V* get(int key_group, const K& key) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                auto [ns, inner] = split_serialized_key(key);
                return get(key_group, ns, inner);
            }
        }
        auto* table = tables_[key_group - start_key_group_].get();
        auto* value = find_root_value(*table, key);
        if constexpr (namespace_placeable) {
            if (!value && table->first_root_write) table->started_with_miss = true;
        }
        publish_resident_controls(table, ResidentControls::root_kind, 0, 0, value);
        record_namespace_read(table, value, false);
        return value;
    }

    // The mutation and the snapshot share the same resolved slot. Capture the
    // original only on the first change to that key in this snapshot.
    template <typename ModifyFn>
    bool modify_in_place(int key_group, const K& key, ModifyFn&& fn) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                auto [ns, inner] = split_serialized_key(key);
                return modify_in_place(key_group, ns, inner, std::forward<ModifyFn>(fn));
            }
        }
        const int idx = key_group - start_key_group_;
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        auto& cs = cow_states_[idx];
        if (cs.active) lock.lock();
        return modify_table(*tables_[idx], key, cs.active ? &cs.cow_entries : nullptr,
                            std::forward<ModifyFn>(fn));
    }

    template <typename ModifyFn>
    void modify_or_remove_in_place(int key_group, const K& key, ModifyFn&& fn) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                auto [ns, inner] = split_serialized_key(key);
                modify_or_remove_in_place(key_group, ns, inner, std::forward<ModifyFn>(fn));
                return;
            }
        }
        const int idx = key_group - start_key_group_;
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        auto& cs = cow_states_[idx];
        if (cs.active) lock.lock();
        auto& table = *tables_[idx];
        auto* value = find_for_edit(table, key);
        if (!value) return;
        if (cs.active) cs.cow_entries.try_emplace(key, *value);
        const bool grew = with_namespace_space(table, [&] { return edit_grows_map(table, *value, fn); });
        if (value->empty()) {
            if (cs.active) preserve_erasure(cs, key, *value);
            table.erase_at_value(*value);
            value = nullptr;
        }
        if (!table.empty() || table.spilled) {
            const bool moved = record_namespace_write(table, true, grew);
            if (!moved && value) remember_map_edit(table, key, *value);
        } else {
            return_empty_root_loan(table);
        }
    }

    // Namespace variant: COW-safe in-place modification.
    template <typename N, typename ModifyFn>
    bool modify_in_place(int key_group, const N& ns, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return false;
        return modify_table(*tbl, key, cs.active ? &get_or_create_ns_cow(idx, ns).cow_entries : nullptr,
                            std::forward<ModifyFn>(fn));
    }

    // Namespace variant: COW-safe in-place modification that may result in key removal.
    template <typename N, typename ModifyFn>
    void modify_or_remove_in_place(int key_group, const N& ns, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return;
        V* val = find_for_edit(*tbl, key);
        if (!val) return;
        auto* changes = cs.active ? &get_or_create_ns_cow(idx, ns) : nullptr;
        if (changes) changes->cow_entries.try_emplace(key, *val);
        with_namespace_space(*tbl, [&] { fn(*val); });
        if (val->empty()) {
            if (changes) preserve_erasure(*changes, key, *val);
            tbl->erase_at_value(*val);
            if (tbl->empty()) remove_namespace_table(key_group, ns);
            else record_namespace_write(*tbl, true);
        } else {
            record_namespace_write(*tbl, true);
        }
    }

    // Construct a missing mapped value with the authoritative table's
    // allocator, then perform the same mutation as for an existing value.
    template<class Fn>
    void update_or_create(int key_group, const K& key, Fn&& fn) {
        mutate_flat(key_group, key, edit_value(fn));
    }

    template<class N, class Fn>
    void update_or_create(int key_group, const N& ns, const K& key, Fn&& fn) {
        mutate_namespace(key_group, ns, key, edit_value(fn));
    }

    V* put(int key_group, const K& key, const V& value) {
        return put_flat(key_group, key, value);
    }

    V* put(int key_group, const K& key, V&& value) {
        return put_flat(key_group, key, std::move(value));
    }

    bool remove(int key_group, const K& key) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                auto [ns, inner] = split_serialized_key(key);
                return remove(key_group, ns, inner);
            }
        }
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto& table = *tables_[idx];
        auto* value = find_for_edit(table, key);
        if (!value) return false;
        if (cs.active) preserve_erasure(cs, key, *value);
        table.erase_at_value(*value);
        if (table.empty()) return_empty_root_loan(table);
        return true;
    }

    // ---- General Namespace operations ----

    template <typename N>
    V* get(int key_group, const N& ns, const K& key) {
        auto* tbl = find_namespace_table(key_group, ns, true);
        auto* value = tbl ? tbl->find(key) : nullptr;
        record_namespace_read(tbl, value);
        if (tbl) publish_window_controls(tbl, ns, value);
        else update_missing_namespace(key_group - start_key_group_, ns, true);
        return value;
    }

    template <typename N>
    V* put(int key_group, const N& ns, const K& key, const V& value) {
        return put_namespace(key_group, ns, key, value);
    }

    template <typename N>
    V* put(int key_group, const N& ns, const K& key, V&& value) {
        return put_namespace(key_group, ns, key, std::move(value));
    }

    template <typename N>
    bool remove(int key_group, const N& ns, const K& key) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return false;
        auto* value = find_for_edit(*tbl, key);
        if (!value) return false;
        if (cs.active) preserve_erasure(get_or_create_ns_cow(idx, ns), key, *value);
        tbl->erase_at_value(*value);
        if (tbl->empty()) remove_namespace_table(key_group, ns);
        return true;
    }

    // ---- Iteration (for checkpoint and getKeys) ----

    template <typename Fn>
    void for_each_void(Fn&& fn) const {
        for (int i = 0; i < num_key_groups_; ++i) {
            if (tables_[i]) {
                int kg = start_key_group_ + i;
                tables_[i]->for_each([&](const K& k, const V& v) {
                    fn(kg, k, v);
                });
            }
        }
    }

    template <typename Fn>
    void for_each_in_key_group(int key_group, Fn&& fn) const {
        int idx = key_group - start_key_group_;
        if (tables_[idx]) {
            tables_[idx]->for_each([&](const K& k, const V& v) {
                fn(k, v);
            });
        }
    }

    // Snapshot-consistent iteration for a key group (VoidNamespace mode).
    // Provides the state as it was at prepare_snapshot() time.
    template <typename Fn>
    void for_each_snapshot_in_key_group(int key_group, Fn&& fn) const {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                for_each_snapshot_in_key_group_ns<std::string>(key_group,
                    [&](const std::string& ns, const K& key, const V& value) {
                        K combined(ns);
                        combined.append(key);
                        fn(combined, value);
                    });
                return;
            }
        }
        int idx = key_group - start_key_group_;
        const auto& cs = cow_states_[idx];

        if (!cs.active) {
            // No active snapshot — just iterate normally
            for_each_in_key_group(key_group, std::forward<Fn>(fn));
            return;
        }

        std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[idx]);

        // OPT-6: Fast path — no COW modifications since snapshot
        if (cs.cow_entries.empty() && cs.deleted.empty()) {
            if (tables_[idx]) {
                tables_[idx]->for_each([&](const K& k, const V& v) {
                    fn(k, v);
                });
            }
            return;
        }

        if (tables_[idx]) visit_snapshot_rows(*tables_[idx], cs.cow_entries, fn);

        // Include deleted entries (they existed at snapshot time)
        for (const auto& [k, v] : cs.deleted) {
            fn(k, v);
        }
    }

    template <typename N, typename Fn>
    void for_each_in_namespace(int key_group, const N& ns, Fn&& fn) const {
        const auto& namespaces = get_ns_maps_ref<N>()[key_group - start_key_group_];
        const auto found = namespaces.find(ns);
        if (found == namespaces.end()) return;
        found->second->for_each([&](const K& key, const V& value) {
            fn(ns, key, value);
        });
    }

    template <typename N, typename Fn>
    void for_each_in_key_group_ns(int key_group, Fn&& fn) const {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            for (auto& [ns, tbl] : int_namespace_maps_[idx]) {
                tbl->for_each([&](const K& k, const V& v) {
                    fn(ns, k, v);
                });
            }
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            for (auto& [ns, tbl] : tw_namespace_maps_[idx]) {
                tbl->for_each([&](const K& k, const V& v) {
                    fn(ns, k, v);
                });
            }
        } else {
            for (auto& [ns, tbl] : str_namespace_maps_[idx]) {
                tbl->for_each([&](const K& k, const V& v) {
                    fn(ns, k, v);
                });
            }
        }
    }

    // Snapshot-consistent iteration for a key group with namespaces.
    // Provides the state as it was at prepare_snapshot() time.
    template <typename N, typename Fn>
    void for_each_snapshot_in_key_group_ns(int key_group, Fn&& fn) const {
        int idx = key_group - start_key_group_;
        const auto& cs = cow_states_[idx];

        if (!cs.active) {
            for_each_in_key_group_ns<N>(key_group, std::forward<Fn>(fn));
            return;
        }

        std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[idx]);

        // Select the appropriate namespace map and COW map
        const auto& ns_map = get_ns_maps_ref<N>()[idx];
        const auto& ns_cow = get_ns_cow_ref<N>()[idx];

        // OPT-6: Fast path — no namespace-level COW modifications
        if (ns_cow.empty()) {
            // Iterate directly under lock (can't call for_each_in_key_group_ns since table access must be protected)
            for (auto& [ns, tbl] : ns_map) {
                tbl->for_each([&](const K& k, const V& v) {
                    fn(ns, k, v);
                });
            }
            return;
        }

        // Step 1: Iterate current namespace tables with COW overrides
        for (auto& [ns, tbl] : ns_map) {
            auto emit = [&](const K& key, const V& value) { fn(ns, key, value); };
            const auto cow = ns_cow.find(ns);
            if (cow == ns_cow.end()) tbl->for_each(emit);
            else visit_snapshot_rows(*tbl, cow->second.cow_entries, emit);
        }

        // Step 2: Emit all deleted entries from COW
        for (auto& [ns, cow] : ns_cow) {
            for (auto& [k, v] : cow.deleted) {
                fn(ns, k, v);
            }
        }
    }

    // ---- Emplace (for restore) ----

    V* emplace_void(int key_group, const K& key, V&& value) {
        if (auto* existing = get(key_group, key)) return existing;
        return put(key_group, key, std::move(value));
    }

    // ---- Merge namespaces ----

    template <typename N, typename MergeFn>
    void merge_namespaces(int key_group, const N& target, const std::vector<N>& sources, MergeFn&& merge) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto* target_tbl = get_or_create_namespace_table(key_group, target);
        for (const auto& src_ns : sources) {
            if (src_ns == target) continue;
            auto* src_tbl = find_namespace_table(key_group, src_ns);
            if (!src_tbl) continue;
            // COW: save values before merge overwrites them
            if (cs.active) {
                src_tbl->for_each([&](const K& k, V& v) {
                    auto& nc_target = get_or_create_ns_cow(idx, target);
                    V* existing = target_tbl->find(k);
                    if (existing && !nc_target.cow_entries.count(k)) {
                        nc_target.cow_entries[k] = *existing;
                    }
                });
                auto& source_cow = get_or_create_ns_cow(idx, src_ns);
                src_tbl->for_each([&](const K& key, const V& value) {
                    auto change = source_cow.cow_entries.find(key);
                    if (change == source_cow.cow_entries.end()) {
                        source_cow.deleted.try_emplace(key, value);
                    } else if (change->second) {
                        source_cow.deleted.try_emplace(key, *change->second);
                    }
                });
                source_cow.cow_entries.clear();
            }
            with_namespace_space(*target_tbl, [&] {
                src_tbl->for_each([&](const K& key, V& value) {
                    if (auto* existing = target_tbl->find(key)) {
                        *existing = merge(*existing, std::move(value));
                    } else {
                        target_tbl->insert_or_assign(key, std::move(value));
                        if (cs.active) {
                            get_or_create_ns_cow(idx, target).cow_entries.emplace(key, std::nullopt);
                        }
                    }
                });
            });
            remove_namespace_table(key_group, src_ns);
        }
        if (!windows_.empty()) {
            if (target_tbl->size() >= 4 && target_tbl->peak_window_rows < 4) windows_[idx].small_openings = 0;
            target_tbl->peak_window_rows = std::max(target_tbl->peak_window_rows, target_tbl->size());
        }
        if (!target_tbl->empty()) record_namespace_write(*target_tbl, true);
    }

    // ---- State metadata ----

    bool is_void_namespace() const { return void_namespace_; }
    int start_key_group() const { return start_key_group_; }
    int num_key_groups() const { return num_key_groups_; }

    size_t total_size() const {
        size_t total = 0;
        for (auto& t : tables_) {
            if (t) total += t->size();
        }
        for (auto& ns_map : int_namespace_maps_) {
            for (auto& [ns, tbl] : ns_map) {
                total += tbl->size();
            }
        }
        for (auto& ns_map : str_namespace_maps_) {
            for (auto& [ns, tbl] : ns_map) {
                total += tbl->size();
            }
        }
        for (auto& ns_map : tw_namespace_maps_) {
            for (auto& [ns, tbl] : ns_map) {
                total += tbl->size();
            }
        }
        return total;
    }

    Table* get_table(int key_group) {
        int idx = key_group - start_key_group_;
        return tables_[idx].get();
    }

    const Table* get_table(int key_group) const {
        int idx = key_group - start_key_group_;
        return tables_[idx].get();
    }

private:
    template<class Changes, class Fn>
    void visit_snapshot_rows(const NamespaceTable& table, const Changes& changes, Fn& emit) const {
        if (changes.empty()) {
            table.for_each(emit);
            return;
        }
        auto changed = [&](const K& key, const V& value) {
            const auto old = changes.find(key);
            if (old == changes.end()) emit(key, value);
            else if (old->second) emit(key, *old->second);
        };
        if (namespace_arena_ && snapshot_marks_.visit(table, changes, emit, changed)) return;
        table.for_each(changed);
    }

    template<class Changes>
    static void preserve_erasure(Changes& changes, const K& key, const V& value) {
        const auto previous = changes.cow_entries.find(key);
        if (previous == changes.cow_entries.end()) {
            changes.deleted.try_emplace(key, value);
            return;
        }
        if (previous->second) changes.deleted.try_emplace(key, std::move(*previous->second));
        changes.cow_entries.erase(previous);
    }

    // Helpers for namespace COW map access
    template <typename N>
    NsCowEntry& get_or_create_ns_cow(int idx, const N& ns) {
        if constexpr (std::is_same_v<N, int64_t>) {
            return int_ns_cow_[idx][ns];
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return tw_ns_cow_[idx][ns];
        } else {
            return str_ns_cow_[idx][ns];
        }
    }

    template <typename N>
    auto& get_ns_maps_ref() {
        if constexpr (std::is_same_v<N, int64_t>) {
            return int_namespace_maps_;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return tw_namespace_maps_;
        } else {
            return str_namespace_maps_;
        }
    }

    template <typename N>
    const auto& get_ns_maps_ref() const {
        if constexpr (std::is_same_v<N, int64_t>) {
            return int_namespace_maps_;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return tw_namespace_maps_;
        } else {
            return str_namespace_maps_;
        }
    }

    template <typename N>
    const auto& get_ns_cow_ref() const {
        if constexpr (std::is_same_v<N, int64_t>) {
            return int_ns_cow_;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return tw_ns_cow_;
        } else {
            return str_ns_cow_;
        }
    }

    std::pair<std::string, K> split_serialized_key(const K& encoded) const {
        ReadBuffer input(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size());
        input.skip_typed_value(*serialized_namespace_);
        const size_t prefix = input.position();
        return {std::string(encoded.data(), prefix), K(encoded.data() + prefix, encoded.size() - prefix)};
    }

    bool tracks_root_position(const NamespaceTable& table) const {
        return void_namespace_ && !resident_controls_.empty() && !table.resident && table.size() >= 256;
    }

    static uint32_t root_position_tag(const K& key) {
        uint64_t bits;
        if constexpr (std::is_same_v<K, FixedPair>) {
            const uint64_t second = static_cast<uint64_t>(key.f[1]);
            bits = static_cast<uint64_t>(key.f[0]) ^ ((second << 23) | (second >> 41));
        } else {
            bits = static_cast<uint64_t>(key);
        }
        return static_cast<uint32_t>(bits ^ (bits >> 32));
    }

    V* previous_root_value(NamespaceTable& table, const K& key, uint32_t tag) {
        const auto& view = resident_controls_[table.group_index];
        const auto position = view.last_root_row;
        return static_cast<uint32_t>(position) && uint32_t(position >> 32) == tag
                ? table.value_at_root_position(view, position, key) : nullptr;
    }

    void remember_root_value(NamespaceTable& table, uint32_t tag, V& value, const K* edited_key = nullptr) {
        const size_t slot = table.position_of(value);
        if (slot >= UINT32_MAX) return;
        auto& view = resident_controls_[table.group_index];
        const auto position = (uint64_t{tag} << 32) | (slot + 1);
        if constexpr (numeric_map) {
            const auto previous = view.last_root_row;
            if (previous && static_cast<uint64_t>(view.second) == previous) {
                const bool repeated = previous == position;
                if constexpr (std::is_nothrow_move_constructible_v<V>) {
                    if (edited_key && (repeated || table.root_edit_hints)
                            && namespace_stats_.dram_root_updates >= next_root_map_payload_sample_) {
                        sample_root_map_payload(table, *edited_key, value, position, repeated);
                    }
                }
                if (repeated) return;
                if (edited_key && (table.root_edit_hints
                        || namespace_stats_.dram_root_updates >= table.root_edit_retry_at)) {
                    remember_older_root_edit(table, previous, position);
                }
            }
        }
        table.remember_root_position(view, position);
    }

    V* find_root_value(NamespaceTable& table, const K& key) {
        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, FixedPair>) {
            if (tracks_root_position(table)) {
                // The L0 directory holds a position in the authoritative DRAM table.
                // A positive result always checks the complete key.
                const auto tag = root_position_tag(key);
                if (auto* value = previous_root_value(table, key, tag)) {
                    ++namespace_stats_.repeated_root_reads;
                    return value;
                }
                auto* value = lookup_root_table(table, key);
                if (value) remember_root_value(table, tag, *value);
                return value;
            }
        }
        auto* value = lookup_root_table(table, key);
        if (namespace_arena_) remember_read_position(table, value);
        return value;
    }

    V* lookup_root_table(NamespaceTable& table, const K& key) {
        if constexpr (namespace_placeable) {
            if (table.has_membership()) {
                if (table.ordinary_reads_left) {
                    if (!--table.ordinary_reads_left) table.enable_shared_membership(true);
                    ++namespace_stats_.membership_bypassed;
                    return table.find(key);
                }
                bool absent = false;
                auto* value = table.find_with_membership(key, absent);
                ++namespace_stats_.membership_probes;
                namespace_stats_.membership_skips += absent;
                table.sample_skips += absent;
                if (++table.membership_sample == 64) {
                    // A cheap scalar key needs more avoided probes to pay
                    // for checking the summary than a multi-field key.
                    constexpr unsigned min_skips = std::is_integral_v<K> ? 44 : 24;
                    uint64_t extra = 0;
                    if (!root_memberships_.empty()) {
                        const auto total = root_memberships_[table.group_index].count();
                        extra = total - table.last_shared_sample;
                        table.last_shared_sample = total;
                    }
                    if ((extra + table.sample_skips) * 64 < (64 + extra) * min_skips) {
                        table.ordinary_reads_left = 1024;
                        table.enable_shared_membership(false);
                    }
                    table.membership_sample = table.sample_skips = 0;
                }
                return value;
            }
        }
        return table.find(key);
    }

    void share_root_membership(NamespaceTable& table) {
        if (!table.has_membership() || root_memberships_.empty()) return;
        auto& view = root_memberships_[table.group_index];
        table.share_membership(view, namespace_arena_->pool_base());
        table.last_shared_sample = view.count();
        if (table.ordinary_reads_left) table.enable_shared_membership(false);
    }

    void consider_probe_index(NamespaceTable& table) {
        if (!table.partition || table.resident || table.has_probe_index() || table.size() < 128) return;
        if (table.probe_index_delay) { --table.probe_index_delay; return; }
        table.probe_index_delay = 2047;
        // A filter that rules out reads still leaves the inserter looking for an
        // empty slot. Once those read/insert pairs can pay for copying the
        // controls, the task can identify absence and the insertion slot together.
        if (table.has_membership() && !table.ordinary_reads_left
                && table.shared_absence_inserts() < std::max<size_t>(128, table.capacity() / 4)) return;
        const size_t capacity = namespace_arena_->primary_capacity();
        const size_t bytes = ctrl_bytes(table.capacity());
        const size_t other_summaries = namespace_stats_.membership_bytes - table.membership_bytes();
        // Keep at least one quarter of the primary span available for whole-table placement.
        if (bytes > capacity / 2 || other_summaries + probe_indexes_.bytes + bytes > capacity * 3 / 4
                || bytes + capacity / 8 > namespace_arena_->free_bytes()) return;
        auto* copy = namespace_arena_->try_allocate_metadata(bytes, 16);
        if (!copy) return;
        std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[table.group_index]);
        if (table.has_membership()) release_root_membership(table);
        table.attach_probe_index(copy, *namespace_arena_, probe_indexes_);
    }

    void release_root_membership(NamespaceTable& table) noexcept {
        namespace_stats_.membership_bytes -= table.membership_bytes();
        namespace_stats_.membership_write_skips += table.membership_write_skips();
        namespace_stats_.prepared_writes += table.prepared_writes();
        table.drop_membership();
        table.request_membership = false;
        table.sampled_reads = table.sampled_empty = 0;
        table.membership_sample = table.sample_skips = 0;
        table.ordinary_reads_left = 1024;
        ++namespace_stats_.memberships_released;
    }

    void consider_membership(NamespaceTable& table) {
        table.request_membership = false;
        if (table.has_probe_index()) return;
        // Cap all root summaries together at half the state's primary span.
        // The other half and elastic loans still admit authoritative tables.
        const size_t per_table = namespace_arena_->primary_capacity() / (2 * tables_.size());
        if (per_table < 64 || per_table > namespace_arena_->free_bytes()) return;
        try {
            if (table.install_membership(*namespace_arena_, per_table)) {
                table.membership_sample = table.sample_skips = 0;
                table.ordinary_reads_left = 0;
                share_root_membership(table);
                namespace_stats_.membership_bytes += table.membership_bytes();
                ++namespace_stats_.memberships_built;
            }
        } catch (const std::bad_alloc&) {
            // Fragmentation or allocation pressure can decline this optional index.
        }
    }

    void release_read_position() noexcept {
        if (!read_position_) return;
        read_position_->~ReadPosition();
        namespace_arena_->deallocate(read_position_, sizeof(ReadPosition));
        read_position_ = nullptr;
        namespace_stats_.read_position_bytes = 0;
        position_samples_ = position_matches_ = 0;
    }

    void remember_read_position(const NamespaceTable& table, const V* value) {
        if (!value) {
            if (read_position_) read_position_->table_id = 0;
            return;
        }
        if (position_samples_ == 64) {
            if (position_matches_ < 16) {
                release_read_position();
                position_retry_ = 1023;
                ++namespace_stats_.read_position_pauses;
                return;
            }
            position_samples_ = position_matches_ = 0;
        }
        if (!read_position_) {
            if (position_retry_) {
                --position_retry_;
                return;
            }
            position_retry_ = 1023;
            if (namespace_arena_->primary_capacity() / 128 < sizeof(ReadPosition)
                    || namespace_arena_->free_bytes() / 4 < sizeof(ReadPosition)) return;
            void* storage = namespace_arena_->try_allocate_metadata(sizeof(ReadPosition), alignof(ReadPosition));
            if (!storage) return;
            read_position_ = new (storage) ReadPosition{};
            namespace_stats_.read_position_bytes = NamespaceCacheArena::allocation_bytes(sizeof(ReadPosition));
        }
        ++position_samples_;
        *read_position_ = {reinterpret_cast<uintptr_t>(&table), table.position_of(*value)};
    }

    void release_map_edit() noexcept {
        if (!map_edit_) return;
        map_edit_->~MapEditPosition();
        namespace_arena_->deallocate(map_edit_, sizeof(MapEditPosition));
        map_edit_ = nullptr;
        namespace_stats_.map_edit_bytes = 0;
        map_edit_retry_ = 1023;
    }

    void release_root_edit_hints(NamespaceTable& table) noexcept {
        if (!table.root_edit_hints) return;
        table.root_edit_hints->~RootEditHints();
        namespace_arena_->deallocate(table.root_edit_hints, sizeof(RootEditHints));
        table.root_edit_hints = nullptr;
        table.root_edit_retry_at = namespace_stats_.dram_root_updates + root_edit_retry_interval;
        namespace_stats_.root_edit_hint_bytes -= NamespaceCacheArena::allocation_bytes(sizeof(RootEditHints));
    }

    [[gnu::noinline]] void remember_older_root_edit(NamespaceTable& table, uint64_t previous, uint64_t current) {
        if (!table.root_edit_hints) {
            table.root_edit_retry_at = namespace_stats_.dram_root_updates + root_edit_retry_interval;
            const size_t bytes = NamespaceCacheArena::allocation_bytes(sizeof(RootEditHints));
            if (namespace_stats_.root_edit_hint_bytes + bytes > namespace_arena_->primary_capacity() / 128) return;
            void* storage = namespace_arena_->try_allocate_metadata(sizeof(RootEditHints), alignof(RootEditHints));
            if (!storage) return;
            table.root_edit_hints = new (storage) RootEditHints{};
            namespace_stats_.root_edit_hint_bytes += bytes;
        }
        auto& positions = table.root_edit_hints->positions;
        auto end = std::find(positions.begin(), positions.end() - 1, current);
        std::move_backward(positions.begin(), end, end + 1);
        positions.front() = previous;
    }

    [[gnu::noinline]] V* find_older_root_edit(NamespaceTable& table, RootEditHints& hints, const K& key, uint32_t tag) {
        V* value = nullptr;
        for (const auto position : hints.positions) {
            const auto index = static_cast<uint32_t>(position);
            if (index && static_cast<uint32_t>(position >> 32) == tag) {
                value = table.value_at_if_equal(index - 1, key);
                if (value) break;
            }
        }
        if (value) {
            ++hints.hits;
            ++namespace_stats_.root_edit_hint_hits;
        }
        if (++hints.probes == 64) {
            if (hints.hits < 16) release_root_edit_hints(table);
            else hints.probes = hints.hits = 0;
        }
        return value;
    }

    [[gnu::noinline]] void sample_root_map_payload(
            NamespaceTable& table, const K& key, V& value, uint64_t position, bool repeated) {
        const auto now = namespace_stats_.dram_root_updates;
        // An odd interval samples across interleaved groups and keys.
        next_root_map_payload_sample_ = now + 47;
        if (!repeated) {
            const auto& positions = table.root_edit_hints->positions;
            if (std::find(positions.begin(), positions.end(), position) == positions.end()) return;
        }
        try {
            if (root_map_payloads_.empty()) {
                if (value.size() < 2 || !namespace_arena_->primary_capacity()) return;
                root_map_payloads_.resize(tables_.size());
            }
            auto& storage = root_map_payloads_[table.group_index];
            if (storage && value.get_allocator().resource() == &storage->allocator) {
                for (auto& slot : storage->slots) {
                    if (slot.last_edit && slot.key == key) {
                        slot.last_edit = now;
                        break;
                    }
                }
                return;
            }
            if (value.size() < 2 || value.copy_storage_bytes() > 4096) return;
            if (!storage) {
                storage = std::make_unique<RootMapPayloads>(
                        *namespace_arena_, *table.dram_resource(), root_map_payload_bytes_);
            }
            // A read followed by an edit may be the only event for this key.
            // Observe the key in two separate samples before copying its map.
            auto& candidates = storage->candidates;
            auto candidate = std::find_if(candidates.begin(), candidates.end(), [&](const auto& entry) {
                return entry.last_edit && entry.key == key;
            });
            const auto less_recent = [](const auto& lhs, const auto& rhs) {
                return lhs.last_edit < rhs.last_edit;
            };
            if (candidate == candidates.end()) {
                *std::min_element(candidates.begin(), candidates.end(), less_recent) = {key, now};
                return;
            }
            candidate->last_edit = now;
            auto& slots = storage->slots;
            auto slot = std::min_element(slots.begin(), slots.end(), less_recent);
            if (slot->last_edit) {
                auto* old = table.find(slot->key);
                if (old && old->get_allocator().resource() == &storage->allocator) {
                    if (now - slot->last_edit < 512) return;
                    old->relocate_storage(typename V::allocator_type(table.dram_resource()));
                    ++namespace_stats_.root_map_payload_evictions;
                }
                slot->last_edit = 0;
            }
            RootMapPayloadAllocator::Placement placement(storage->allocator);
            value.relocate_storage(typename V::allocator_type(&storage->allocator));
            *slot = {key, now};
            ++namespace_stats_.root_map_payload_promotions;
        } catch (const std::bad_alloc&) {
            // The live row remains valid when the optional move fails.
            next_root_map_payload_sample_ = now + 1023;
        }
    }

    void remember_map_edit(NamespaceTable& table, const K& key, V& value, bool edited_existing = true) {
        if constexpr (numeric_map) {
            if (!namespace_arena_ || !table.partition) return;
            if (tracks_root_position(table)) {
                remember_root_value(table, root_position_tag(key), value, edited_existing ? &key : nullptr);
                return;
            }
            if (!map_edit_) {
                if (map_edit_retry_) { --map_edit_retry_; return; }
                map_edit_retry_ = 1023;
                if (namespace_arena_->primary_capacity() / 128 < sizeof(MapEditPosition)
                        || namespace_arena_->free_bytes() / 4 < sizeof(MapEditPosition)) return;
                auto* memory = namespace_arena_->try_allocate_metadata(sizeof(MapEditPosition), alignof(MapEditPosition));
                if (!memory) return;
                map_edit_ = new (memory) MapEditPosition{};
                namespace_stats_.map_edit_bytes = NamespaceCacheArena::allocation_bytes(sizeof(MapEditPosition));
            }
            if (map_edit_->samples == 64) {
                if (map_edit_->matches < 8) {
                    release_map_edit();
                    ++namespace_stats_.map_edit_pauses;
                    return;
                }
                map_edit_->samples = map_edit_->matches = 0;
            }
            ++map_edit_->samples;
            map_edit_->table = &table;
            map_edit_->key = key;
            map_edit_->slot = table.position_of(value);
        }
    }

    V* take_map_edit(NamespaceTable& table, const K& key) {
        if constexpr (numeric_map) {
            if (map_edit_ && map_edit_->table == &table && map_edit_->key == key) {
                map_edit_->table = nullptr;
                if (auto* value = table.value_at_if_equal(map_edit_->slot, key)) {
                    ++map_edit_->matches;
                    ++namespace_stats_.map_edit_reuses;
                    return value;
                }
            }
        }
        return nullptr;
    }

    V* consume_read_position(NamespaceTable& table, const K& key) {
        if constexpr (std::is_same_v<K, int64_t> || std::is_same_v<K, FixedPair>) {
            if (tracks_root_position(table)) {
                // The per-group position already recorded by the root lookup
                // can also serve the following edit.
                const auto tag = root_position_tag(key);
                auto* value = previous_root_value(table, key, tag);
                if constexpr (numeric_map) {
                    if (!value && table.root_edit_hints) value = find_older_root_edit(table, *table.root_edit_hints, key, tag);
                }
                if (value) ++namespace_stats_.repeated_root_writes;
                return value;
            }
        }
        if (!resident_controls_.empty()) {
            if (auto* value = table.take_read_slot(key)) return value;
        }
        if (read_position_ && read_position_->table_id) {
            const auto position = *read_position_;
            read_position_->table_id = 0;
            if (position.table_id == reinterpret_cast<uintptr_t>(&table)) {
                if (auto* value = table.value_at_if_equal(position.slot, key)) {
                    ++position_matches_;
                    ++namespace_stats_.read_position_writes;
                    return value;
                }
            }
        }
        return take_map_edit(table, key);
    }

    V* find_for_edit(NamespaceTable& table, const K& key) {
        if (auto* previous = consume_read_position(table, key)) return previous;
        return table.find(key);
    }

    // On failure a newly inserted slot must remain invisible both in the
    // running state and in the checkpoint. Existing entries keep their owner.
    static void capture_slot(NamespaceTable& table, const K& key, V& value, bool inserted, FrozenRows* rows) {
        if (!rows) return;
        if (!inserted) {
            rows->try_emplace(key, value);
            return;
        }
        try {
            rows->try_emplace(key, std::nullopt);
        } catch (...) {
            table.erase_at_value(value);
            throw;
        }
    }

    struct WriteResult {
        V* value;
        bool inserted;
        bool inner_growth;
    };

    // Repeatedly adding new inner keys touches the outer slot but still grows
    // the working set. Count a replacement of an existing inner entry as reuse.
    template<class Fn>
    bool edit_grows_map(const NamespaceTable& table, V& value, Fn&& edit) {
        if constexpr (is_state_map<V>) {
            if (namespace_arena_ && table.partition && !table.backing_exceeds_quota) {
                const auto before = value.size();
                edit(value);
                return value.size() > before;
            }
        }
        edit(value);
        return false;
    }

    template<class Input>
    WriteResult assign_value(NamespaceTable& table, const K& key, Input&& input, FrozenRows* rows) {
        auto* previous = consume_read_position(table, key);
        auto result = [&]() -> std::pair<V*, bool> {
            if (previous) return {previous, false};
            if constexpr (std::is_same_v<V, NamespaceBlob>) return table.emplace(key, std::string_view(input));
            else return table.emplace(key, std::forward<Input>(input));
        }();
        capture_slot(table, key, *result.first, result.second, rows);
        const bool grew = !result.second && edit_grows_map(table, *result.first, [&](V& existing) {
            Table::assign_existing(existing, std::forward<Input>(input));
        });
        return {result.first, result.second, grew};
    }

    uint64_t control_skips() const {
        uint64_t total = 0;
        for (const auto& view : resident_controls_) total += view.count();
        return total;
    }

    // Only a canonical namespace-directory miss can publish this certificate. A live table
    // (including one in DRAM) always takes precedence and may reuse this descriptor.
    template <class N>
    void update_missing_namespace(int group, const N& ns, bool missing) {
        if constexpr (std::is_same_v<N, int64_t> || std::is_same_v<N, TimeWindow>) {
            if (resident_controls_.empty() || void_namespace_) return;
            const auto identity = describe_namespace(ns, nullptr);
            const uint64_t kind = std::is_same_v<N, int64_t> ? 2 : 3;
            const size_t slot = group * ResidentControls::per_group(false)
                    + ResidentControls::window_bucket(identity.first, identity.second);
            auto& view = resident_controls_[slot];
            if (missing) {
                ++namespace_stats_.missing_namespace_reads;
                if (view.address) { ++namespace_stats_.missing_namespace_busy; return; }
                view.first = identity.first;
                view.second = identity.second;
                view.kind = kind | ResidentControls::missing_bit;
            } else if (view.kind == (kind | ResidentControls::missing_bit)
                    && view.first == identity.first && view.second == identity.second) {
                view.kind = 0;
            }
        }
    }

    template <class N>
    void publish_window_controls(NamespaceTable* table, const N& ns, const V* value, bool recurring = false) {
        if constexpr (std::is_same_v<N, int64_t>) publish_resident_controls(table, 2, ns, 0, value, recurring);
        else if constexpr (std::is_same_v<N, TimeWindow>) publish_resident_controls(table, 3, ns.first, ns.second, value, recurring);
    }

    void publish_resident_controls(NamespaceTable* table, uint64_t kind, int64_t first, int64_t second,
                                   const V* value, bool recurring = false) {
        constexpr unsigned key_layout = std::is_same_v<K, int64_t> ? 1 : std::is_same_v<K, int32_t> ? 2
                : std::is_same_v<K, WindowStateKey> ? 3 : std::is_same_v<K, FixedPair> ? 4 : 0;
        constexpr bool fixed_map = std::is_same_v<V, InnerMapLongLong> || std::is_same_v<V, InnerMapTimeWindow>
                || std::is_same_v<V, InnerMapRowFour> || std::is_same_v<V, InnerMapWideRowFour>
                || std::is_same_v<V, InnerMapLongString>;
        constexpr bool readable = key_layout && (std::is_same_v<V, int64_t> || std::is_same_v<V, double>
                || std::is_same_v<V, NamespaceRow> || is_fixed_window_row<V> || std::is_same_v<V, ElementList> || fixed_map);
        if (resident_controls_.empty() || !table || table->spilled) return;
        if (value ? (!readable || !table->resident) : (!table->resident && !table->has_probe_index())) return;
        const size_t index = void_namespace_ ? table->group_index
                : table->group_index * ResidentControls::per_group(false) + ResidentControls::window_bucket(first, second);
        auto& view = resident_controls_[index];
        table->publish_controls(view, kind, first, second);
        if constexpr (readable) {
            if (!value || (!recurring && table->resident_reads < reads_to_warm)) return;
            constexpr bool mutable_value = std::is_same_v<V, int64_t> || std::is_same_v<V, double>
                    || std::is_same_v<V, NamespaceRow> || is_fixed_window_row<V> || fixed_map;
            const auto writable = mutable_value && !cow_states_[table->group_index].active
                    ? ResidentControls::writable_bit : uint64_t{0};
            if (view.slots) {
                view.slot_layout |= writable;
                return;
            }
            // Derive offsets from a constructed slot, including its actual pair layout on this ABI.
            const auto index = table->position_of(*value);
            const auto key = reinterpret_cast<uintptr_t>(&table->slot_data()[index].first);
            auto offset = [&](const void* field) { return reinterpret_cast<uintptr_t>(field) - key; };
            using Slot = typename Table::slot_type;
            static_assert(sizeof(Slot) < (1u << 16));
            uint64_t encoding = sizeof(Slot) | (uint64_t(key_layout) << 16);
            if constexpr (std::is_same_v<V, NamespaceRow> || is_fixed_window_row<V> || std::is_same_v<V, ElementList>) {
                const auto& bytes = [&]() -> const auto& {
                    if constexpr (std::is_same_v<V, ElementList>) return value->storage();
                    else return *value;
                }();
                encoding |= offset(bytes.representation_address()) << 24 | offset(bytes.storage_address()) << 40
                        | uint64_t(std::decay_t<decltype(bytes)>::slot_format) << 56;
            } else if constexpr (fixed_map) {
                using Longs = std::pair<int64_t, int64_t>;
                using Windows = std::pair<TimeWindow, TimeWindow>;
                static_assert(offsetof(Longs, first) == 0 && offsetof(Longs, second) == 8 && sizeof(Longs) == 16);
                static_assert(offsetof(Windows, first) == 0 && offsetof(Windows, second) == 16 && sizeof(Windows) == 32);
                using WideRowInt = std::pair<WideMapKey, FourByteMapValue>;
                static_assert(WideMapKey::length_offset() == 20 && offsetof(WideRowInt, first) == 0);
                static_assert(sizeof(WideRowInt) == 32 && offsetof(WideRowInt, second) == 24);
                using RowInt = std::pair<CompactMapKey, FourByteMapValue>;
                static_assert(CompactMapKey::length_offset() == 44 && offsetof(RowInt, first) == 0);
                static_assert(sizeof(RowInt) == 56 && offsetof(RowInt, second) == 48);
                using LongRow = std::pair<int64_t, NamespaceRow>;
                static_assert(sizeof(LongRow) == 40 && offsetof(LongRow, first) == 0 && offsetof(LongRow, second) == 8);
                static_assert(NamespaceRow::representation_offset() == 0 && NamespaceRow::storage_offset() == 8);
                constexpr uint64_t format = std::is_same_v<V, InnerMapWideRowFour> ? 249
                        : std::is_same_v<V, InnerMapLongString> ? 250
                        : std::is_same_v<V, InnerMapRowFour> ? 251
                        : std::is_same_v<V, InnerMapTimeWindow> ? 254 : 253;
                encoding |= offset(value->extent_address()) << 24 | offset(value->storage_address()) << 40
                        | format << 56;
            } else {
                encoding |= offset(value) << 24;
            }
            view.slot_layout = encoding | writable;
            view.slots = key - index * sizeof(Slot);
        }
    }

    void record_namespace_read(NamespaceTable* table, const V* value, bool record_position = true) {
        if constexpr (namespace_placeable) {
            if (!namespace_arena_) return;
            if (record_position && table) remember_read_position(*table, value);
            const bool resident = table && table->resident && !table->spilled;
            if (table && table->partition && !resident && !table->has_membership() && !table->has_probe_index()
                    && !table->request_membership && table->size() >= 64) {
                if (table->ordinary_reads_left) {
                    --table->ordinary_reads_left;
                } else {
                    table->sampled_empty += value == nullptr;
                    ++table->sampled_reads;
                }
                if (table->sampled_reads == 64) {
                    table->request_membership = table->sampled_empty >= 48;
                    table->sampled_reads = table->sampled_empty = 0;
                }
            }
            namespace_arena_->backing()->record_serialized_lookup(resident && value);
            if (resident) {
                ++namespace_stats_.resident_lookups;
                if (value && table->partition) {
                    root_copy_budget_ = std::min(root_copy_limit_, root_copy_budget_ + 32);
                }
                if (value) {
                    reference_resident(*table);
                    warm_resident(*table);
                }
            } else if (value && !table->backing_exceeds_quota) {
                observe_namespace_reuse(*table);
                if (table->dram_reads < reads_to_warm) ++table->dram_reads;
                if (table->partition && table->partition_reads != UINT32_MAX) {
                    ++table->partition_reads;
                }
            }
        }
    }

    template<class Fn>
    bool modify_table(NamespaceTable& table, const K& key, FrozenRows* rows, Fn&& fn) {
        auto* value = find_for_edit(table, key);
        if (!value) return false;
        if (rows) rows->try_emplace(key, *value);
        const bool grew = with_namespace_space(table, [&] { return edit_grows_map(table, *value, fn); });
        if constexpr (namespace_placeable) {
            if (!record_namespace_write(table, true, grew)) remember_map_edit(table, key, *value);
        }
        return true;
    }

    template<class Fn>
    auto edit_value(Fn& fn) {
        return [this, &fn](NamespaceTable& table, const K& key, FrozenRows* rows) {
            auto* previous = consume_read_position(table, key);
            auto result = previous ? std::pair<V*, bool>{previous, false} : table.emplace(key);
            capture_slot(table, key, *result.first, result.second, rows);
            try {
                const bool grew = edit_grows_map(table, *result.first, fn);
                return WriteResult{result.first, result.second, grew};
            } catch (...) {
                if (result.second) {
                    if (rows) rows->erase(key);
                    table.erase_at_value(*result.first);
                }
                if (table.spilled) demote_namespace(table, true);
                throw;
            }
        };
    }

    template<class Input>
    V* put_flat(int key_group, const K& key, Input&& value) {
        return mutate_flat(key_group, key, [&](auto& table, const K& stored_key, FrozenRows* rows) {
            return assign_value(table, stored_key, std::forward<Input>(value), rows);
        });
    }

    template<class Operation>
    V* mutate_flat(int key_group, const K& key, Operation&& operation) {
        if constexpr (std::is_same_v<K, NamespaceBlob>) {
            if (serialized_namespace_) {
                auto [ns, inner] = split_serialized_key(key);
                return mutate_namespace(key_group, ns, inner, std::forward<Operation>(operation));
            }
        }
        const int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto& table = *tables_[idx];
        if constexpr (namespace_placeable) {
            if (table.first_root_write) place_empty_root(table);
        }
        auto [pointer, inserted, inner_growth] = with_namespace_space(table, [&] {
            return operation(table, key, cs.active ? &cs.cow_entries : nullptr);
        });
        if constexpr (namespace_placeable) {
            if (record_namespace_write(table, !inserted, inner_growth)) pointer = table.find(key);
            if constexpr (numeric_map) {
                if (pointer) remember_map_edit(table, key, *pointer, !inserted);
            }
        }
        return pointer;
    }

    template <typename N, typename VV>
    V* put_namespace(int key_group, const N& ns, const K& key, VV&& value) {
        return mutate_namespace(key_group, ns, key, [&](auto& table, const K& stored_key, FrozenRows* rows) {
            return assign_value(table, stored_key, std::forward<VV>(value), rows);
        });
    }

    template<class N, class Operation>
    V* mutate_namespace(int key_group, const N& ns, const K& key, Operation&& operation) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        auto* table = get_or_create_namespace_table(key_group, ns);
        auto [pointer, inserted, inner_growth] = [&] {
            try {
                auto* rows = cs.active ? &get_or_create_ns_cow(idx, ns).cow_entries : nullptr;
                return with_namespace_space(*table, [&] { return operation(*table, key, rows); });
            } catch (...) {
                if (table->empty()) remove_namespace_table(key_group, ns);
                throw;
            }
        }();
        if constexpr (namespace_placeable) {
            if (inserted && !windows_.empty()) {
                table->peak_window_rows = std::max(table->peak_window_rows, table->size());
                auto& window = windows_[idx];
                if (table->size() == 2) window.singleton_lifetimes = 0;
                if (table->size() == 4) window.small_openings = 0;
                auto& hint = window.capacity;
                if (table->resident && table->capacity() > hint && (hint != 1 || table->size() > 1)) {
                    hint = std::max(hint, std::min(initial_table_capacity_, table->capacity()));
                }
            }
            if (record_namespace_write(*table, !inserted, inner_growth)) pointer = table->find(key);
            // A three-row window can expire before two positive reads warm it.
            // Successive short lifetimes establish reuse before the next window opens.
            if (inserted && table->size() == 1 && !windows_.empty() && windows_[idx].small_lifetimes >= 4) {
                publish_window_controls(table, ns, pointer, true);
            }
        }
        return pointer;
    }

    // The directory contains only namespace identities and stable table
    // headers. Each key group has its own slice in the state's arena.
    template<class N>
    NamespaceLookup* directory_slot(int group, const N& ns) {
        if constexpr (std::is_same_v<N, int64_t> || std::is_same_v<N, TimeWindow>) {
            if (namespace_lookup_.empty()) return nullptr;
            auto identity = describe_namespace(ns, nullptr);
            uint64_t hash = ResidentControls::window_hash(identity.first, identity.second);
            if (window_directory_) return &(*window_directory_)[group * directory_width_ + (hash >> directory_shift_)];
            static_assert(std::tuple_size<NamespaceHints>::value == 4);
            return &namespace_lookup_[group][hash >> 62];
        }
        return nullptr;
    }

    template<class N>
    static NamespaceLookup describe_namespace(const N& ns, NamespaceTable* table) {
        if constexpr (std::is_same_v<N, int64_t>) return {table, NamespaceLookup::Kind::Long, ns, 0};
        else if constexpr (std::is_same_v<N, TimeWindow>) return {table, NamespaceLookup::Kind::Window, ns.first, ns.second};
        else return {};
    }

    void consider_window_directory(size_t live_namespaces) {
        if (window_directory_ || !namespace_arena_ || live_namespaces < 8) return;
        if (directory_retry_) { --directory_retry_; return; }
        if (++directory_samples_ < 8) return;
        directory_samples_ = 0;
        directory_retry_ = 1023;
        const size_t max_per_group = namespace_arena_->primary_capacity()
                / (32 * sizeof(NamespaceLookup)) / size_t(num_key_groups_);
        if (max_per_group < 8) return;
        size_t width = 8;
        unsigned shift = 61;
        while (width < 512 && width <= max_per_group / 2) { width *= 2; --shift; }
        const size_t count = width * size_t(num_key_groups_);
        const size_t bytes = count * sizeof(NamespaceLookup);
        if (bytes > namespace_arena_->free_bytes() / 2) return;
        try {
            auto directory = std::make_unique<std::pmr::vector<NamespaceLookup>>(
                    count, NamespaceLookup{}, namespace_arena_.get());
            namespace_stats_.directory_bytes = directory->capacity() * sizeof(NamespaceLookup);
            // The compact hints are dormant until this directory yields its space.
            // A formerly missing namespace can acquire a live table during that time.
            std::fill(namespace_lookup_.begin(), namespace_lookup_.end(), NamespaceHints{});
            window_directory_ = std::move(directory);
            directory_width_ = width;
            directory_shift_ = shift;
            ++namespace_stats_.directories_built;
        } catch (const std::bad_alloc&) {
            // The ordinary directory continues to own every table.
        }
    }

    template <typename N>
    NamespaceTable* find_namespace_table(int key_group, const N& ns, bool read = false) {
        const int idx = key_group - start_key_group_;
        if (auto* slot = directory_slot(idx, ns)) {
            auto expected = describe_namespace(ns, nullptr);
            if (slot->kind == expected.kind && slot->first == expected.first
                    && slot->second == expected.second) return slot->table;
        }
        auto& namespaces = get_ns_maps_ref<N>()[idx];
        auto found = namespaces.find(ns);
        auto* table = found == namespaces.end() ? nullptr : found->second.get();
        if constexpr (std::is_same_v<N, int64_t> || std::is_same_v<N, TimeWindow>) {
            if (read && table && !window_directory_) consider_window_directory(namespaces.size());
            // Directory creation during this lookup can change the applicable slot.
            remember_namespace(idx, ns, table);
        }
        return table;
    }

    template <typename N>
    void remember_namespace(int idx, const N& ns, NamespaceTable* table) {
        if (auto* slot = directory_slot(idx, ns)) *slot = describe_namespace(ns, table);
    }

    template<class N>
    auto& retired_directory_nodes() {
        if constexpr (std::is_same_v<N, int64_t>) return retired_integer_node_;
        else if constexpr (std::is_same_v<N, TimeWindow>) return retired_window_node_;
        else return retired_string_node_;
    }

    template<class Directory, class N>
    void insert_namespace(Directory& directory, const N& ns, std::unique_ptr<NamespaceTable> table) {
        if (namespace_arena_) {
            auto& spare = retired_directory_nodes<N>();
            if (!spare.empty()) {
                // The local node releases its new table if changing a string key or rehashing fails.
                auto node = spare.take();
                node.key() = ns;
                node.mapped() = std::move(table);
                if (!directory.insert(std::move(node)).inserted) throw std::logic_error("namespace already present");
                ++namespace_stats_.directory_node_reuses;
                return;
            }
        }
        if (!directory.emplace(ns, std::move(table)).second) throw std::logic_error("namespace already present");
    }

    template <typename N>
    NamespaceTable* get_or_create_namespace_table(int key_group, const N& ns) {
        if (auto* found = find_namespace_table(key_group, ns)) return found;
        const int idx = key_group - start_key_group_;
        update_missing_namespace(idx, ns, false);
        auto& namespaces = get_ns_maps_ref<N>()[idx];
        if constexpr (namespace_placeable && alignof(typename Table::slot_type) <= 64) {
            if (namespace_arena_ && unproductive_growth_ < growth_limit) {
                // Reserve spare shared capacity before falling back to DRAM.
                // Inserting the empty table then requires no whole-table copy.
                if (!windows_.empty() && initial_table_capacity_ > kGroupWidth) {
                    auto& window = windows_[idx];
                    // Concurrent window lifetimes reveal their shape before the first expiry.
                    if (window.live_namespaces && window.matching_lifetimes < 3 && window.small_openings < 4
                            && ++window.small_openings == 4 && window.capacity > 4) {
                        window.capacity = 4;
                        ++namespace_stats_.overlap_compactions;
                    }
                }
                const size_t capacity = windows_.empty() ? initial_table_capacity_ : windows_[idx].capacity;
                const size_t bytes = Table::initial_backing_bytes(capacity, capacity < kGroupWidth);
                const size_t alignment = Table::initial_backing_alignment(capacity, capacity < kGroupWidth);
                auto publish = [&](std::unique_ptr<NamespaceTable> table) {
                    table->group_index = idx;
                    auto* pointer = table.get();
                    insert_namespace(namespaces, ns, std::move(table));
                    if (!windows_.empty()) { ++windows_[idx].live_namespaces; ++live_window_count_; }
                    add_resident(*pointer);
                    if (capacity == 1) ++namespace_stats_.singleton_windows;
                    ++namespace_stats_.promotions;
                    remember_namespace(idx, ns, pointer);
                    return pointer;
                };
                try {
                    if (!windows_.empty()) {
                        auto& window = windows_[idx];
                        auto* spare = &window.empty_table;
                        if (*spare && (*spare)->packed_backing_bytes() != bytes) release_idle_window(*spare);
                        if (!*spare && shared_empty_window_ && shared_empty_window_->packed_backing_bytes() == bytes)
                            spare = &shared_empty_window_;
                        const size_t bucket = window_reuse_bucket(capacity);
                        if (!*spare && window_batches_ && bucket < window_batches_->size()) {
                            auto& batch = (*window_batches_)[bucket];
                            if (batch.count && batch.tables[batch.count - 1]->packed_backing_bytes() == bytes) {
                                spare = &batch.tables[--batch.count];
                                ++namespace_stats_.batched_window_reuses;
                            } else if (capacity >= 8 && capacity <= 32) {
                                // A recently emptied table can absorb the next larger window
                                // without a grow and rehash. Compact windows keep their own sizes.
                                auto& larger = (*window_batches_)[bucket + 1];
                                if (larger.count) {
                                    spare = &larger.tables[--larger.count];
                                    ++namespace_stats_.batched_window_reuses;
                                }
                            }
                        }
                        if (*spare) {
                            namespace_stats_.idle_window_bytes -= NamespaceCacheArena::allocation_bytes(
                                    (*spare)->packed_backing_bytes());
                            auto* published = publish(std::move(*spare));
                            ++namespace_stats_.reused_windows;
                            if (spare == &shared_empty_window_) ++namespace_stats_.shared_window_reuses;
                            return published;
                        }
                    }
                    while (true) {
                        NamespaceCacheArena::Packing reserved(*namespace_arena_, bytes, alignment);
                        if (!reserved.active()) {
                            if (release_memberships_for(bytes, bytes, alignment)) continue;
                            if (borrow_cache_space(bytes, bytes, alignment)) continue;
                            break;
                        }
                        auto table = std::make_unique<NamespaceTable>(capacity, alloc_,
                            max_table_capacity_, max_load_factor_, namespace_arena_.get());
                        return publish(std::move(table));
                    }
                } catch (const std::bad_alloc&) {
                    namespace_arena_->release_empty_loans();
                }
            }
        }
        auto table = std::make_unique<NamespaceTable>(initial_table_capacity_, alloc_,
            max_table_capacity_, max_load_factor_);
        table->group_index = idx;
        auto* pointer = table.get();
        insert_namespace(namespaces, ns, std::move(table));
        if (!windows_.empty()) { ++windows_[idx].live_namespaces; ++live_window_count_; }
        remember_namespace(idx, ns, pointer);
        return pointer;
    }

    template <typename N>
    void remove_namespace_table(int key_group, const N& ns) {
        const int idx = key_group - start_key_group_;
        auto& namespaces = get_ns_maps_ref<N>()[idx];
        auto found = namespaces.find(ns);
        if (found == namespaces.end()) return;
        found->second->withdraw_controls();
        auto* table = found->second.get();
        if constexpr (namespace_placeable) {
            if (!windows_.empty()) {
                auto& window = windows_[idx];
                if (table->peak_window_rows == 1) {
                    if (window.singleton_lifetimes < 8) ++window.singleton_lifetimes;
                    if (window.singleton_lifetimes == 8 && table->resident) window.capacity = 1;
                } else if (table->peak_window_rows > 1) {
                    window.singleton_lifetimes = 0;
                }
                auto& streak = window.small_lifetimes;
                if (table->peak_window_rows > 3) {
                    streak = 0;
                    observe_window_capacity(window, *table);
                } else if (table->peak_window_rows > 0) {
                    window.matching_lifetimes = 0;
                    if (streak < 4) ++streak;
                    // A read-active run of tiny windows can use a single compact group.
                    // Write-only turnover can reuse its already-sized empty backing.
                    if (streak == 4 && window.capacity > 4 && (table->resident_reads
                            || initial_table_capacity_ > kGroupWidth || window.last_fitting_capacity)) {
                        size_t fitting = 4;
                        while (fitting < window.capacity && fitting * max_load_factor_ < 3) fitting *= 2;
                        window.capacity = fitting;
                    }
                }
            }
            const bool was_resident = table->resident;
            if (was_resident) {
                namespace_arena_->forget_payload_owner(&table->spilled);
                remove_resident(*table);
            }
            if (auto* slot = directory_slot(idx, ns); slot && slot->table == table) *slot = {};
            if (!windows_.empty()) {
                auto& window = windows_[idx];
                // The checkpoint finds deleted rows by their original namespace and key.
                // Empty backing can be reused even while that checkpoint is active.
                const bool reusable = was_resident && (table->empty() || !cow_states_[idx].active);
                --live_window_count_;
                if (!--window.live_namespaces) {
                    window.small_openings = 0;
                    if (window.empty_table) release_idle_window(window.empty_table);
                    if (!live_window_count_) {
                        // No active window can reuse a burst of empty backings. The
                        // existing idle-successor rule can retain one proven size.
                        if (window_batches_) {
                            for (auto& batch : *window_batches_) {
                                while (batch.count) release_idle_window(batch.tables[--batch.count]);
                            }
                        }
                        if (shared_empty_window_) release_idle_window(shared_empty_window_);
                        const bool short_window = table->peak_window_rows && table->peak_window_rows <= 3;
                        if (short_window) {
                            if (consecutive_small_idle_ < 4) ++consecutive_small_idle_;
                        } else consecutive_small_idle_ = 0;
                        if (consecutive_small_idle_ == 4 && reusable
                                && window.small_lifetimes >= 4 && table->capacity() <= 16
                                && table->packed_backing_bytes() <= namespace_arena_->primary_capacity() / 128) {
                            retain_empty_window(window, shared_empty_window_, found->second);
                            if (shared_empty_window_) ++namespace_stats_.idle_successors;
                        }
                        if (read_position_) { release_read_position(); position_retry_ = 31; }
                    } else if (reusable && window.matching_lifetimes == 3) {
                        retain_empty_window(window, shared_empty_window_, found->second);
                    }
                } else if (reusable) {
                    retain_empty_window(window, window.empty_table, found->second);
                }
                if (found->second && reusable && live_window_count_
                        && (has_reusable_medium_backing(*table)
                            || window.matching_lifetimes == 3 || window.small_lifetimes >= 4)) {
                    const size_t bucket = window_reuse_bucket(table->capacity());
                    if (bucket < std::tuple_size<WindowBatches>::value) {
                        if (!window_batches_) window_batches_.reset(new (std::nothrow) WindowBatches{});
                        if (window_batches_) {
                            auto& batch = (*window_batches_)[bucket];
                            if (batch.count < batch.tables.size()
                                    && retain_empty_window(window, batch.tables[batch.count], found->second, true)) {
                                ++batch.count;
                            }
                        }
                    }
                }
            }
        }
        if (namespace_arena_) {
            auto& spare = retired_directory_nodes<N>();
            bool bounded = true;
            if constexpr (std::is_same_v<N, std::string>) bounded = found->first.capacity() <= 128;
            if (!bounded || !spare.retain(namespaces, found)) namespaces.erase(found);
            namespace_arena_->release_empty_loans();
        } else namespaces.erase(found);
    }

    void observe_window_capacity(WindowReuse& window, const NamespaceTable& table) {
        if (!table.resident || table.spilled) { window.matching_lifetimes = 0; return; }
        size_t required = table.capacity();
        while (required > 8 && table.peak_window_rows < required / 2
                && table.peak_window_rows <= static_cast<size_t>((required / 2) * max_load_factor_)) {
            required /= 2;
        }
        if (Table::initial_backing_bytes(required, required < kGroupWidth) > namespace_arena_->primary_capacity() / 32) {
            window.matching_lifetimes = 0;
            return;
        }
        if (window.last_fitting_capacity != required) {
            window.last_fitting_capacity = required;
            window.matching_lifetimes = 0;
        }
        if (window.matching_lifetimes < 3 && ++window.matching_lifetimes == 3) {
            window.capacity = required;
            ++namespace_stats_.learned_window_sizes;
        }
    }

    void release_idle_window(std::unique_ptr<NamespaceTable>& spare) {
        namespace_stats_.idle_window_bytes -= NamespaceCacheArena::allocation_bytes(
                spare->packed_backing_bytes());
        spare.reset();
        ++namespace_stats_.released_idle_windows;
    }

    bool retain_empty_window(const WindowReuse& window, std::unique_ptr<NamespaceTable>& spare,
                             std::unique_ptr<NamespaceTable>& table, bool use_capacity_batch = false) {
        const bool medium = use_capacity_batch && has_reusable_medium_backing(*table);
        const bool small = window.small_lifetimes >= 4 && table->size() <= 3 && table->capacity() <= 64;
        const bool recurring = window.matching_lifetimes == 3 && table->empty();
        if ((!small && !recurring && !medium) || spare || !table->peak_window_rows || table->spilled
                || !namespace_arena_->contains_primary(table->ctrl_data())) return false;
        const auto backing = table->packed_backing_bytes();
        const auto charge = NamespaceCacheArena::allocation_bytes(backing);
        if ((!medium && backing != Table::initial_backing_bytes(window.capacity, window.capacity < kGroupWidth))
                || charge + namespace_stats_.idle_window_bytes > namespace_arena_->primary_capacity() / 16
                || namespace_arena_->free_bytes() < namespace_arena_->primary_capacity() / 4) return false;

        // Merged source tables can still own moved-from rows. End their lifetime before
        // returning this empty backing to the same group for a new namespace.
        table->clear();
        static_cast<NamespaceUsage&>(*table) = {};
        spare = std::move(table);
        namespace_stats_.idle_window_bytes += charge;
        return true;
    }

    // Stable headers link resident tables in admission order. References
    // update one timestamp without writing into neighboring table headers.
    void prepend_resident(NamespaceTable& table) noexcept {
        table.newer = nullptr;
        table.older = newest_resident_;
        if (newest_resident_) newest_resident_->newer = &table;
        else oldest_resident_ = &table;
        newest_resident_ = &table;
    }

    void unlink_resident(NamespaceTable& table) noexcept {
        if (table.older) table.older->newer = table.newer;
        else oldest_resident_ = table.newer;
        if (table.newer) table.newer->older = table.older;
        else newest_resident_ = table.older;
    }

    void add_resident(NamespaceTable& table) noexcept {
        prepend_resident(table);
        if (!replacement_hand_) replacement_hand_ = &table;
        reference_resident(table);
        if (table.partition && !resident_controls_.empty()) {
            table.negative_at_promotion = resident_controls_[table.group_index].count();
        }
        table.resident = true;
        ++resident_count_;
    }

    void remove_resident(NamespaceTable& table) noexcept {
        if (replacement_hand_ == &table) {
            replacement_hand_ = resident_count_ == 1 ? nullptr : following_resident(table);
        }
        if (last_referenced_ == &table) last_referenced_ = nullptr;
        unlink_resident(table);
        table.newer = table.older = nullptr;
        table.resident = false;
        --resident_count_;
    }

    NamespaceTable* following_resident(const NamespaceTable& table) const noexcept {
        return table.newer ? table.newer : oldest_resident_;
    }

    void reference_resident(NamespaceTable& table) noexcept {
        if (last_referenced_ == &table) return;
        table.last_reference = ++reference_clock_;
        last_referenced_ = &table;
    }

    void warm_resident(NamespaceTable& table) {
        if (table.resident_reads < reads_to_warm && ++table.resident_reads == reads_to_warm) {
            table.admission_failures = 0;
            if (unproductive_growth_ > 0) --unproductive_growth_;
        }
    }

    static void observe_namespace_reuse(NamespaceTable& table) {
        if (table.reuse < std::numeric_limits<uint32_t>::max()) ++table.reuse;
    }

    void place_empty_root(NamespaceTable& table) {
        table.first_root_write = false;
        const size_t bytes = table.packed_backing_bytes();
        if (!table.empty() || table.resident || bytes > initial_root_bytes_left_ || namespace_arena_->in_mutation()
                || namespace_arena_->free_bytes() < namespace_arena_->primary_capacity() / 4) return;
        // A state that opens its groups with misses may be building a large
        // working set. Seed a few groups and let the others earn placement
        // through recent existing-key accesses.
        if (table.started_with_miss && initial_miss_root_tables_ == 2) {
            ++namespace_stats_.initial_miss_root_deferrals;
            return;
        }
        try {
            NamespaceCacheArena::Packing space(*namespace_arena_, bytes, table.packed_backing_alignment());
            if (!space.active()) return;
            table.relocate(namespace_arena_.get());
        } catch (const std::bad_alloc&) {
            return;
        }
        add_resident(table);
        table.transfer_bytes = bytes;
        initial_root_bytes_left_ -= bytes;
        if (table.started_with_miss) ++initial_miss_root_tables_;
        ++namespace_stats_.initial_root_tables;
    }

    void return_empty_root_loan(NamespaceTable& table) {
        if (!table.resident) return;
        namespace_arena_->forget_payload_owner(&table.spilled);
        if (!namespace_arena_->has_table_loan()) return;
        try {
            table.reset_empty_on_dram(initial_table_capacity_, max_table_capacity_, max_load_factor_);
        } catch (const std::bad_alloc&) {
            return;
        }
        remove_resident(table);
        table.writes_before_admission = 1;
        table.reuse = table.partition_reads = table.partition_updates = 0;
        table.spent_dram_copy_credit = 0;
        table.reuse_at_probe = table.rows_at_probe = table.transfer_bytes = 0;
        table.dram_reads = table.resident_reads = table.admission_failures = 0;
        table.ordinary_reads_left = table.sampled_reads = table.sampled_empty = 0;
        table.request_membership = table.backing_exceeds_quota = false;
        table.was_evicted = false;
        namespace_arena_->release_empty_loans();
        ++namespace_stats_.empty_root_returns;
    }

    void demote_namespace(NamespaceTable& table, bool growth) {
        // The task thread owns residency changes. Snapshot readers lock
        // the victim's key group while using its controls and slots.
        std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[table.group_index]);
        NamespaceCacheArena::ReleaseBatch release(*namespace_arena_);
        namespace_arena_->forget_payload_owner(&table.spilled);
        table.return_to_dram();
        remove_resident(table);
        table.spilled = false;
        table.transfer_bytes = growth && table.partition ? table.dram_bytes() : 0;
        table.writes_before_admission = 63;
        table.reuse = 0;
        if (growth) {
            ++namespace_stats_.growth_demotions;
            if (table.partition && table.resident_reads < reads_to_warm) defer_partition(table);
            namespace_arena_->release_empty_loans();
            if (table.resident_reads < reads_to_warm && unproductive_growth_ < growth_limit) {
                ++unproductive_growth_;
            }
        } else {
            ++namespace_stats_.evictions;
            table.was_evicted = true;
        }
        table.dram_reads = 0;
        table.resident_reads = 0;
        table.partition_reads = table.partition_updates = 0;
        table.spent_dram_copy_credit = 0;
        table.reuse_at_probe = 0;
        table.rows_at_probe = table.size();
        namespace_arena_->backing()->record_serialized_invalidation();
    }

    template <typename Operation>
    decltype(auto) with_namespace_space(NamespaceTable& table, Operation&& operation) {
        if constexpr (namespace_placeable) {
            if (table.resident) {
                NamespaceCacheArena::Mutation mutation(*namespace_arena_, table.spilled, &table);
                return operation();
            }
        }
        return operation();
    }

    template<class T>
    static size_t allocation_bytes(const T& value) {
        if constexpr (is_serialized_map_key<T>) {
            return value.size() <= T::inline_bytes ? 0 : NamespaceCacheArena::allocation_bytes(value.size());
        } else if constexpr (is_fixed_window_row<T>) {
            size_t allocation = value.external_bytes();
            return allocation ? NamespaceCacheArena::allocation_bytes(allocation) : 0;
        } else if constexpr (std::is_same_v<T, NamespaceRow>) {
            return value.size() <= NamespaceRow::inline_bytes ? 0
                    : NamespaceCacheArena::allocation_bytes(value.size());
        } else if constexpr (std::is_same_v<T, NamespaceBlob>) {
            const auto object = reinterpret_cast<uintptr_t>(&value);
            const auto data = reinterpret_cast<uintptr_t>(value.data());
            return (data < object || data >= object + sizeof(value))
                    ? NamespaceCacheArena::allocation_bytes(value.size() + 1) : 0;
        } else if constexpr (std::is_same_v<T, ElementList>) {
            return allocation_bytes(value.storage());
        } else if constexpr (std::uses_allocator_v<T, std::pmr::polymorphic_allocator<char>>) {
            size_t bytes = value.copy_storage_bytes();
            for (const auto& entry : value) {
                bytes += allocation_bytes(entry.first) + allocation_bytes(entry.second);
            }
            return bytes;
        } else {
            return 0;
        }
    }

    size_t namespace_allocation_bytes(const NamespaceTable& table, size_t limit) const {
        size_t bytes = NamespaceCacheArena::allocation_bytes(table.packed_backing_bytes());
        if (bytes > limit) return bytes;
        if constexpr (std::uses_allocator_v<K, std::pmr::polymorphic_allocator<char>>
                   || std::uses_allocator_v<V, std::pmr::polymorphic_allocator<char>>) {
            table.for_each([&](const K& key, const V& value) {
                bytes += allocation_bytes(key) + allocation_bytes(value);
            });
        }
        return std::min(bytes, table.dram_bytes());
    }

    enum class Placement { Moved, BackingUnavailable, PayloadUnavailable };
    struct PayloadRequest {
        size_t bytes = 0;
        size_t alignment = 0;
    };

    Placement relocate_with_reserved_backing(NamespaceTable& table, size_t bytes,
                                             const PayloadRequest& payload) {
        const size_t backing = table.packed_backing_bytes();
        const size_t alignment = table.packed_backing_alignment();
        constexpr bool variable = std::uses_allocator_v<K, std::pmr::polymorphic_allocator<char>>
                || std::uses_allocator_v<V, std::pmr::polymorphic_allocator<char>>;
        std::optional<NamespaceCacheArena::Packing> space;
        const size_t requested = variable && !payload.bytes ? std::max(backing, bytes) : backing;
        space.emplace(*namespace_arena_, requested, alignment);
        const bool packed = variable && !payload.bytes && space->active();
        if (!space->active() && requested != backing) {
            space.emplace(*namespace_arena_, backing, alignment);
        }
        if (!space->active()) {
            // Fragmented aggregate free bytes cannot hold a contiguous slot array.
            ++namespace_stats_.blocked_backings;
            return Placement::BackingUnavailable;
        }
        if (payload.bytes && !space->reserve_payload(payload.bytes, payload.alignment)) {
            ++namespace_stats_.blocked_payloads;
            return Placement::PayloadUnavailable;
        }
        table.relocate(namespace_arena_.get());
        if (packed) ++namespace_stats_.packed_promotions;
        if (space->used_payload()) ++namespace_stats_.reserved_payload_promotions;
        return Placement::Moved;
    }

    bool record_namespace_write(NamespaceTable& table, bool updated_existing_key, bool inner_growth = false) {
        if (!namespace_arena_) return false;
        const bool reused_entry = updated_existing_key && !inner_growth;
        if (inner_growth && updated_existing_key) ++namespace_stats_.growing_map_updates;
        if (table.partition && !table.resident && (updated_existing_key
                || (table.has_membership() && !resident_controls_.empty()))) consider_probe_index(table);
        if (table.request_membership && !table.resident) consider_membership(table);
        if (table.partition && updated_existing_key && !table.spilled) {
            if (table.resident) ++namespace_stats_.resident_root_updates;
            else ++namespace_stats_.dram_root_updates;
            if (reused_entry && !table.backing_exceeds_quota && table.partition_updates < UINT32_MAX) {
                ++table.partition_updates;
                if (table.resident) {
                    root_copy_budget_ = std::min(root_copy_limit_, root_copy_budget_ + 16);
                    if ((table.partition_updates & 3u) == 0) warm_resident(table);
                } else if ((table.partition_updates & 3u) == 0 && table.dram_reads < reads_to_warm) {
                    ++table.dram_reads;
                }
            }
        }
        if (table.backing_exceeds_quota) return false;
        auto* counters = namespace_arena_->backing();
        if (table.spilled) {
            demote_namespace(table, true);
            return true;
        }
        if (table.resident) {
            if (reused_entry) reference_resident(table);
            counters->record_serialized_write();
            return false;
        }
        if (reused_entry) observe_namespace_reuse(table);
        if (unproductive_growth_ == growth_limit && table.dram_reads < reads_to_warm) {
            return false;
        }
        // Admission happens after a mutation, never on a read that could
        // invalidate a still-borrowed payload. One-write namespaces settle
        // on DRAM, and failed attempts cool down before trying again.
        if (table.writes_before_admission != 0) {
            --table.writes_before_admission;
            return false;
        }
        table.writes_before_admission = 63;
        // The backing only grows. Variable payload size may later shrink,
        // but a slot array larger than this state's quota can never move.
        if (table.packed_backing_bytes() > namespace_arena_->maximum_table_capacity()) {
            table.backing_exceeds_quota = true;
            ++namespace_stats_.rejected;
            counters->record_serialized_bypass();
            return false;
        }
        if (table.partition) {
            const size_t rows = table.size();
            const size_t growth = rows > table.rows_at_probe ? rows - table.rows_at_probe : 0;
            const uint64_t references = table.partition_reuse();
            const uint64_t reused = references - table.reuse_at_probe;
            table.reuse_at_probe = references;
            table.rows_at_probe = rows;
            // Charge each new key against reuse. Repeated in-place updates
            // also avoid a trip to DRAM, even without a preceding get.
            // An inner MapState can grow while the outer row count stays flat.
            // Count reuse since the spill against the bytes moved in both directions.
            // Shrinking a payload reduces the next transfer cost immediately.
            if (table.transfer_bytes) {
                const size_t footprint = table.dram_bytes();
                const bool growing = footprint > table.transfer_bytes;
                table.transfer_bytes = footprint;
                if (growing && references * 32 < footprint) {
                    table.writes_before_admission = 63;
                    ++namespace_stats_.growth_reentry_deferrals;
                    ++namespace_stats_.admission_deferrals;
                    return false;
                }
            }
            const bool single_updated = rows == 1 && reused_entry;
            if (!single_updated && (reused / 2 < growth || (!reused && !reused_entry))) {
                table.writes_before_admission = 127;
                ++namespace_stats_.admission_deferrals;
                return false;
            }
        }
        // VoidNamespace tables live for the whole job. A write-heavy shard
        // needs prior reuse before it can displace another live shard.
        const bool observed_reuse = table.reuse >= 8 + table.size() / 4;
        const bool can_replace = observed_reuse && (!table.partition
                || table.partition_reuse() / 2 >= table.size());
        // Two useful references per row qualify a table for unused shared space.
        // Unseen or rapidly growing tables keep their ordinary admission limit.
        const bool sustained = can_replace && table.reuse / 2 >= table.size();
        const size_t single_table_limit = namespace_arena_->maximum_table_capacity();
        const size_t limit = sustained ? single_table_limit : std::min(single_table_limit,
                std::max(namespace_arena_->capacity(), namespace_arena_->maximum_capacity()));
        const size_t bytes = table.dram_bytes() <= limit
                ? table.dram_bytes() : namespace_allocation_bytes(table, limit);
        if (bytes > limit) {
            table.writes_before_admission = 1023;
        }
        // Spare space can hold a new window. Replacing live tables also pays
        // for returning their contents to DRAM, so require observed reuse.
        if (bytes <= limit) {
            // The hint is only valid for this mutation. A later write can
            // shrink, replace or remove the payload that needed a larger gap.
            PayloadRequest payload;
            bool displaced_resident = false;
            try {
                while (true) {
                    bool fragmented = false;
                    if (bytes <= namespace_arena_->free_bytes()) {
                        // Successful relocation also returns this table's optional summary.
                        const size_t index_bytes = table.membership_bytes();
                        const size_t skipped_writes = table.membership_write_skips();
                        const size_t prepared = table.prepared_writes();
                        const size_t before = namespace_arena_->free_bytes() + index_bytes;
                        try {
                            const Placement placement = relocate_with_reserved_backing(table, bytes, payload);
                            if (placement == Placement::Moved) {
                                if (displaced_resident && table.partition) spend_copy_credit(table, bytes);
                                add_resident(table);
                                namespace_stats_.membership_bytes -= index_bytes;
                                namespace_stats_.membership_write_skips += skipped_writes;
                                namespace_stats_.prepared_writes += prepared;
                                table.transfer_bytes = before - namespace_arena_->free_bytes();
                                if constexpr (numeric_map) release_root_edit_hints(table);
                                ++namespace_stats_.promotions;
                                counters->record_serialized_write();
                                return true;
                            }
                            fragmented = placement == Placement::PayloadUnavailable;
                        } catch (const NamespaceCacheFull& full) {
                            ++namespace_stats_.failed_relocations;
                            if (!payload.bytes && full.bytes > NamespaceCacheArena::largest_cached_block()
                                    && full.alignment <= 64) {
                                payload = {full.bytes, full.alignment};
                                continue;
                            }
                            fragmented = true;
                        }
                    }
                    if (release_memberships_for(bytes, table.packed_backing_bytes(),
                                                table.packed_backing_alignment(), fragmented)) continue;
                    if (table.reuse && complete_initial_share(bytes, table.packed_backing_bytes())) continue;
                    if (can_replace && borrow_cache_space(bytes, table.packed_backing_bytes(), table.packed_backing_alignment(), sustained)) continue;
                    if (bytes > namespace_arena_->capacity()) break;
                    if (!can_replace || !try_replace_namespace(table, bytes)) break;
                    displaced_resident = true;
                }
            } catch (const std::bad_alloc&) {
                // An optional move can fail due to fragmentation or the
                // native memory limit. The authoritative table remains valid.
            }
        }
        if (bytes <= namespace_arena_->capacity() && !can_replace && oldest_resident_) {
            ++namespace_stats_.admission_deferrals;
            table.writes_before_admission = 15;
        }
        if (table.partition) defer_partition(table);
        ++namespace_stats_.rejected;
        counters->record_serialized_bypass();
        return false;
    }

    static void defer_partition(NamespaceTable& table) {
        // Failed copies can otherwise recur on every short write interval
        // even when the partition and the arena have not changed enough.
        if (table.admission_failures < 7) ++table.admission_failures;
        const auto delay = static_cast<uint16_t>((64u << table.admission_failures) - 1);
        table.writes_before_admission = std::max(table.writes_before_admission, delay);
    }

    size_t replacement_copy_credit(const NamespaceTable& table) const {
        // Derive credit on admission from existing-key accesses since the last demotion.
        // New keys and failed lookups do not pay for moving either authoritative table.
        const uint64_t probation = std::max<uint64_t>(256, 2 * table.size());
        const uint64_t reuse = table.partition_reuse();
        const uint64_t earned = reuse > probation ? (reuse - probation) * 32 : 0;
        const uint64_t unspent = earned - std::min(earned, table.spent_dram_copy_credit);
        // Share resident-side credit for an initial displacement. A returning
        // root must earn its own credit while in DRAM to avoid repeated swaps
        // among roots with similar access rates.
        const size_t shared = table.was_evicted ? 0 : root_copy_budget_;
        return shared + std::min<uint64_t>(unspent, root_copy_limit_ - shared);
    }

    void spend_copy_credit(NamespaceTable& table, size_t bytes) {
        const size_t resident_credit = table.was_evicted ? 0 : std::min(bytes, root_copy_budget_);
        root_copy_budget_ -= resident_credit;
        table.spent_dram_copy_credit += bytes - resident_credit;
        namespace_stats_.dram_reuse_credit_spent += bytes - resident_credit;
        namespace_stats_.charged_root_copy_bytes += bytes;
    }

    bool try_replace_namespace(NamespaceTable& incoming, size_t incoming_bytes) {
        if (!replacement_hand_) return false;
        const size_t credit = incoming.partition ? replacement_copy_credit(incoming) : SIZE_MAX;
        if (incoming.partition && incoming_bytes >= credit) return false;
        const size_t available = incoming.partition ? credit - incoming_bytes : SIZE_MAX;
        NamespaceTable* victim = nullptr;
        size_t victim_bytes = 0;
        uint64_t oldest_age = 0;
        // Bound admission work even with thousands of small, live windows.
        // Advance between samples so every resident is eligible over time.
        auto* cursor = replacement_hand_;
        for (size_t i = 0; i < std::min(resident_count_, size_t{8}); ++i) {
            const uint64_t age = reference_clock_ - cursor->last_reference;
            if (!victim || age > oldest_age) {
                const size_t bytes = std::max(cursor->transfer_bytes, cursor->packed_backing_bytes());
                if (bytes <= available) {
                    victim = cursor;
                    victim_bytes = bytes;
                    oldest_age = age;
                }
            }
            cursor = following_resident(*cursor);
        }
        replacement_hand_ = cursor;
        if (!victim) return false;
        demote_namespace(*victim, false);
        // Reserve the incoming copy across successive victims. Charge it once, on promotion.
        if (incoming.partition) spend_copy_credit(incoming, victim_bytes);
        return true;
    }

    // Optional negative-lookup indexes yield space before whole namespace tables.
    // Snapshot readers only access the authoritative tables, never these indexes.
    bool release_memberships_for(size_t bytes, size_t backing, size_t alignment,
                                 bool fragmented_payload = false) {
        if (namespace_arena_->payload_run_slack()) {
            namespace_arena_->release_payload_slack();
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        const size_t reclaimable = probe_indexes_.bytes + namespace_stats_.membership_bytes + namespace_stats_.directory_bytes
                + namespace_stats_.idle_window_bytes + namespace_stats_.read_position_bytes
                + namespace_stats_.map_edit_bytes + namespace_stats_.root_edit_hint_bytes + namespace_stats_.snapshot_index_bytes;
        if (!reclaimable || bytes > namespace_arena_->free_bytes() + reclaimable) return false;
        if (map_edit_) {
            release_map_edit();
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        if constexpr (numeric_map) {
            if (namespace_stats_.root_edit_hint_bytes) {
                for (auto& table : tables_) {
                    if (!table->root_edit_hints) continue;
                    release_root_edit_hints(*table);
                    if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                            && namespace_arena_->has_backing_space(backing, alignment))) return true;
                }
            }
        }
        if (namespace_stats_.snapshot_index_bytes && snapshot_marks_.release_if_idle()) {
            namespace_stats_.snapshot_index_bytes = 0;
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        if (read_position_) {
            release_read_position();
            position_retry_ = 4095;
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        if (namespace_stats_.idle_window_bytes) {
            if (window_batches_) {
                for (auto& batch : *window_batches_) {
                    while (batch.count) {
                        release_idle_window(batch.tables[--batch.count]);
                        if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                                && namespace_arena_->has_backing_space(backing, alignment))) return true;
                    }
                }
            }
            if (shared_empty_window_) {
                release_idle_window(shared_empty_window_);
                if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                        && namespace_arena_->has_backing_space(backing, alignment))) return true;
            }
            for (auto& window : windows_) {
                if (!window.empty_table) continue;
                release_idle_window(window.empty_table);
                if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                        && namespace_arena_->has_backing_space(backing, alignment))) return true;
            }
        }
        if (window_directory_) {
            window_directory_.reset();
            namespace_stats_.directory_bytes = 0;
            ++namespace_stats_.directories_released;
            directory_retry_ = 4095;
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        if (probe_indexes_.bytes) {
            for (auto& root : tables_) {
                if (!root->has_probe_index()) continue;
                std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[root->group_index]);
                root->drop_probe_index();
                root->probe_index_delay = 4095;
                if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                        && namespace_arena_->has_backing_space(backing, alignment))) return true;
            }
        }
        for (auto& root : tables_) {
            if (!root->has_membership()) continue;
            release_root_membership(*root);
            if (fragmented_payload || (bytes <= namespace_arena_->free_bytes()
                    && namespace_arena_->has_backing_space(backing, alignment))) return true;
        }
        return false;
    }

    bool make_room_for_growth(size_t bytes, size_t alignment) {
        if (release_memberships_for(bytes, bytes, alignment)) return true;
        // A hot namespace can grow into a free process span without moving
        // itself or invalidating input borrowed from any neighboring table.
        const auto* owner = static_cast<const NamespaceTable*>(namespace_arena_->mutation_owner());
        if (!owner || !owner->resident || owner->spilled) return false;
        if (complete_initial_share(bytes, bytes)) return true;
        // An interleaved read can refer to a different key group immediately before
        // this allocation. Only the complete table being changed earns the loan.
        const bool warm = owner->resident_reads >= reads_to_warm;
        bool proven_misses = false;
        if (owner->partition && !resident_controls_.empty()) {
            const auto skipped = resident_controls_[owner->group_index].count() - owner->negative_at_promotion;
            proven_misses = skipped >= std::max<size_t>(64, owner->size() / 2);
        }
        if (!warm && !proven_misses) return false;
        if (!borrow_cache_space(bytes, bytes, alignment, bytes >= namespace_arena_->primary_capacity())) return false;
        if (owner != last_referenced_) ++namespace_stats_.interleaved_growth_loans;
        if (!warm) ++namespace_stats_.negative_growth_loans;
        ++namespace_stats_.growth_loans;
        return true;
    }

    bool complete_initial_share(size_t bytes, size_t backing) {
        if (!namespace_arena_->has_unclaimed_share()) return false;
        if (share_retry_) { --share_retry_; return false; }
        if (namespace_arena_->try_complete_share(bytes, backing)) {
            ++namespace_stats_.initial_share_expansions;
            ++namespace_stats_.loans_granted;
            return true;
        }
        share_retry_ = 31;
        return false;
    }

    bool borrow_cache_space(size_t bytes, size_t backing_bytes, size_t alignment = 64, bool sustained = false) {
        if (loan_retry_ > 0) {
            --loan_retry_;
            return false;
        }
        if (sustained && namespace_arena_->try_borrow_whole_table(bytes)) {
            ++namespace_stats_.whole_table_loans;
            ++namespace_stats_.loans_granted;
            return true;
        }
        if (namespace_arena_->try_borrow(bytes, backing_bytes, alignment)) {
            ++namespace_stats_.loans_granted;
            return true;
        }
        loan_retry_ = 31;
        return false;
    }

    int start_key_group_;
    int num_key_groups_;
    bool void_namespace_;
    Allocator* alloc_;
    size_t initial_table_capacity_;
    size_t max_table_capacity_;
    double max_load_factor_;

    // General Namespace mode: supports int64_t, std::string, and TimeWindow namespace types
    // OPT-4: Outer layer uses vector (indexed by key group offset) for O(1) lookup
    // Declared before the namespace maps so their tables and payloads are
    // destroyed before the arena returns its span to the process manager.
    std::unique_ptr<NamespaceCacheArena> namespace_arena_;
    size_t root_map_payload_bytes_ = 0;
    uint64_t next_root_map_payload_sample_ = 47;
    // Declared before any live or recycled table that can publish here.
    NamespaceControlResource control_resource_;
    std::pmr::vector<ResidentControls> resident_controls_{&control_resource_};
    mutable SnapshotSlotMarks snapshot_marks_;
    NamespaceTable* newest_resident_ = nullptr;
    NamespaceTable* oldest_resident_ = nullptr;
    NamespaceTable* replacement_hand_ = nullptr;
    NamespaceTable* last_referenced_ = nullptr;
    uint64_t reference_clock_ = 0;
    size_t resident_count_ = 0;
    std::vector<NamespaceHints> namespace_lookup_;
    std::unique_ptr<std::pmr::vector<NamespaceLookup>> window_directory_;
    // Most state accesses read and then update one namespace immediately.
    // Keep one checked cursor so alternating groups do not dirty two tables.
    ReadPosition* read_position_ = nullptr;
    MapEditPosition* map_edit_ = nullptr;
    uint16_t map_edit_retry_ = 31;
    uint8_t position_samples_ = 0;
    uint8_t position_matches_ = 0;
    uint16_t position_retry_ = 31;
    size_t directory_width_ = 0;
    unsigned directory_shift_ = 0;
    uint16_t directory_retry_ = 0;
    uint8_t directory_samples_ = 0;
    ProbeIndexUsage probe_indexes_;
    std::vector<WindowReuse> windows_;
    size_t live_window_count_ = 0;
    // All entries have relinquished their old namespace, controls and payload.
    // They share the same reclaimable byte budget as the per-group empty table.
    std::unique_ptr<WindowBatches> window_batches_;
    std::unique_ptr<NamespaceTable> shared_empty_window_;
    NamespaceCacheStats namespace_stats_;
    static constexpr uint8_t reads_to_warm = 2;
    static constexpr uint8_t growth_limit = 4;
    static constexpr uint64_t root_edit_retry_interval = 4096;
    uint8_t unproductive_growth_ = 0;
    size_t initial_root_bytes_left_ = 0;
    unsigned initial_miss_root_tables_ = 0;
    size_t root_copy_budget_ = 0;
    size_t root_copy_limit_ = 0;
    uint8_t loan_retry_ = 0;
    uint8_t share_retry_ = 0;
    std::unique_ptr<TypeLayout> serialized_namespace_;
    // The shared views outlive every membership that publishes to them.
    std::pmr::vector<RootMembershipView> root_memberships_{&control_resource_};
    // Payload resources outlive the root rows that own their allocations.
    std::vector<std::unique_ptr<RootMapPayloads>> root_map_payloads_;
    std::vector<std::unique_ptr<NamespaceTable>> tables_;
    std::vector<IntegerDirectory> int_namespace_maps_;
    std::vector<StringDirectory> str_namespace_maps_;
    std::vector<WindowDirectory> tw_namespace_maps_;
    RetiredDirectoryNodes<IntegerDirectory> retired_integer_node_;
    RetiredDirectoryNodes<StringDirectory> retired_string_node_;
    RetiredDirectoryNodes<WindowDirectory> retired_window_node_;
    uint8_t consecutive_small_idle_ = 0;

    // COW state per key group (for snapshot consistency)
    std::vector<COWState> cow_states_;
    // Per-key-group mutexes: protects cow_states_[i], tables_[i], and ns COW maps during active snapshot
    std::vector<std::unique_ptr<std::recursive_mutex>> cow_mutexes_;

    // Namespace-level COW tracking: idx → namespace → NsCowEntry
    // OPT-4: Outer layer uses vector for O(1) lookup
    std::vector<std::unordered_map<int64_t, NsCowEntry>> int_ns_cow_;
    std::vector<std::unordered_map<std::string, NsCowEntry>> str_ns_cow_;
    std::vector<std::unordered_map<TimeWindow, NsCowEntry, TimeWindowHash>> tw_ns_cow_;
};

// ============================================================================
//  StateEngine: top-level container for all states in a keyed backend
// ============================================================================

// Type-erased base for state tables
class StateTableHandle {
public:
    virtual ~StateTableHandle() = default;
    virtual size_t total_size() const = 0;
    virtual bool is_void_namespace() const = 0;
    virtual void prepare_snapshot() = 0;
    virtual void release_snapshot() = 0;
    virtual void release_snapshot_group(int group) = 0;
    virtual void set_serialized_namespace(std::unique_ptr<TypeLayout> ns) = 0;
    virtual const NamespaceCacheArena* namespace_arena() const = 0;
    virtual NamespaceCacheStats namespace_cache_stats() const = 0;
    virtual RootMembershipView* root_membership_views() = 0;
    virtual ResidentControls* resident_control_views() = 0;
    virtual void flush_shared_reads() = 0;
    virtual uint64_t pending_shared_reads() const = 0;
    virtual uint64_t pending_shared_hits() const = 0;
    virtual uint64_t pending_shared_writes() const = 0;
};

// Typed wrapper
template <typename K, typename V>
class TypedStateTableHandle : public StateTableHandle {
public:
    explicit TypedStateTableHandle(std::unique_ptr<StateTable<K, V>> table)
        : table_(std::move(table)) {}

    StateTable<K, V>* get() { return table_.get(); }
    const StateTable<K, V>* get() const { return table_.get(); }

    size_t total_size() const override { return table_->total_size(); }
    bool is_void_namespace() const override { return table_->is_void_namespace(); }
    void prepare_snapshot() override { table_->prepare_snapshot(); }
    void release_snapshot() override { table_->release_snapshot(); }
    void release_snapshot_group(int group) override { table_->release_snapshot_group(group); }
    void set_serialized_namespace(std::unique_ptr<TypeLayout> ns) override {
        table_->set_serialized_namespace(std::move(ns));
    }
    const NamespaceCacheArena* namespace_arena() const override { return table_->namespace_arena(); }
    RootMembershipView* root_membership_views() override { return table_->root_membership_views(); }
    ResidentControls* resident_control_views() override { return table_->resident_control_views(); }
    void flush_shared_reads() override { table_->flush_shared_reads(); }
    uint64_t pending_shared_reads() const override { return table_->pending_shared_reads(); }
    uint64_t pending_shared_hits() const override { return table_->pending_shared_hits(); }
    uint64_t pending_shared_writes() const override { return table_->pending_shared_writes(); }
    NamespaceCacheStats namespace_cache_stats() const override { return table_->namespace_cache_stats(); }

private:
    std::unique_ptr<StateTable<K, V>> table_;
};

class StateEngine {
public:
    StateEngine(int start_key_group, int num_key_groups, int total_key_groups,
                std::shared_ptr<HotCacheManager> hot_cache = nullptr,
                size_t initial_table_capacity = 16,
                size_t max_table_capacity = 0,
                double max_load_factor = 0.875,
                size_t native_memory_max_bytes = 0,
                size_t hot_cache_state_bytes = 12 * 1024)
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          total_key_groups_(total_key_groups),
          owned_allocator_(new CountingAllocator(native_memory_max_bytes)),
          alloc_(owned_allocator_.get()),
          initial_table_capacity_(initial_table_capacity),
          max_table_capacity_(max_table_capacity),
          max_load_factor_(max_load_factor),
          hot_cache_state_bytes_(hot_cache_state_bytes),
          snapshot_version_(0),
          hot_cache_manager_(std::move(hot_cache)) {}

    ~StateEngine() = default;

    // Register a new state. Returns handle ID.
    template <typename K, typename V>
    int64_t register_state(const std::string& state_name, bool void_namespace) {
        auto table = std::make_unique<StateTable<K, V>>(
            start_key_group_, num_key_groups_, void_namespace, alloc_, initial_table_capacity_,
            max_table_capacity_, max_load_factor_);
        table->enable_namespace_storage(hot_cache_manager_, hot_cache_state_bytes_);
        auto handle = std::make_unique<TypedStateTableHandle<K, V>>(std::move(table));
        int64_t id = next_handle_id_++;
        state_names_[id] = state_name;
        state_handles_[id] = std::move(handle);
        name_to_id_[state_name] = id;
        return id;
    }

    template <typename K, typename V>
    int64_t register_namespace_state(const std::string& name) {
        return register_state<K, V>(name, false);
    }

    // Get a typed state table by handle ID.
    template <typename K, typename V>
    StateTable<K, V>* get_state_table(int64_t handle_id) {
        auto it = state_handles_.find(handle_id);
        if (it == state_handles_.end()) return nullptr;
        auto* typed = static_cast<TypedStateTableHandle<K, V>*>(it->second.get());
        return typed->get();
    }

    // Get state handle by name
    int64_t get_state_id(const std::string& name) const {
        auto it = name_to_id_.find(name);
        return it != name_to_id_.end() ? it->second : -1;
    }

    // Total entries across all states
    size_t total_entries() const {
        size_t total = 0;
        for (auto& [id, handle] : state_handles_) {
            total += handle->total_size();
        }
        return total;
    }

    // Snapshot version management (for COW)
    uint64_t prepare_snapshot() {
        ++snapshot_version_;
        // Propagate COW preparation to all registered state tables
        for (auto& [id, handle] : state_handles_) {
            handle->prepare_snapshot();
        }
        return snapshot_version_;
    }

    void release_snapshot() {
        // Propagate COW release to all registered state tables
        for (auto& [id, handle] : state_handles_) {
            handle->release_snapshot();
        }
    }

    uint64_t snapshot_version() const {
        return snapshot_version_;
    }

    void release_snapshot_group(int group) {
        if (group < start_key_group_ || group - start_key_group_ >= num_key_groups_)
            throw std::out_of_range("Checkpoint key group outside backend range");
        for (auto& state : state_handles_) state.second->release_snapshot_group(group);
    }

    // Accessors
    int start_key_group() const { return start_key_group_; }
    int num_key_groups() const { return num_key_groups_; }
    int total_key_groups() const { return total_key_groups_; }
    Allocator* allocator() const { return alloc_; }
    HotCacheManager* hot_cache_manager() const { return hot_cache_manager_.get(); }
    CountingAllocator* counting_allocator() const { return owned_allocator_.get(); }
    size_t hot_cache_state_bytes() const { return hot_cache_state_bytes_; }

    const std::unordered_map<int64_t, std::unique_ptr<StateTableHandle>>& state_handles() const {
        return state_handles_;
    }

    const std::unordered_map<int64_t, std::string>& state_names() const {
        return state_names_;
    }

    // Registry for checkpoint: maps state_id → opaque StateHandle* (from JNI layer).
    // StateEngine takes ownership and frees all handles on destruction.
    using OwnedPtr = std::unique_ptr<void, void(*)(void*)>;

    void register_state_handle_ptr(int64_t state_id, void* handle_ptr,
                                   void(*deleter)(void*)) {
        owned_state_handles_.emplace_back(OwnedPtr(handle_ptr, deleter));
        state_handle_ptrs_[state_id] = handle_ptr;
    }

    // Returns map of state_id → StateHandle* (cast by caller).
    template <typename T>
    std::unordered_map<int64_t, T*> registered_state_handles() const {
        std::unordered_map<int64_t, T*> result;
        for (auto& [id, ptr] : state_handle_ptrs_) {
            result[id] = static_cast<T*>(ptr);
        }
        return result;
    }

private:
    int start_key_group_;
    int num_key_groups_;
    int total_key_groups_;
    std::unique_ptr<CountingAllocator> owned_allocator_;
    Allocator* alloc_;
    size_t initial_table_capacity_;
    size_t max_table_capacity_;
    double max_load_factor_;
    size_t hot_cache_state_bytes_;

    int64_t next_handle_id_ = 1;
    std::unordered_map<int64_t, std::unique_ptr<StateTableHandle>> state_handles_;
    std::unordered_map<int64_t, std::string> state_names_;
    std::unordered_map<std::string, int64_t> name_to_id_;
    std::unordered_map<int64_t, void*> state_handle_ptrs_;  // state_id → StateHandle* (non-owning)
    std::vector<OwnedPtr> owned_state_handles_;  // owns all StateHandle allocations

    uint64_t snapshot_version_;
    std::shared_ptr<HotCacheManager> hot_cache_manager_;
};

}  // namespace forl0
