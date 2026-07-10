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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>
#include <mutex>
#include <atomic>

namespace forl0 {

// Forward declaration
class StateTableBase;

// ============================================================================
//  StateTable: per-state-descriptor storage, templated on K, V
// ============================================================================

template <typename K, typename V>
class StateTable {
public:
    using Table = SwissTable<K, V>;

    StateTable(int start_key_group, int num_key_groups, bool void_namespace,
               Allocator* alloc = &DefaultAllocator::instance(),
               size_t initial_table_capacity = 16)
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          void_namespace_(void_namespace),
          alloc_(alloc),
          initial_table_capacity_(initial_table_capacity) {
        // Always allocate flat tables — void-namespace API uses them directly,
        // and they serve as fallback for non-void-namespace states when Java
        // code calls void-namespace JNI functions (e.g., window operators).
        tables_.resize(num_key_groups);
        for (int i = 0; i < num_key_groups; ++i) {
            tables_[i] = std::make_unique<Table>(initial_table_capacity_, alloc_);
        }
        // COW state per key group
        cow_states_.resize(num_key_groups);
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

    ~StateTable() = default;

    StateTable(const StateTable&) = delete;
    StateTable& operator=(const StateTable&) = delete;

    // ---- COW Snapshot support ----

    // OPT-5: Merged COW tracking — single map instead of overwritten + added_after.
    // cow_entries: optional<V> with value = overwritten (old value), nullopt = added_after.
    struct COWState {
        bool active = false;
        std::unordered_map<K, std::optional<V>> cow_entries;
        std::unordered_map<K, V> deleted;       // old values of deleted entries
    };

    // Per-namespace COW tracking (used by namespace put/remove)
    struct NsCowEntry {
        std::unordered_map<K, std::optional<V>> cow_entries;
        std::unordered_map<K, V> deleted;
        void clear() { cow_entries.clear(); deleted.clear(); }
    };

    void prepare_snapshot() {
        for (int i = 0; i < num_key_groups_; ++i) {
            std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[i]);
            cow_states_[i].active = true;
            cow_states_[i].cow_entries.clear();
            cow_states_[i].deleted.clear();
            int_ns_cow_[i].clear();
            str_ns_cow_[i].clear();
            tw_ns_cow_[i].clear();
        }
    }

    void release_snapshot() {
        for (int i = 0; i < num_key_groups_; ++i) {
            std::lock_guard<std::recursive_mutex> lock(*cow_mutexes_[i]);
            cow_states_[i].active = false;
            cow_states_[i].cow_entries.clear();
            cow_states_[i].deleted.clear();
            int_ns_cow_[i].clear();
            str_ns_cow_[i].clear();
            tw_ns_cow_[i].clear();
        }
    }

    // Check if a snapshot is active for a given key group.
    // When true, in-place modifications to values obtained via get() are unsafe
    // and callers must use copy-and-replace through put() instead.
    bool is_snapshot_active(int key_group) const {
        return cow_states_[key_group - start_key_group_].active;
    }

    // ---- VoidNamespace operations ----

    V* get(int key_group, const K& key) {
        return tables_[key_group - start_key_group_]->find(key);
    }

    // COW-safe in-place modification. Locks + cow_before_write internally,
    // then calls fn(*val). Returns true if key existed and was modified.
    // Total: 1 copy per unique key per snapshot (in cow_before_write), vs 2 with copy-and-put.
    template <typename ModifyFn>
    bool modify_in_place(int key_group, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) {
            lock.lock();
            cow_before_write(idx, key);
        }
        V* val = tables_[idx]->find(key);
        if (val) { fn(*val); return true; }
        return false;
    }

    // COW-safe in-place modification that may result in key removal.
    // After fn(*val), if val->empty(), removes the key (with proper COW erase tracking).
    template <typename ModifyFn>
    void modify_or_remove_in_place(int key_group, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) {
            lock.lock();
            cow_before_write(idx, key);
        }
        V* val = tables_[idx]->find(key);
        if (!val) return;
        fn(*val);
        if (val->empty()) {
            cow_before_erase(idx, key);
            tables_[idx]->erase(key);
        }
    }

    // Namespace variant: COW-safe in-place modification.
    template <typename N, typename ModifyFn>
    bool modify_in_place(int key_group, const N& ns, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) {
            lock.lock();
            cow_before_write_ns(idx, ns, key);
        }
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return false;
        V* val = tbl->find(key);
        if (val) { fn(*val); return true; }
        return false;
    }

    // Namespace variant: COW-safe in-place modification that may result in key removal.
    template <typename N, typename ModifyFn>
    void modify_or_remove_in_place(int key_group, const N& ns, const K& key, ModifyFn&& fn) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) {
            lock.lock();
            cow_before_write_ns(idx, ns, key);
        }
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return;
        V* val = tbl->find(key);
        if (!val) return;
        fn(*val);
        if (val->empty()) {
            cow_before_erase_ns(idx, ns, key);
            tbl->erase(key);
            if (tbl->empty()) remove_namespace_table(key_group, ns);
        }
    }

    V* put(int key_group, const K& key, const V& value) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_write(idx, key);
        auto [ptr, inserted] = tables_[idx]->insert_or_assign(key, value);
        if (inserted && cs.active) {
            cs.cow_entries.emplace(key, std::nullopt);
        }
        return ptr;
    }

    V* put(int key_group, const K& key, V&& value) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_write(idx, key);
        auto [ptr, inserted] = tables_[idx]->insert_or_assign(key, std::move(value));
        if (inserted && cs.active) {
            cs.cow_entries.emplace(key, std::nullopt);
        }
        return ptr;
    }

    bool remove(int key_group, const K& key) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_erase(idx, key);
        return tables_[idx]->erase(key);
    }

    // ---- General Namespace operations ----

    template <typename N>
    V* get(int key_group, const N& ns, const K& key) {
        auto* tbl = find_namespace_table(key_group, ns);
        return tbl ? tbl->find(key) : nullptr;
    }

    template <typename N>
    V* put(int key_group, const N& ns, const K& key, const V& value) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_write_ns(idx, ns, key);
        auto* tbl = get_or_create_namespace_table(key_group, ns);
        auto [ptr, inserted] = tbl->insert_or_assign(key, value);
        if (inserted && cs.active) {
            get_or_create_ns_cow(idx, ns).cow_entries.emplace(key, std::nullopt);
        }
        return ptr;
    }

    template <typename N>
    V* put(int key_group, const N& ns, const K& key, V&& value) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_write_ns(idx, ns, key);
        auto* tbl = get_or_create_namespace_table(key_group, ns);
        auto [ptr, inserted] = tbl->insert_or_assign(key, std::move(value));
        if (inserted && cs.active) {
            get_or_create_ns_cow(idx, ns).cow_entries.emplace(key, std::nullopt);
        }
        return ptr;
    }

    template <typename N>
    bool remove(int key_group, const N& ns, const K& key) {
        int idx = key_group - start_key_group_;
        auto& cs = cow_states_[idx];
        std::unique_lock<std::recursive_mutex> lock(*cow_mutexes_[idx], std::defer_lock);
        if (cs.active) lock.lock();
        cow_before_erase_ns(idx, ns, key);
        auto* tbl = find_namespace_table(key_group, ns);
        if (!tbl) return false;
        bool erased = tbl->erase(key);
        if (erased && tbl->empty()) {
            remove_namespace_table(key_group, ns);
        }
        return erased;
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

        // Iterate current table, applying COW overrides
        if (tables_[idx]) {
            tables_[idx]->for_each([&](const K& k, const V& v) {
                auto it = cs.cow_entries.find(k);
                if (it != cs.cow_entries.end()) {
                    if (it->second.has_value()) {
                        fn(k, *it->second);  // overwritten — use old value
                    }
                    // else: added_after — skip
                    return;
                }
                fn(k, v);
            });
        }

        // Include deleted entries (they existed at snapshot time)
        for (const auto& [k, v] : cs.deleted) {
            fn(k, v);
        }
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
            const NsCowEntry* cow = nullptr;
            auto cow_ns_it = ns_cow.find(ns);
            if (cow_ns_it != ns_cow.end()) {
                cow = &cow_ns_it->second;
            }
            tbl->for_each([&](const K& k, const V& v) {
                if (cow) {
                    auto it = cow->cow_entries.find(k);
                    if (it != cow->cow_entries.end()) {
                        if (it->second.has_value()) {
                            fn(ns, k, *it->second);  // overwritten
                        }
                        // else: added_after — skip
                        return;
                    }
                }
                fn(ns, k, v);
            });
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
        auto [ptr, _] = tables_[key_group - start_key_group_]->emplace(key, std::move(value));
        return ptr;
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
                // COW: save all entries from source namespace before removal
                auto& nc_src = get_or_create_ns_cow(idx, src_ns);
                src_tbl->for_each([&](const K& k, V& v) {
                    if (!nc_src.cow_entries.count(k) && !nc_src.deleted.count(k)) {
                        nc_src.deleted[k] = v;
                    }
                });
            }
            src_tbl->for_each([&](const K& k, V& v) {
                V* existing = target_tbl->find(k);
                if (existing) {
                    *existing = merge(*existing, std::move(v));
                } else {
                    target_tbl->insert_or_assign(k, std::move(v));
                    if (cs.active) {
                        get_or_create_ns_cow(idx, target).cow_entries.emplace(k, std::nullopt);
                    }
                }
            });
            remove_namespace_table(key_group, src_ns);
        }
    }

    // ---- State metadata ----

    bool is_void_namespace() const { return void_namespace_; }
    int start_key_group() const { return start_key_group_; }
    int num_key_groups() const { return num_key_groups_; }

    size_t total_size() const {
        size_t total = 0;
        if (void_namespace_) {
            for (auto& t : tables_) {
                if (t) total += t->size();
            }
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
    void cow_before_write(int idx, const K& key) {
        auto& cs = cow_states_[idx];
        if (!cs.active) return;
        if (cs.cow_entries.count(key)) return;  // already tracked
        V* existing = tables_[idx]->find(key);
        if (existing) {
            cs.cow_entries[key] = *existing;  // optional<V> with value
        }
    }

    void cow_before_erase(int idx, const K& key) {
        auto& cs = cow_states_[idx];
        if (!cs.active) return;
        auto it = cs.cow_entries.find(key);
        if (it != cs.cow_entries.end()) {
            if (it->second.has_value()) {
                // Was overwritten — move old value to deleted
                cs.deleted[key] = std::move(*it->second);
            }
            // else: was added_after — no old value to save
            cs.cow_entries.erase(it);
            return;
        }
        if (cs.deleted.count(key)) return;
        V* existing = tables_[idx]->find(key);
        if (existing) {
            cs.deleted[key] = *existing;  // save deleted entry
        }
    }

    // Namespace COW: save old value before writing to a namespace table
    template <typename N>
    void cow_before_write_ns(int idx, const N& ns, const K& key) {
        auto& cs = cow_states_[idx];
        if (!cs.active) return;
        auto& nc = get_or_create_ns_cow(idx, ns);
        if (nc.cow_entries.count(key)) return;
        int kg = idx + start_key_group_;
        auto* tbl = find_namespace_table(kg, ns);
        if (!tbl) return;
        V* existing = tbl->find(key);
        if (existing) {
            nc.cow_entries[key] = *existing;
        }
    }

    // Namespace COW: save old value before erasing from a namespace table
    template <typename N>
    void cow_before_erase_ns(int idx, const N& ns, const K& key) {
        auto& cs = cow_states_[idx];
        if (!cs.active) return;
        auto& nc = get_or_create_ns_cow(idx, ns);
        auto it = nc.cow_entries.find(key);
        if (it != nc.cow_entries.end()) {
            if (it->second.has_value()) {
                nc.deleted[key] = std::move(*it->second);
            }
            nc.cow_entries.erase(it);
            return;
        }
        if (nc.deleted.count(key)) return;
        int kg = idx + start_key_group_;
        auto* tbl = find_namespace_table(kg, ns);
        if (!tbl) return;
        V* existing = tbl->find(key);
        if (existing) {
            nc.deleted[key] = *existing;
        }
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

    // Namespace table lookup — supports int64_t, std::string, and TimeWindow namespaces
    template <typename N>
    Table* find_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            auto nit = int_namespace_maps_[idx].find(ns);
            return nit != int_namespace_maps_[idx].end() ? nit->second.get() : nullptr;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            auto nit = tw_namespace_maps_[idx].find(ns);
            return nit != tw_namespace_maps_[idx].end() ? nit->second.get() : nullptr;
        } else {
            auto nit = str_namespace_maps_[idx].find(ns);
            return nit != str_namespace_maps_[idx].end() ? nit->second.get() : nullptr;
        }
    }

    template <typename N>
    Table* get_or_create_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            auto& ns_map = int_namespace_maps_[idx];
            auto it = ns_map.find(ns);
            if (it != ns_map.end()) return it->second.get();
            auto tbl = std::make_unique<Table>(initial_table_capacity_, alloc_);
            auto* ptr = tbl.get();
            ns_map.emplace(ns, std::move(tbl));
            return ptr;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            auto& ns_map = tw_namespace_maps_[idx];
            auto it = ns_map.find(ns);
            if (it != ns_map.end()) return it->second.get();
            auto tbl = std::make_unique<Table>(initial_table_capacity_, alloc_);
            auto* ptr = tbl.get();
            ns_map.emplace(ns, std::move(tbl));
            return ptr;
        } else {
            auto& ns_map = str_namespace_maps_[idx];
            auto it = ns_map.find(ns);
            if (it != ns_map.end()) return it->second.get();
            auto tbl = std::make_unique<Table>(initial_table_capacity_, alloc_);
            auto* ptr = tbl.get();
            ns_map.emplace(ns, std::move(tbl));
            return ptr;
        }
    }

    template <typename N>
    void remove_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            int_namespace_maps_[idx].erase(ns);
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            tw_namespace_maps_[idx].erase(ns);
        } else {
            str_namespace_maps_[idx].erase(ns);
        }
    }

    int start_key_group_;
    int num_key_groups_;
    bool void_namespace_;
    Allocator* alloc_;
    size_t initial_table_capacity_;

    // VoidNamespace mode: one table per key group
    std::vector<std::unique_ptr<Table>> tables_;

    // General Namespace mode: supports int64_t, std::string, and TimeWindow namespace types
    // OPT-4: Outer layer uses vector (indexed by key group offset) for O(1) lookup
    std::vector<std::unordered_map<int64_t, std::unique_ptr<Table>>> int_namespace_maps_;
    std::vector<std::unordered_map<std::string, std::unique_ptr<Table>>> str_namespace_maps_;
    std::vector<std::unordered_map<TimeWindow, std::unique_ptr<Table>, TimeWindowHash>> tw_namespace_maps_;

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

private:
    std::unique_ptr<StateTable<K, V>> table_;
};

class StateEngine {
public:
    StateEngine(int start_key_group, int num_key_groups, int total_key_groups,
                Allocator* alloc = &DefaultAllocator::instance(),
                std::unique_ptr<HotCacheManager> hot_cache = nullptr,
                size_t initial_table_capacity = 16)
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          total_key_groups_(total_key_groups),
          alloc_(alloc),
          initial_table_capacity_(initial_table_capacity),
          snapshot_version_(0),
          hot_cache_manager_(std::move(hot_cache)) {}

    ~StateEngine() = default;

    // Register a new state. Returns handle ID.
    template <typename K, typename V>
    int64_t register_state(const std::string& state_name, bool void_namespace) {
        auto table = std::make_unique<StateTable<K, V>>(
            start_key_group_, num_key_groups_, void_namespace, alloc_, initial_table_capacity_);
        auto handle = std::make_unique<TypedStateTableHandle<K, V>>(std::move(table));
        int64_t id = next_handle_id_++;
        state_names_[id] = state_name;
        state_handles_[id] = std::move(handle);
        name_to_id_[state_name] = id;
        return id;
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

    // Accessors
    int start_key_group() const { return start_key_group_; }
    int num_key_groups() const { return num_key_groups_; }
    int total_key_groups() const { return total_key_groups_; }
    Allocator* allocator() const { return alloc_; }
    HotCacheManager* hot_cache_manager() const { return hot_cache_manager_.get(); }

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
    Allocator* alloc_;
    size_t initial_table_capacity_;

    int64_t next_handle_id_ = 1;
    std::unordered_map<int64_t, std::unique_ptr<StateTableHandle>> state_handles_;
    std::unordered_map<int64_t, std::string> state_names_;
    std::unordered_map<std::string, int64_t> name_to_id_;
    std::unordered_map<int64_t, void*> state_handle_ptrs_;  // state_id → StateHandle* (non-owning)
    std::vector<OwnedPtr> owned_state_handles_;  // owns all StateHandle allocations

    uint64_t snapshot_version_;
    std::unique_ptr<HotCacheManager> hot_cache_manager_;
};

}  // namespace forl0
