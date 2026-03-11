// StateEngine: Core state storage component.
// Replaces the Java ForL0StateStore hierarchy.
//
// Organization: KeyGroup → Namespace → SwissTable<K, V>
//   - VoidNamespace mode: tables[keyGroup] directly
//   - General Namespace mode: namespace_maps[keyGroup] → SwissTable per namespace
//
// Manages multiple named states (one StateTable per state descriptor).

#pragma once

#include "swiss_table.h"
#include "type_layout.h"
#include "allocator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
               Allocator* alloc = &DefaultAllocator::instance())
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          void_namespace_(void_namespace),
          alloc_(alloc) {
        // Always allocate flat tables — void-namespace API uses them directly,
        // and they serve as fallback for non-void-namespace states when Java
        // code calls void-namespace JNI functions (e.g., window operators).
        tables_.resize(num_key_groups);
        for (int i = 0; i < num_key_groups; ++i) {
            tables_[i] = std::make_unique<Table>(16, alloc_);
        }
        // COW state per key group
        cow_states_.resize(num_key_groups);
    }

    ~StateTable() = default;

    StateTable(const StateTable&) = delete;
    StateTable& operator=(const StateTable&) = delete;

    // ---- COW Snapshot support ----

    struct COWState {
        bool active = false;
        std::unordered_map<K, V> overwritten;  // old values of modified entries
        std::unordered_map<K, V> deleted;       // old values of deleted entries
        std::unordered_set<K> added_after;      // keys added after snapshot
    };

    void prepare_snapshot() {
        for (auto& cs : cow_states_) {
            cs.active = true;
            cs.overwritten.clear();
            cs.deleted.clear();
            cs.added_after.clear();
        }
    }

    void release_snapshot() {
        for (auto& cs : cow_states_) {
            cs.active = false;
            cs.overwritten.clear();
            cs.deleted.clear();
            cs.added_after.clear();
        }
    }

    // ---- VoidNamespace operations ----

    V* get(int key_group, const K& key) {
        return tables_[key_group - start_key_group_]->find(key);
    }

    V* put(int key_group, const K& key, const V& value) {
        int idx = key_group - start_key_group_;
        cow_before_write(idx, key);
        auto [ptr, inserted] = tables_[idx]->insert_or_assign(key, value);
        if (inserted && cow_states_[idx].active) {
            cow_states_[idx].added_after.insert(key);
        }
        return ptr;
    }

    V* put(int key_group, const K& key, V&& value) {
        int idx = key_group - start_key_group_;
        cow_before_write(idx, key);
        auto [ptr, inserted] = tables_[idx]->insert_or_assign(key, std::move(value));
        if (inserted && cow_states_[idx].active) {
            cow_states_[idx].added_after.insert(key);
        }
        return ptr;
    }

    bool remove(int key_group, const K& key) {
        int idx = key_group - start_key_group_;
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
        auto* tbl = get_or_create_namespace_table(key_group, ns);
        auto [ptr, _] = tbl->insert_or_assign(key, value);
        return ptr;
    }

    template <typename N>
    V* put(int key_group, const N& ns, const K& key, V&& value) {
        auto* tbl = get_or_create_namespace_table(key_group, ns);
        auto [ptr, _] = tbl->insert_or_assign(key, std::move(value));
        return ptr;
    }

    template <typename N>
    bool remove(int key_group, const N& ns, const K& key) {
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

        // Iterate current table, applying COW overrides
        if (tables_[idx]) {
            tables_[idx]->for_each([&](const K& k, const V& v) {
                // Skip entries added after snapshot
                if (cs.added_after.find(k) != cs.added_after.end()) {
                    return;
                }
                // Use old value if overwritten
                auto it = cs.overwritten.find(k);
                if (it != cs.overwritten.end()) {
                    fn(k, it->second);
                } else {
                    fn(k, v);
                }
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
        auto it = int_namespace_maps_.find(idx);
        if (it != int_namespace_maps_.end()) {
            for (auto& [ns, tbl] : it->second) {
                tbl->for_each([&](const K& k, const V& v) {
                    fn(ns, k, v);
                });
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
        auto* target_tbl = get_or_create_namespace_table(key_group, target);
        for (const auto& src_ns : sources) {
            auto* src_tbl = find_namespace_table(key_group, src_ns);
            if (!src_tbl) continue;
            src_tbl->for_each([&](const K& k, V& v) {
                V* existing = target_tbl->find(k);
                if (existing) {
                    *existing = merge(*existing, std::move(v));
                } else {
                    target_tbl->insert_or_assign(k, std::move(v));
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
        for (auto& [idx, ns_map] : int_namespace_maps_) {
            for (auto& [ns, tbl] : ns_map) {
                total += tbl->size();
            }
        }
        for (auto& [idx, ns_map] : str_namespace_maps_) {
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
        if (cs.overwritten.find(key) != cs.overwritten.end()) return;
        if (cs.added_after.find(key) != cs.added_after.end()) return;
        V* existing = tables_[idx]->find(key);
        if (existing) {
            cs.overwritten[key] = *existing;  // deep copy old value
        }
    }

    void cow_before_erase(int idx, const K& key) {
        auto& cs = cow_states_[idx];
        if (!cs.active) return;
        if (cs.overwritten.find(key) != cs.overwritten.end()) return;
        if (cs.deleted.find(key) != cs.deleted.end()) return;
        if (cs.added_after.find(key) != cs.added_after.end()) return;
        V* existing = tables_[idx]->find(key);
        if (existing) {
            cs.deleted[key] = *existing;  // save deleted entry
        }
    }

    // Namespace table lookup — supports both int64_t and std::string namespaces
    template <typename N>
    Table* find_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            auto it = int_namespace_maps_.find(idx);
            if (it == int_namespace_maps_.end()) return nullptr;
            auto nit = it->second.find(ns);
            return nit != it->second.end() ? nit->second.get() : nullptr;
        } else {
            auto it = str_namespace_maps_.find(idx);
            if (it == str_namespace_maps_.end()) return nullptr;
            auto nit = it->second.find(ns);
            return nit != it->second.end() ? nit->second.get() : nullptr;
        }
    }

    template <typename N>
    Table* get_or_create_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            auto& ns_map = int_namespace_maps_[idx];
            auto it = ns_map.find(ns);
            if (it != ns_map.end()) return it->second.get();
            auto tbl = std::make_unique<Table>(16, alloc_);
            auto* ptr = tbl.get();
            ns_map.emplace(ns, std::move(tbl));
            return ptr;
        } else {
            auto& ns_map = str_namespace_maps_[idx];
            auto it = ns_map.find(ns);
            if (it != ns_map.end()) return it->second.get();
            auto tbl = std::make_unique<Table>(16, alloc_);
            auto* ptr = tbl.get();
            ns_map.emplace(ns, std::move(tbl));
            return ptr;
        }
    }

    template <typename N>
    void remove_namespace_table(int key_group, const N& ns) {
        int idx = key_group - start_key_group_;
        if constexpr (std::is_same_v<N, int64_t>) {
            auto it = int_namespace_maps_.find(idx);
            if (it != int_namespace_maps_.end()) {
                it->second.erase(ns);
                if (it->second.empty()) int_namespace_maps_.erase(it);
            }
        } else {
            auto it = str_namespace_maps_.find(idx);
            if (it != str_namespace_maps_.end()) {
                it->second.erase(ns);
                if (it->second.empty()) str_namespace_maps_.erase(it);
            }
        }
    }

    int start_key_group_;
    int num_key_groups_;
    bool void_namespace_;
    Allocator* alloc_;

    // VoidNamespace mode: one table per key group
    std::vector<std::unique_ptr<Table>> tables_;

    // General Namespace mode: supports both int64_t and std::string namespace types
    std::unordered_map<int, std::unordered_map<int64_t, std::unique_ptr<Table>>> int_namespace_maps_;
    std::unordered_map<int, std::unordered_map<std::string, std::unique_ptr<Table>>> str_namespace_maps_;

    // COW state per key group (for snapshot consistency)
    std::vector<COWState> cow_states_;
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

private:
    std::unique_ptr<StateTable<K, V>> table_;
};

class StateEngine {
public:
    StateEngine(int start_key_group, int num_key_groups, int total_key_groups,
                Allocator* alloc = &DefaultAllocator::instance())
        : start_key_group_(start_key_group),
          num_key_groups_(num_key_groups),
          total_key_groups_(total_key_groups),
          alloc_(alloc),
          snapshot_version_(0) {}

    ~StateEngine() = default;

    // Register a new state. Returns handle ID.
    template <typename K, typename V>
    int64_t register_state(const std::string& state_name, bool void_namespace) {
        auto table = std::make_unique<StateTable<K, V>>(
            start_key_group_, num_key_groups_, void_namespace, alloc_);
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
        // Prepare COW in all state tables — must be called on the main thread
        // during the synchronous snapshot phase.
        // The actual COW state preparation is done per-table when needed.
        return snapshot_version_;
    }

    void release_snapshot() {
        // Release is called after async snapshot completes.
        // Individual tables release via typed dispatch from JNI layer.
    }

    uint64_t snapshot_version() const {
        return snapshot_version_;
    }

    // Accessors
    int start_key_group() const { return start_key_group_; }
    int num_key_groups() const { return num_key_groups_; }
    int total_key_groups() const { return total_key_groups_; }

    const std::unordered_map<int64_t, std::unique_ptr<StateTableHandle>>& state_handles() const {
        return state_handles_;
    }

    const std::unordered_map<int64_t, std::string>& state_names() const {
        return state_names_;
    }

    // Registry for checkpoint: maps state_id → opaque StateHandle* (from JNI layer).
    // The JNI layer registers these after state registration.
    void register_state_handle_ptr(int64_t state_id, void* handle_ptr) {
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

    int64_t next_handle_id_ = 1;
    std::unordered_map<int64_t, std::unique_ptr<StateTableHandle>> state_handles_;
    std::unordered_map<int64_t, std::string> state_names_;
    std::unordered_map<std::string, int64_t> name_to_id_;
    std::unordered_map<int64_t, void*> state_handle_ptrs_;  // state_id → StateHandle*

    uint64_t snapshot_version_;
};

}  // namespace forl0
