// hot_cache.cpp — HotCacheManager implementation.
//
// On dev machines without /dev/hisi_l0 the loader fails gracefully and
// `is_active()` returns false. Per design §3.2 we never fall back to heap.

#include "hot_cache.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <unistd.h>

namespace forl0 {

// ---------------------------------------------------------------------------
//  Default loader: real dlopen of libl0mempool.so
// ---------------------------------------------------------------------------
bool default_l0_bindings_loader(L0LibBindings* out, std::string* reason) {
    static const char* candidates[] = {
        "libl0mempool.so",
        "/usr/lib/libl0mempool.so",
        "/usr/lib64/libl0mempool.so",
        "/lib/libl0mempool.so",
        "/lib64/libl0mempool.so",
    };

    void* handle = nullptr;
    const char* last_err = nullptr;
    for (const char* p : candidates) {
        handle = dlopen(p, RTLD_NOW);
        if (handle) break;
        last_err = dlerror();
    }
    if (!handle) {
        if (reason) {
            *reason = "dlopen(libl0mempool.so) failed: ";
            *reason += last_err ? last_err : "unknown error";
        }
        return false;
    }

    // Sanity: also require /dev/hisi_l0. Otherwise even with the library
    // present cache_tuner_init may behave unpredictably.
    if (access("/dev/hisi_l0", F_OK) != 0 && access("/dev/l0", F_OK) != 0) {
        if (reason) *reason = "/dev/hisi_l0 and /dev/l0 not present";
        dlclose(handle);
        return false;
    }

    auto sym_init    = reinterpret_cast<L0LibBindings::CacheTunerInitFn>(   dlsym(handle, "cache_tuner_init"));
    auto sym_destroy = reinterpret_cast<L0LibBindings::CacheTunerDestroyFn>(dlsym(handle, "cache_tuner_destroy"));
    auto sym_alloc   = reinterpret_cast<L0LibBindings::L0MemAllocFn>(       dlsym(handle, "l0_mem_alloc"));
    auto sym_free    = reinterpret_cast<L0LibBindings::L0MemFreeFn>(        dlsym(handle, "l0_mem_free"));
    if (!sym_init || !sym_destroy || !sym_alloc || !sym_free) {
        if (reason) *reason = "dlsym failed for one or more libl0mempool symbols";
        dlclose(handle);
        return false;
    }

    out->lib_handle          = handle;
    out->cache_tuner_init    = sym_init;
    out->cache_tuner_destroy = sym_destroy;
    out->l0_mem_alloc        = sym_alloc;
    out->l0_mem_free         = sym_free;
    out->owns_lib_handle     = true;
    return true;
}

// ---------------------------------------------------------------------------
//  Manager construction
// ---------------------------------------------------------------------------
HotCacheManager::HotCacheManager(size_t requested_capacity_bytes,
                                 L0BindingsLoader loader,
                                 bool strict_allocation,
                                 uint64_t write_only_bypass_threshold)
    : requested_capacity_bytes_(requested_capacity_bytes),
      strict_allocation_(strict_allocation),
      write_only_bypass_threshold_(write_only_bypass_threshold) {
    if (requested_capacity_bytes < sizeof(HotSet)) {
        failure_reason_ = "l0-cache.size too small (< one HotSet)";
        return;
    }

    L0BindingsLoader use_loader = loader ? loader : &default_l0_bindings_loader;
    std::string load_reason;
    if (!use_loader(&bindings_, &load_reason)) {
        failure_reason_ = std::move(load_reason);
        return;
    }

    int rc = bindings_.cache_tuner_init(&tuner_, requested_capacity_bytes);
    if (rc != 0 || !tuner_) {
        failure_reason_ = "cache_tuner_init failed (rc != 0)";
        if (bindings_.lib_handle && bindings_.owns_lib_handle) dlclose(bindings_.lib_handle);
        bindings_ = {};
        return;
    }

    // Probe the configured quota without ever asking l0_mem_alloc for more
    // than cache_tuner_init registered. Non-strict mode may halve on failure;
    // strict mode makes exactly one full-quota attempt.
    size_t want = requested_capacity_bytes;
    void* base = nullptr;
    while (want >= sizeof(HotSet)) {
        base = bindings_.l0_mem_alloc(tuner_, want);
        if (base) break;
        if (strict_allocation_) break;
        want >>= 1;
    }
    if (!base) {
        failure_reason_ = "l0_mem_alloc probe failed";
        bindings_.cache_tuner_destroy(tuner_);
        tuner_ = nullptr;
        if (bindings_.lib_handle && bindings_.owns_lib_handle) dlclose(bindings_.lib_handle);
        bindings_ = {};
        return;
    }

    uintptr_t addr         = reinterpret_cast<uintptr_t>(base);
    uintptr_t aligned_addr = (addr + 63u) & ~uintptr_t(63);
    size_t pad             = aligned_addr - addr;
    if (pad + sizeof(HotSet) > want) {
        failure_reason_ = "l0_mem_alloc returned unalignable region";
        bindings_.l0_mem_free(tuner_, base);
        bindings_.cache_tuner_destroy(tuner_);
        tuner_ = nullptr;
        if (bindings_.lib_handle && bindings_.owns_lib_handle) dlclose(bindings_.lib_handle);
        bindings_ = {};
        return;
    }
    raw_base_       = base;
    l0_base_        = reinterpret_cast<void*>(aligned_addr);
    size_t usable   = want - pad;
    total_sets_     = static_cast<uint32_t>(usable / sizeof(HotSet));
    capacity_bytes_ = static_cast<size_t>(total_sets_) * sizeof(HotSet);
    bump_next_      = 0;
    active_         = total_sets_ > 0;
    if (!active_) failure_reason_ = "no HotSet fits in usable region";
    // A caller-provided pointer may need at most 63 bytes of 64B alignment
    // padding. This deterministic loss is not quota shrinkage.
    const size_t strict_usable_floor = requested_capacity_bytes_ > 63
        ? ((requested_capacity_bytes_ - 63) / sizeof(HotSet)) * sizeof(HotSet)
        : 0;
    if (active_ && strict_allocation_ && capacity_bytes_ < strict_usable_floor) {
        failure_reason_ = "strict L0 allocation returned less than requested quota";
        bindings_.l0_mem_free(tuner_, raw_base_);
        raw_base_ = nullptr;
        l0_base_ = nullptr;
        bindings_.cache_tuner_destroy(tuner_);
        tuner_ = nullptr;
        if (bindings_.lib_handle && bindings_.owns_lib_handle) dlclose(bindings_.lib_handle);
        bindings_ = {};
        total_sets_ = 0;
        capacity_bytes_ = 0;
        active_ = false;
    }
}

HotCacheManager::~HotCacheManager() { shutdown(); }

void HotCacheManager::shutdown() noexcept {
    for (auto& o : owned_) delete o.cache;
    owned_.clear();
    released_runs_.clear();
    active_ = false;

    if (tuner_ && bindings_.l0_mem_free && raw_base_) {
        bindings_.l0_mem_free(tuner_, raw_base_);
    }
    raw_base_ = nullptr;
    l0_base_  = nullptr;
    if (tuner_ && bindings_.cache_tuner_destroy) {
        bindings_.cache_tuner_destroy(tuner_);
    }
    tuner_ = nullptr;
    if (bindings_.lib_handle && bindings_.owns_lib_handle) {
        dlclose(bindings_.lib_handle);
    }
    bindings_ = {};
}

uint32_t HotCacheManager::free_sets() const noexcept {
    if (!active_) return 0;
    uint32_t f = total_sets_ - bump_next_;
    for (auto& r : released_runs_) f += r.second;
    return f;
}

size_t HotCacheManager::used_bytes() const noexcept {
    if (!active_) return 0;
    return (static_cast<size_t>(total_sets_) - free_sets()) * sizeof(HotSet);
}

std::unique_ptr<HotCacheLL> HotCacheManager::acquire_ll(uint32_t sets_requested) {
    if (!active_ || sets_requested == 0) return nullptr;

    uint32_t bump_avail = total_sets_ - bump_next_;
    uint32_t best_run   = bump_avail;
    for (auto& r : released_runs_) {
        if (r.second > best_run) best_run = r.second;
    }
    if (best_run == 0) return nullptr;

    uint32_t want = std::min(sets_requested, best_run);
    uint32_t n    = pow2_floor(want);
    if (n == 0) return nullptr;

    uint32_t start = 0;
    if (bump_avail >= n) {
        start = bump_next_;
        bump_next_ += n;
    } else {
        // Pick the smallest released run that still fits (best-fit).
        size_t pick = SIZE_MAX;
        for (size_t i = 0; i < released_runs_.size(); ++i) {
            if (released_runs_[i].second >= n) {
                if (pick == SIZE_MAX || released_runs_[i].second < released_runs_[pick].second) {
                    pick = i;
                }
            }
        }
        if (pick == SIZE_MAX) return nullptr;
        start = released_runs_[pick].first;
        uint32_t leftover = released_runs_[pick].second - n;
        if (leftover > 0) {
            released_runs_[pick] = {start + n, leftover};
        } else {
            released_runs_.erase(released_runs_.begin() + pick);
        }
    }

    HotSet* run_start = base_sets() + start;
    auto cache = std::unique_ptr<HotCacheLL>(
        new HotCacheLL(run_start, n, write_only_bypass_threshold_));
    owned_.push_back(Owned{cache.get(), start, n});
    return cache;
}

void HotCacheManager::release_ll(HotCacheLL* cache) {
    if (!cache) return;
    for (auto it = owned_.begin(); it != owned_.end(); ++it) {
        if (it->cache == cache) {
            uint32_t start = it->start;
            uint32_t count = it->count;
            owned_.erase(it);
            // Try to merge with adjacent released runs and the bump tail.
            // Adjacency to bump tail: if start + count == bump_next_, return to bump.
            if (start + count == bump_next_) {
                bump_next_ = start;
                // Also pull back any released run that ends exactly at the new frontier.
                bool merged;
                do {
                    merged = false;
                    for (auto rit = released_runs_.begin(); rit != released_runs_.end(); ++rit) {
                        if (rit->first + rit->second == bump_next_) {
                            bump_next_ = rit->first;
                            released_runs_.erase(rit);
                            merged = true;
                            break;
                        }
                    }
                } while (merged);
            } else {
                released_runs_.emplace_back(start, count);
                // Merge any adjacent runs.
                std::sort(released_runs_.begin(), released_runs_.end());
                std::vector<std::pair<uint32_t, uint32_t>> coalesced;
                for (auto& r : released_runs_) {
                    if (!coalesced.empty() && coalesced.back().first + coalesced.back().second == r.first) {
                        coalesced.back().second += r.second;
                    } else {
                        coalesced.push_back(r);
                    }
                }
                released_runs_.swap(coalesced);
            }
            delete cache;
            return;
        }
    }
}

// ---------------------------------------------------------------------------
//  Aggregate counters (Phase C MetricGroup integration).
// ---------------------------------------------------------------------------
uint64_t HotCacheManager::total_lookups() const noexcept {
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->lookups();
    return sum;
}

uint64_t HotCacheManager::total_hits() const noexcept {
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->hits();
    return sum;
}

uint64_t HotCacheManager::total_invalidations() const noexcept {
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->invalidations();
    return sum;
}

uint64_t HotCacheManager::total_writes() const noexcept {
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->writes();
    return sum;
}

uint64_t HotCacheManager::total_bypass_events() const noexcept {
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->bypass_events();
    return sum;
}

// ---------------------------------------------------------------------------
//  Adaptive rebalancer (Phase C §6.3).
//
//  Best-effort heuristic: every `interval_ops` lookups per cache, if the
//  short-horizon miss_rate exceeds the threshold, reset that cache's set
//  array and local counters so eviction/recency state restarts fresh with
//  the current hot working set. Clearing a cache is correct (SwissTable
//  remains the single source of truth) and cheap (one memset per set).
// ---------------------------------------------------------------------------
uint32_t HotCacheManager::rebalance_if_needed(uint64_t interval_ops,
                                              double miss_rate_threshold) noexcept {
    if (!active_) return 0;
    uint32_t rebalanced = 0;
    for (auto& o : owned_) {
        HotCacheLL* c = o.cache;
        // Snapshot hits/misses BEFORE consuming the delta so we can compute
        // the miss_rate over the recent window.
        uint64_t hits   = c->hits();
        uint64_t misses = c->misses();
        uint64_t delta  = c->consume_rebalance_delta();
        if (delta < interval_ops) continue;
        uint64_t lookups = hits + misses;
        if (lookups == 0) continue;
        double miss_rate = static_cast<double>(misses) / static_cast<double>(lookups);
        if (miss_rate <= miss_rate_threshold) continue;
        c->clear();
        c->reset_stats();
        ++rebalanced;
    }
    return rebalanced;
}

}  // namespace forl0
