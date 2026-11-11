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
#include <stdexcept>
#include <new>
#include <chrono>
#include <unistd.h>
#include <unistd.h>

namespace forl0 {

namespace {
std::mutex g_process_manager_mutex;
std::weak_ptr<HotCacheManager> g_process_manager;


// Opt-in ordinary-memory mode: same cache code, layout, quota and replacement
// policy; only the backing allocator changes. This is not simulated latency.
struct DramTuner {
    size_t capacity;
    void* allocation = nullptr;
};
int dram_tuner_init(void** out,size_t capacity) {
    auto* tuner=new (std::nothrow) DramTuner{capacity,nullptr};
    if(!tuner)return -1;
    *out=tuner;return 0;
}
void* dram_alloc(void* handle,size_t size) {
    auto* tuner=static_cast<DramTuner*>(handle);
    if(!tuner||tuner->allocation||size>tuner->capacity)return nullptr;
    void* memory=nullptr;
    if(posix_memalign(&memory,64,size)!=0)return nullptr;
    tuner->allocation=memory;return memory;
}
int dram_free(void* handle,void* memory) {
    auto* tuner=static_cast<DramTuner*>(handle);
    if(!tuner||memory!=tuner->allocation)return -1;
    std::free(memory);tuner->allocation=nullptr;return 0;
}
int dram_tuner_destroy(void* handle) {
    auto* tuner=static_cast<DramTuner*>(handle);
    if(!tuner)return 0;
    std::free(tuner->allocation);delete tuner;return 0;
}

}  // namespace

// ---------------------------------------------------------------------------
//  Default loader: real dlopen of libl0mempool.so
// ---------------------------------------------------------------------------
bool default_l0_bindings_loader(L0LibBindings* out, std::string* reason) {
    const char* mode=std::getenv("FORL0_L0_MEMORY_MODE");
    if(mode&&std::strcmp(mode,"dram")==0) {
        out->cache_tuner_init=&dram_tuner_init;
        out->cache_tuner_destroy=&dram_tuner_destroy;
        out->l0_mem_alloc=&dram_alloc;out->l0_mem_free=&dram_free;
        out->owns_lib_handle=false;out->memory_mode="dram_emulation";
        return true;
    }
    if(mode&&mode[0]&&std::strcmp(mode,"hardware")!=0) {
        if(reason)*reason="invalid FORL0_L0_MEMORY_MODE; expected hardware or dram";
        return false;
    }

    const char* explicit_library=std::getenv("FORL0_L0_LIBRARY_PATH");
    const char* candidates[] = {
        explicit_library,
        "libl0mempool.so",
        "/usr/lib/libl0mempool.so",
        "/usr/lib64/libl0mempool.so",
        "/lib/libl0mempool.so",
        "/lib64/libl0mempool.so",
    };

    void* handle = nullptr;
    const char* last_err = nullptr;
    for (const char* p : candidates) {
        if(!p||!*p)continue;
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

    memory_mode_=bindings_.memory_mode;
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

HotCacheManager::~HotCacheManager() {
    {std::lock_guard<std::mutex> lock(telemetry_mutex_);telemetry_stop_=true;}
    telemetry_cv_.notify_all();if(telemetry_thread_.joinable())telemetry_thread_.join();
    if(telemetry_file_){std::fclose(telemetry_file_);telemetry_file_=nullptr;} shutdown(); }

std::shared_ptr<HotCacheManager> HotCacheManager::acquire_process_shared(
        size_t requested_capacity_bytes,
        L0BindingsLoader loader,
        bool strict_allocation,
        uint64_t write_only_bypass_threshold,
        bool* reused) {
    std::lock_guard<std::mutex> registry_lock(g_process_manager_mutex);
    if (auto existing = g_process_manager.lock()) {
        if (existing->requested_capacity_bytes_ != requested_capacity_bytes
                || existing->strict_allocation_ != strict_allocation
                || existing->write_only_bypass_threshold_ != write_only_bypass_threshold) {
            throw std::runtime_error(
                "incompatible process-shared L0 configuration while another StateEngine is active");
        }
        if (reused) *reused = true;
        return existing;
    }

    auto created = std::shared_ptr<HotCacheManager>(
        new HotCacheManager(requested_capacity_bytes, loader, strict_allocation,
                            write_only_bypass_threshold));
    created->start_telemetry();
    g_process_manager = created;
    if (reused) *reused = false;
    return created;
}

void HotCacheManager::reset_process_shared_for_tests() {
    std::lock_guard<std::mutex> registry_lock(g_process_manager_mutex);
    g_process_manager.reset();
}

void HotCacheManager::shutdown() noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    for(auto& o:rows_)delete o.cache;
    rows_.clear();
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
    std::lock_guard<std::mutex> lock(manager_mutex_);
    return free_sets_unlocked();
}

uint32_t HotCacheManager::free_sets_unlocked() const noexcept {
    if (!active_) return 0;
    uint32_t f = total_sets_ - bump_next_;
    for (auto& r : released_runs_) f += r.second;
    return f;
}

size_t HotCacheManager::used_bytes() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    if (!active_) return 0;
    return (static_cast<size_t>(total_sets_) - free_sets_unlocked()) * sizeof(HotSet);
}


std::pair<uint32_t,uint32_t> HotCacheManager::acquire_run_unlocked(uint32_t n) {
    if(!active_||n==0)return {0,0};
    const uint32_t available=total_sets_-bump_next_;
    if(available>=n){uint32_t start=bump_next_;bump_next_+=n;return {start,n};}
    size_t pick=SIZE_MAX;
    for(size_t i=0;i<released_runs_.size();++i)
        if(released_runs_[i].second>=n&&(pick==SIZE_MAX||released_runs_[i].second<released_runs_[pick].second))pick=i;
    if(pick==SIZE_MAX)return {0,0};
    uint32_t start=released_runs_[pick].first,left=released_runs_[pick].second-n;
    if(left)released_runs_[pick]={start+n,left};
    else released_runs_.erase(released_runs_.begin()+pick);
    return {start,n};
}
void HotCacheManager::release_run_unlocked(uint32_t start,uint32_t count) {
    released_runs_.emplace_back(start,count);
    std::sort(released_runs_.begin(),released_runs_.end());
    std::vector<std::pair<uint32_t,uint32_t>> combined;
    for(auto r:released_runs_){
        if(!combined.empty()&&combined.back().first+combined.back().second==r.first)combined.back().second+=r.second;
        else combined.push_back(r);
    }
    while(!combined.empty()&&combined.back().first+combined.back().second==bump_next_) {
        bump_next_=combined.back().first;combined.pop_back();
    }
    released_runs_.swap(combined);
}
std::unique_ptr<HotCacheLL> HotCacheManager::acquire_ll(uint32_t requested) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint32_t best=total_sets_-bump_next_;
    for(auto r:released_runs_)best=std::max(best,r.second);
    auto run=acquire_run_unlocked(pow2_floor(std::min(requested,best)));
    if(!run.second)return nullptr;
    auto cache=std::make_unique<HotCacheLL>(base_sets()+run.first,run.second,write_only_bypass_threshold_);
    owned_.push_back({cache.get(),run.first,run.second});return cache;
}
void HotCacheManager::release_ll(HotCacheLL* cache) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    for(auto it=owned_.begin();it!=owned_.end();++it)if(it->cache==cache) {
        release_run_unlocked(it->start,it->count);owned_.erase(it);delete cache;return;
    }
}
std::unique_ptr<HotRowCache> HotCacheManager::acquire_rows(size_t bytes) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint32_t best=total_sets_-bump_next_;
    for(auto r:released_runs_)best=std::max(best,r.second);
    uint32_t sets=static_cast<uint32_t>(std::min(bytes,static_cast<size_t>(best)*sizeof(HotSet))/sizeof(HotRowSet));
    uint32_t units=static_cast<uint32_t>((static_cast<size_t>(sets)*sizeof(HotRowSet)+sizeof(HotSet)-1)/sizeof(HotSet));
    auto run=acquire_run_unlocked(units);if(!sets||!run.second)return nullptr;
    auto cache=std::make_unique<HotRowCache>(base_sets()+run.first,sets);
    rows_.push_back({cache.get(),run.first,run.second});return cache;
}
void HotCacheManager::release_rows(HotRowCache* cache) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    for(auto it=rows_.begin();it!=rows_.end();++it)if(it->cache==cache) {
        release_run_unlocked(it->start,it->count);rows_.erase(it);delete cache;return;
    }
}
void HotCacheManager::start_telemetry() {
    const char* dir=std::getenv("FORL0_L0_STATS_DIRECTORY");if(!dir||!*dir||!active_)return;
    std::string path=std::string(dir)+"/l0-"+std::to_string(getpid())+".jsonl";
    telemetry_file_=std::fopen(path.c_str(),"a");
    if(!telemetry_file_)throw std::runtime_error("Cannot create L0 telemetry file: "+path);
    telemetry_thread_=std::thread([this]{
        std::unique_lock<std::mutex> lock(telemetry_mutex_);
        while(!telemetry_stop_) {
            lock.unlock();write_telemetry();lock.lock();
            telemetry_cv_.wait_for(lock,std::chrono::seconds(1),[this]{return telemetry_stop_;});
        }
    });
}
void HotCacheManager::write_telemetry() {
    if(!telemetry_file_)return;
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t scalar_lookups=0,scalar_hits=0,writes=0,row_lookups=0,row_hits=0,fills=0,evictions=0,row_bytes=0,hit_lines=0,bypasses=0,writebacks=0;
    for(auto& o:owned_){scalar_lookups+=o.cache->lookups();scalar_hits+=o.cache->hits();writes+=o.cache->writes();}
    for(auto& o:rows_){row_lookups+=o.cache->lookups();row_hits+=o.cache->hits();fills+=o.cache->fill_bytes();evictions+=o.cache->evictions();row_bytes+=o.cache->num_sets()*sizeof(HotRowSet);hit_lines+=o.cache->hit_lines();bypasses+=o.cache->bypasses();writebacks+=o.cache->writeback_bytes();}
    auto now=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::fprintf(telemetry_file_,"{\"time_ms\":%lld,\"pid\":%d,\"mode\":\"%s\",\"capacity_bytes\":%zu,\"used_bytes\":%zu,\"scalar_lookups\":%llu,\"scalar_hits\":%llu,\"scalar_writes\":%llu,\"row_lookups\":%llu,\"row_hits\":%llu,\"row_fill_bytes\":%llu,\"row_evictions\":%llu,\"row_writeback_bytes\":%llu,\"row_bypasses\":%llu,\"row_hit_lines\":%llu,\"row_cache_bytes\":%llu,\"scalar_caches\":%zu,\"row_caches\":%zu}\n",
        static_cast<long long>(now),getpid(),memory_mode_,capacity_bytes_,(static_cast<size_t>(total_sets_)-free_sets_unlocked())*sizeof(HotSet),
        (unsigned long long)scalar_lookups,(unsigned long long)scalar_hits,(unsigned long long)writes,
        (unsigned long long)row_lookups,(unsigned long long)row_hits,(unsigned long long)fills,(unsigned long long)evictions,(unsigned long long)writebacks,(unsigned long long)bypasses,(unsigned long long)hit_lines,(unsigned long long)row_bytes,owned_.size(),rows_.size());
    std::fflush(telemetry_file_);
}

// ---------------------------------------------------------------------------
//  Aggregate counters (Phase C MetricGroup integration).
// ---------------------------------------------------------------------------
uint64_t HotCacheManager::total_lookups() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->lookups();
    for(const auto& o:rows_)sum+=o.cache->lookups();
    return sum;
}

uint64_t HotCacheManager::total_hits() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->hits();
    for(const auto& o:rows_)sum+=o.cache->hits();
    return sum;
}

uint64_t HotCacheManager::total_invalidations() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->invalidations();
    return sum;
}

uint64_t HotCacheManager::total_writes() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->writes();
    return sum;
}

uint64_t HotCacheManager::total_bypass_events() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    uint64_t sum = 0;
    for (const auto& o : owned_) sum += o.cache->bypass_events();
    return sum;
}

uint32_t HotCacheManager::cache_count() const noexcept {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    return static_cast<uint32_t>(owned_.size()+rows_.size());
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
    std::lock_guard<std::mutex> lock(manager_mutex_);
    if (!active_) return 0;
    uint32_t rebalanced = 0;
    for (auto& o : owned_) {
        if (o.cache->rebalance_if_needed(interval_ops, miss_rate_threshold)) {
            ++rebalanced;
        }
    }
    return rebalanced;
}

}  // namespace forl0
