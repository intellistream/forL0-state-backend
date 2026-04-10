// L0 Allocator: allocates SwissTable memory in Kunpeng L0 Memory (L3 Cache partition).
//
// When L0 hardware is available (libl0mempool.so + /dev/hisi_l0), small allocations
// go to L0 memory, eliminating LLC misses on ctrl/slot loads. Large allocations and
// all allocations when L0 is unavailable transparently fall back to DefaultAllocator.
//
// Thread safety: Not required — Flink state access is single-threaded.

#pragma once

#include "allocator.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unordered_map>
#include <dlfcn.h>

namespace forl0 {

class L0Allocator final : public Allocator {
public:
    // l0_capacity:    Total L0 budget in bytes (e.g. 20MB).
    // max_per_alloc:  Per-allocation L0 threshold — allocations larger than this go to Heap.
    // numa_node_id:   Desired NUMA node (-1 = auto-detect, currently unused by l0_mem_alloc).
    L0Allocator(size_t l0_capacity, size_t max_per_alloc, int numa_node_id = -1)
        : tuner_(nullptr),
          l0_capacity_(l0_capacity),
          l0_allocated_(0),
          max_per_alloc_(max_per_alloc),
          numa_node_id_(numa_node_id),
          heap_count_(0),
          l0_total_alloc_count_(0),
          lib_handle_(nullptr),
          cache_tuner_init_fn_(nullptr),
          cache_tuner_destroy_fn_(nullptr),
          l0_mem_alloc_fn_(nullptr),
          l0_mem_free_fn_(nullptr) {
        if (!init_l0_library()) {
            // L0 not available — all allocations will use DefaultAllocator.
            // This is the expected path on dev machines without Kunpeng hardware.
            fprintf(stderr, "[ForL0-L0Allocator] L0 library not available, "
                    "all allocations will use heap memory.\n");
        }
    }

    ~L0Allocator() override {
        // Free all outstanding L0 allocations.
        if (tuner_) {
            for (auto& [aligned_ptr, record] : l0_regions_) {
                l0_mem_free_fn_(tuner_, record.raw_ptr);
            }
            l0_regions_.clear();
            l0_allocated_ = 0;

            cache_tuner_destroy_fn_(tuner_);
            tuner_ = nullptr;
        }
        if (lib_handle_) {
            dlclose(lib_handle_);
            lib_handle_ = nullptr;
        }
    }

    void* allocate(size_t size, size_t alignment) override {
        // Strategy: small allocation + L0 has space → L0; otherwise → Heap.
        if (tuner_ && size <= max_per_alloc_
            && l0_allocated_ + size + alignment <= l0_capacity_) {
            // Over-allocate to guarantee alignment.
            size_t padded = size + alignment;
            void* raw = l0_mem_alloc_fn_(tuner_, padded);
            if (raw) {
                uintptr_t addr = reinterpret_cast<uintptr_t>(raw);
                uintptr_t aligned = (addr + alignment - 1) & ~(alignment - 1);
                void* result = reinterpret_cast<void*>(aligned);
                l0_regions_[result] = AllocRecord{raw, padded};
                l0_allocated_ += padded;
                l0_total_alloc_count_++;
                return result;
            }
            // L0 alloc failed — fall through to heap.
        }
        heap_count_++;
        return DefaultAllocator::instance().allocate(size, alignment);
    }

    void deallocate(void* ptr, size_t size) override {
        auto it = l0_regions_.find(ptr);
        if (it != l0_regions_.end()) {
            l0_mem_free_fn_(tuner_, it->second.raw_ptr);
            l0_allocated_ -= it->second.padded_size;
            l0_regions_.erase(it);
        } else {
            DefaultAllocator::instance().deallocate(ptr, size);
        }
    }

    // Diagnostics
    bool is_l0_active() const { return tuner_ != nullptr; }
    size_t l0_allocated() const { return l0_allocated_; }
    size_t l0_capacity() const { return l0_capacity_; }
    size_t l0_alloc_count() const { return l0_regions_.size(); }
    size_t l0_total_alloc_count() const { return l0_total_alloc_count_; }
    size_t heap_alloc_count() const { return heap_count_; }

private:
    struct AllocRecord {
        void* raw_ptr;       // Original pointer from l0_mem_alloc (before alignment)
        size_t padded_size;  // Actual allocation size (including alignment padding)
    };

    bool init_l0_library() {
        // dlopen the L0 memory pool library.
        lib_handle_ = dlopen("libl0mempool.so", RTLD_NOW);
        if (!lib_handle_) {
            fprintf(stderr, "[ForL0-L0Allocator] dlopen(libl0mempool.so) failed: %s\n",
                    dlerror());
            return false;
        }

        // Resolve function pointers.
        cache_tuner_init_fn_ = reinterpret_cast<CacheTunerInitFn>(
            dlsym(lib_handle_, "cache_tuner_init"));
        cache_tuner_destroy_fn_ = reinterpret_cast<CacheTunerDestroyFn>(
            dlsym(lib_handle_, "cache_tuner_destroy"));
        l0_mem_alloc_fn_ = reinterpret_cast<L0MemAllocFn>(
            dlsym(lib_handle_, "l0_mem_alloc"));
        l0_mem_free_fn_ = reinterpret_cast<L0MemFreeFn>(
            dlsym(lib_handle_, "l0_mem_free"));

        if (!cache_tuner_init_fn_ || !cache_tuner_destroy_fn_
            || !l0_mem_alloc_fn_ || !l0_mem_free_fn_) {
            fprintf(stderr, "[ForL0-L0Allocator] Failed to resolve L0 API symbols.\n");
            dlclose(lib_handle_);
            lib_handle_ = nullptr;
            return false;
        }

        // Initialize the cache tuner with requested capacity.
        int ret = cache_tuner_init_fn_(&tuner_, l0_capacity_);
        if (ret != 0 || !tuner_) {
            fprintf(stderr, "[ForL0-L0Allocator] cache_tuner_init failed (ret=%d). "
                    "L0 hardware may not be present.\n", ret);
            tuner_ = nullptr;
            dlclose(lib_handle_);
            lib_handle_ = nullptr;
            return false;
        }

        fprintf(stderr, "[ForL0-L0Allocator] L0 memory initialized: capacity=%zuMB, "
                "max_per_alloc=%zuKB\n",
                l0_capacity_ / (1024 * 1024), max_per_alloc_ / 1024);
        return true;
    }

    // Opaque cache_tuner handle
    using CacheTuner = void;

    // Function pointer types matching libl0mempool.so API.
    using CacheTunerInitFn    = int (*)(CacheTuner** tuner, size_t max_capacity);
    using CacheTunerDestroyFn = int (*)(CacheTuner* tuner);
    using L0MemAllocFn        = void* (*)(CacheTuner* tuner, size_t size);
    using L0MemFreeFn         = int (*)(CacheTuner* tuner, void* p);

    CacheTuner* tuner_;
    size_t l0_capacity_;
    size_t l0_allocated_;
    size_t max_per_alloc_;
    int numa_node_id_;
    std::unordered_map<void*, AllocRecord> l0_regions_;

    size_t heap_count_;
    size_t l0_total_alloc_count_;

    void* lib_handle_;
    CacheTunerInitFn cache_tuner_init_fn_;
    CacheTunerDestroyFn cache_tuner_destroy_fn_;
    L0MemAllocFn l0_mem_alloc_fn_;
    L0MemFreeFn l0_mem_free_fn_;
};

}  // namespace forl0
