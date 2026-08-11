// Memory allocator interface for SwissTable and StateEngine.
// The default implementation uses aligned malloc. L0 memory is NOT used here;
// it is instead managed by HotCacheManager (see engine/hot_cache.h) as an
// explicit hot-key cache above StateTable.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>

namespace forl0 {

// Result of a split allocation — ctrl and slots may reside in different memory regions.
struct SplitResult {
    void* ctrl_ptr;    // ctrl array memory (may be L0 or heap)
    void* slots_ptr;   // slots array memory (always heap for large tables)
    bool is_split;     // true = ctrl and slots are separate allocations
};

// Abstract allocator interface — allows substitution of L0 Cache allocator.
class Allocator {
public:
    virtual ~Allocator() = default;

    // Allocate 'size' bytes with 'alignment' byte alignment.
    virtual void* allocate(size_t size, size_t alignment) = 0;

    // Deallocate a previously allocated block.
    virtual void deallocate(void* ptr, size_t size) = 0;

    // Split allocation: allocate ctrl and slots separately. Default is a single
    // contiguous allocation (ctrl followed by slots). The previous L0Allocator
    // override has been removed — L0 is now a hot-key cache above StateTable,
    // not a memory backend for SwissTable internals.
    virtual SplitResult allocate_split(size_t ctrl_size, size_t ctrl_align,
                                       size_t slots_size, size_t slots_align) {
        // Default: pack into one allocation (original behavior).
        size_t ctrl_padded = (ctrl_size + slots_align - 1) & ~(slots_align - 1);
        size_t total = ctrl_padded + slots_size;
        size_t align = std::max(ctrl_align, slots_align);
        void* p = allocate(total, align);
        return SplitResult{p, static_cast<char*>(p) + ctrl_padded, false};
    }

    // Deallocate a split allocation.
    virtual void deallocate_split(const SplitResult& sr,
                                  size_t ctrl_size, size_t slots_size) {
        if (sr.is_split) {
            // Should not happen with default allocator — defensive fallback.
            deallocate(sr.ctrl_ptr, ctrl_size);
            deallocate(sr.slots_ptr, slots_size);
        } else {
            // Single allocation: ctrl_ptr is the base.
            deallocate(sr.ctrl_ptr, ctrl_size + slots_size);
        }
    }
};

// Default allocator using posix_memalign / aligned_alloc.
class DefaultAllocator final : public Allocator {
public:
    static DefaultAllocator& instance() {
        static DefaultAllocator inst;
        return inst;
    }

    void* allocate(size_t size, size_t alignment) override {
        void* ptr = nullptr;
#if defined(_WIN32)
        ptr = _aligned_malloc(size, alignment);
        if (!ptr) throw std::bad_alloc();
#else
        if (posix_memalign(&ptr, alignment, size) != 0) {
            throw std::bad_alloc();
        }
#endif
        return ptr;
    }

    void deallocate(void* ptr, size_t /*size*/) override {
#if defined(_WIN32)
        _aligned_free(ptr);
#else
        free(ptr);
#endif
    }

private:
    DefaultAllocator() = default;
};

// Per-StateEngine native-memory accounting and admission control. SwissTable
// backing allocations go through this wrapper so a TaskManager receives a
// deterministic Java exception before native growth reaches the container's
// OOM killer. Split allocations are kept separate to make byte accounting
// exact (the default packed layout contains alignment padding that is not
// represented in Allocator::deallocate_split's arguments).
class CountingAllocator final : public Allocator {
public:
    explicit CountingAllocator(size_t max_bytes,
                               Allocator* delegate = &DefaultAllocator::instance())
        : delegate_(delegate), max_bytes_(max_bytes) {}

    void* allocate(size_t size, size_t alignment) override {
        reserve(size);
        try {
            return delegate_->allocate(size, alignment);
        } catch (...) {
            used_bytes_.fetch_sub(size, std::memory_order_relaxed);
            throw;
        }
    }

    void deallocate(void* ptr, size_t size) override {
        if (!ptr) return;
        delegate_->deallocate(ptr, size);
        used_bytes_.fetch_sub(size, std::memory_order_relaxed);
    }

    SplitResult allocate_split(size_t ctrl_size, size_t ctrl_align,
                               size_t slots_size, size_t slots_align) override {
        void* ctrl = allocate(ctrl_size, ctrl_align);
        try {
            void* slots = allocate(slots_size, slots_align);
            return SplitResult{ctrl, slots, true};
        } catch (...) {
            deallocate(ctrl, ctrl_size);
            throw;
        }
    }

    size_t used_bytes() const noexcept {
        return used_bytes_.load(std::memory_order_relaxed);
    }

    size_t peak_bytes() const noexcept {
        return peak_bytes_.load(std::memory_order_relaxed);
    }

    size_t max_bytes() const noexcept { return max_bytes_; }

private:
    void reserve(size_t size) {
        size_t current = used_bytes_.load(std::memory_order_relaxed);
        while (true) {
            if (max_bytes_ > 0 && (current > max_bytes_ || size > max_bytes_ - current)) {
                throw std::bad_alloc();
            }
            if (used_bytes_.compare_exchange_weak(
                    current, current + size,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                break;
            }
        }
        size_t peak = peak_bytes_.load(std::memory_order_relaxed);
        const size_t updated = current + size;
        while (updated > peak && !peak_bytes_.compare_exchange_weak(
                peak, updated, std::memory_order_relaxed, std::memory_order_relaxed)) {}
    }

    Allocator* delegate_;
    size_t max_bytes_;
    std::atomic<size_t> used_bytes_{0};
    std::atomic<size_t> peak_bytes_{0};
};

}  // namespace forl0
