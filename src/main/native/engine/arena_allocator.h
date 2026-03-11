// Memory allocator interface for SwissTable and StateEngine.
// Default implementation uses aligned malloc. L0 Cache allocator can be
// substituted via the Allocator interface.

#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>

namespace forl0 {

// Abstract allocator interface — allows substitution of L0 Cache allocator.
class Allocator {
public:
    virtual ~Allocator() = default;

    // Allocate 'size' bytes with 'alignment' byte alignment.
    virtual void* allocate(size_t size, size_t alignment) = 0;

    // Deallocate a previously allocated block.
    virtual void deallocate(void* ptr, size_t size) = 0;
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

}  // namespace forl0
