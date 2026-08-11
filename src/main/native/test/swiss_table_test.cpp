// SwissTable unit tests — covers insert/find/delete/rehash/iterate and boundary conditions.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "swiss_table.h"
#include "type_layout.h"  // FixedRow, FixedRowHash

#include <map>
#include <set>
#include <string>

using namespace forl0;

// ============================================================================
//  Basic Operations
// ============================================================================

TEST(SwissTableTest, InsertAndFind) {
    SwissTable<int64_t, int64_t> table(16);

    auto [ptr1, inserted1] = table.insert_or_assign(100, 1000);
    ASSERT_TRUE(inserted1);
    ASSERT_EQ(*ptr1, 1000);

    // Update existing key
    auto [ptr2, inserted2] = table.insert_or_assign(100, 2000);
    ASSERT_FALSE(inserted2);
    ASSERT_EQ(*ptr2, 2000);

    // Find
    int64_t* found = table.find(100);
    ASSERT_NE(found, nullptr);
    ASSERT_EQ(*found, 2000);

    // Non-existent key
    ASSERT_EQ(table.find(999), nullptr);
}

TEST(SwissTableTest, EraseAndSize) {
    SwissTable<int64_t, std::string> table;

    table.insert_or_assign(1, std::string("one"));
    table.insert_or_assign(2, std::string("two"));
    table.insert_or_assign(3, std::string("three"));
    ASSERT_EQ(table.size(), 3u);

    bool erased = table.erase(2);
    ASSERT_TRUE(erased);
    ASSERT_EQ(table.size(), 2u);

    ASSERT_EQ(table.find(2), nullptr);
    ASSERT_NE(table.find(1), nullptr);
    ASSERT_NE(table.find(3), nullptr);

    // Erase non-existent returns false
    ASSERT_FALSE(table.erase(999));
    ASSERT_EQ(table.size(), 2u);
}

TEST(SwissTableTest, EmplaceInsertIfAbsent) {
    SwissTable<int64_t, int64_t> table;

    auto [ptr1, ins1] = table.emplace(10, 100);
    ASSERT_TRUE(ins1);
    ASSERT_EQ(*ptr1, 100);

    // Emplace same key — should not overwrite
    auto [ptr2, ins2] = table.emplace(10, 999);
    ASSERT_FALSE(ins2);
    ASSERT_EQ(*ptr2, 100);  // original value retained
}

TEST(SwissTableTest, Clear) {
    SwissTable<int64_t, int64_t> table;
    table.insert_or_assign(1, 10);
    table.insert_or_assign(2, 20);
    ASSERT_EQ(table.size(), 2u);

    table.clear();
    ASSERT_EQ(table.size(), 0u);
    ASSERT_TRUE(table.empty());
    ASSERT_EQ(table.find(1), nullptr);

    // Can insert after clear
    table.insert_or_assign(3, 30);
    ASSERT_EQ(table.size(), 1u);
    ASSERT_EQ(*table.find(3), 30);
}

// ============================================================================
//  Growth and Rehashing
// ============================================================================

TEST(SwissTableTest, AutomaticGrowth) {
    SwissTable<int32_t, int32_t> table(16);
    size_t initial_capacity = table.capacity();

    // Insert enough to trigger growth (load factor 7/8)
    for (int i = 0; i < 20; ++i) {
        table.insert_or_assign(i, i * 100);
    }

    ASSERT_GT(table.capacity(), initial_capacity);
    ASSERT_EQ(table.size(), 20u);

    // All values still retrievable
    for (int i = 0; i < 20; ++i) {
        int32_t* v = table.find(i);
        ASSERT_NE(v, nullptr);
        ASSERT_EQ(*v, i * 100);
    }
}

TEST(SwissTableTest, MaxCapacityFailsClosedWithoutLosingEntries) {
    SwissTable<int64_t, int64_t> table(
        16, &DefaultAllocator::instance(), 16, 0.75);
    for (int64_t i = 0; i < 12; ++i) table.insert_or_assign(i, i * 10);
    ASSERT_THROW(table.insert_or_assign(12, 120), std::length_error);
    ASSERT_EQ(table.size(), 12u);
    for (int64_t i = 0; i < 12; ++i) ASSERT_EQ(*table.find(i), i * 10);
}

TEST(SwissTableTest, CountingAllocatorEnforcesAndReleasesBudget) {
    CountingAllocator allocator(96);
    void* first = allocator.allocate(64, 16);
    ASSERT_EQ(allocator.used_bytes(), 64u);
    ASSERT_THROW(allocator.allocate(64, 16), std::bad_alloc);
    ASSERT_EQ(allocator.used_bytes(), 64u);
    allocator.deallocate(first, 64);
    ASSERT_EQ(allocator.used_bytes(), 0u);
    ASSERT_EQ(allocator.peak_bytes(), 64u);
}

TEST(SwissTableTest, RejectedGrowthLeavesExistingTableUsable) {
    CountingAllocator allocator(500);
    SwissTable<int64_t, int64_t> table(16, &allocator, 0, 0.75);
    for (int64_t i = 0; i < 12; ++i) table.insert_or_assign(i, i * 10);
    ASSERT_THROW(table.insert_or_assign(12, 120), std::bad_alloc);
    ASSERT_EQ(table.size(), 12u);
    for (int64_t i = 0; i < 12; ++i) ASSERT_EQ(*table.find(i), i * 10);
    table.insert_or_assign(5, 999);
    ASSERT_EQ(*table.find(5), 999);
}

TEST(SwissTableTest, TombstoneReclamation) {
    SwissTable<int64_t, int64_t> table(16);

    // Fill and erase repeatedly to create tombstones
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 10; ++i) {
            table.insert_or_assign(i, round * 100 + i);
        }
        for (int i = 0; i < 10; ++i) {
            table.erase(i);
        }
    }

    ASSERT_TRUE(table.empty());

    // Should still work after tombstone reclamation on next insert cycle
    for (int i = 0; i < 10; ++i) {
        table.insert_or_assign(i, i);
    }
    ASSERT_EQ(table.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(*table.find(i), i);
    }
}

// ============================================================================
//  Iteration
// ============================================================================

TEST(SwissTableTest, ForEachIteration) {
    SwissTable<int64_t, int64_t> table;

    table.insert_or_assign(10, 100);
    table.insert_or_assign(20, 200);
    table.insert_or_assign(30, 300);

    std::map<int64_t, int64_t> collected;
    table.for_each([&collected](const int64_t& k, int64_t& v) {
        collected[k] = v;
    });

    ASSERT_EQ(collected.size(), 3u);
    ASSERT_EQ(collected[10], 100);
    ASSERT_EQ(collected[20], 200);
    ASSERT_EQ(collected[30], 300);
}

TEST(SwissTableTest, ForEachEmptyTable) {
    SwissTable<int64_t, int64_t> table;
    int count = 0;
    table.for_each([&count](const int64_t&, int64_t&) { ++count; });
    ASSERT_EQ(count, 0);
}

// ============================================================================
//  Move Semantics
// ============================================================================

TEST(SwissTableTest, MoveConstruction) {
    SwissTable<int64_t, int64_t> table1(16);
    table1.insert_or_assign(1, 10);
    table1.insert_or_assign(2, 20);
    size_t cap1 = table1.capacity();

    SwissTable<int64_t, int64_t> table2(std::move(table1));

    // table1 is now empty
    ASSERT_EQ(table1.size(), 0u);
    ASSERT_EQ(table1.capacity(), 0u);

    // table2 owns the data
    ASSERT_EQ(table2.size(), 2u);
    ASSERT_EQ(table2.capacity(), cap1);
    ASSERT_EQ(*table2.find(1), 10);
    ASSERT_EQ(*table2.find(2), 20);
}

TEST(SwissTableTest, MoveAssignment) {
    SwissTable<int64_t, int64_t> table1;
    table1.insert_or_assign(1, 10);

    SwissTable<int64_t, int64_t> table2;
    table2.insert_or_assign(99, 99);

    table2 = std::move(table1);

    ASSERT_EQ(table2.size(), 1u);
    ASSERT_EQ(*table2.find(1), 10);
    ASSERT_EQ(table2.find(99), nullptr);
}

// ============================================================================
//  String Values
// ============================================================================

TEST(SwissTableTest, StringValues) {
    SwissTable<std::string, std::string> table;

    table.insert_or_assign(std::string("hello"), std::string("world"));
    table.insert_or_assign(std::string("foo"), std::string("bar"));

    ASSERT_EQ(*table.find(std::string("hello")), "world");
    ASSERT_EQ(*table.find(std::string("foo")), "bar");

    table.erase(std::string("hello"));
    ASSERT_EQ(table.find(std::string("hello")), nullptr);
}

// ============================================================================
//  FixedRow Keys
// ============================================================================

TEST(SwissTableTest, FixedRowKey) {
    SwissTable<FixedRow, int64_t, FixedRowHash> table;

    FixedRow key1(3);
    key1.f[0] = 100;
    key1.f[1] = 200;
    key1.f[2] = 300;

    table.insert_or_assign(key1, 1000);

    // Find with same values
    FixedRow key2(3);
    key2.f[0] = 100;
    key2.f[1] = 200;
    key2.f[2] = 300;

    int64_t* val = table.find(key2);
    ASSERT_NE(val, nullptr);
    ASSERT_EQ(*val, 1000);

    // Different key
    FixedRow key3(3);
    key3.f[0] = 100;
    key3.f[1] = 200;
    key3.f[2] = 999;

    ASSERT_EQ(table.find(key3), nullptr);
}

// ============================================================================
//  Boundary Conditions
// ============================================================================

TEST(SwissTableTest, FullLoadAndRehash) {
    // Start small, fill to capacity
    SwissTable<int64_t, int64_t> table(16);
    size_t cap = table.capacity();

    // Insert exactly max_load entries (7/8 of capacity)
    size_t max_load = cap - cap / 8;
    for (size_t i = 0; i < max_load; ++i) {
        table.insert_or_assign(static_cast<int64_t>(i), static_cast<int64_t>(i * 10));
    }
    ASSERT_EQ(table.size(), max_load);

    // One more should trigger growth
    table.insert_or_assign(static_cast<int64_t>(max_load), 0);
    ASSERT_GT(table.capacity(), cap);

    // All entries still accessible
    for (size_t i = 0; i <= max_load; ++i) {
        ASSERT_NE(table.find(static_cast<int64_t>(i)), nullptr);
    }
}

TEST(SwissTableTest, DeleteAllThenInsert) {
    SwissTable<int64_t, int64_t> table;

    for (int i = 0; i < 100; ++i) {
        table.insert_or_assign(i, i);
    }
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(table.erase(i));
    }
    ASSERT_TRUE(table.empty());

    // Insert again after full deletion
    for (int i = 0; i < 50; ++i) {
        table.insert_or_assign(i, i * 2);
    }
    ASSERT_EQ(table.size(), 50u);
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(*table.find(i), i * 2);
    }
}

// ============================================================================
//  Stress Test
// ============================================================================

TEST(SwissTableTest, StressManyEntries) {
    SwissTable<int64_t, int64_t> table;
    const int N = 10000;

    for (int i = 0; i < N; ++i) {
        table.insert_or_assign(i, static_cast<int64_t>(i) * 1000);
    }

    ASSERT_EQ(table.size(), static_cast<size_t>(N));

    // Verify samples
    ASSERT_EQ(*table.find(0), 0);
    ASSERT_EQ(*table.find(5000), 5000000);
    ASSERT_EQ(*table.find(N - 1), static_cast<int64_t>(N - 1) * 1000);

    // Delete half
    for (int i = 0; i < N / 2; ++i) {
        table.erase(i);
    }
    ASSERT_EQ(table.size(), static_cast<size_t>(N / 2));

    // Verify remaining
    for (int i = N / 2; i < N; ++i) {
        ASSERT_NE(table.find(i), nullptr);
    }
}

TEST(SwissTableTest, StressMixedOps) {
    SwissTable<int64_t, std::string> table;

    // Insert, erase, re-insert same keys
    for (int cycle = 0; cycle < 3; ++cycle) {
        for (int i = 0; i < 100; ++i) {
            table.insert_or_assign(i, std::string("value_") + std::to_string(cycle * 100 + i));
        }
        for (int i = 0; i < 100; ++i) {
            table.erase(i);
        }
    }

    ASSERT_TRUE(table.empty());
}

// ============================================================================
//  Split Allocation Tests
// ============================================================================

// A test allocator that forces split allocation for tables whose total size
// exceeds a configurable threshold. Tracks all allocations to verify no leaks.
class ForceSplitAllocator : public Allocator {
public:
    // split_threshold: total allocation size above which split is forced.
    explicit ForceSplitAllocator(size_t split_threshold = 1024)
        : split_threshold_(split_threshold),
          alloc_count_(0), dealloc_count_(0),
          split_alloc_count_(0), split_dealloc_count_(0),
          total_allocated_(0) {}

    ~ForceSplitAllocator() override = default;

    void* allocate(size_t size, size_t alignment) override {
        void* ptr = nullptr;
        if (posix_memalign(&ptr, alignment, size) != 0) {
            throw std::bad_alloc();
        }
        alloc_count_++;
        total_allocated_ += size;
        return ptr;
    }

    void deallocate(void* ptr, size_t size) override {
        free(ptr);
        dealloc_count_++;
        total_allocated_ -= size;
    }

    SplitResult allocate_split(size_t ctrl_size, size_t ctrl_align,
                               size_t slots_size, size_t slots_align) override {
        size_t sa = std::max(ctrl_align, slots_align);
        size_t ctrl_padded = (ctrl_size + sa - 1) & ~(sa - 1);
        size_t total = ctrl_padded + slots_size;

        if (total > split_threshold_) {
            // Force split: allocate ctrl and slots separately
            void* ctrl_ptr = nullptr;
            if (posix_memalign(&ctrl_ptr, ctrl_align, ctrl_size) != 0) {
                throw std::bad_alloc();
            }
            void* slots_ptr = nullptr;
            if (posix_memalign(&slots_ptr, slots_align, slots_size) != 0) {
                free(ctrl_ptr);
                throw std::bad_alloc();
            }
            std::memset(slots_ptr, 0, slots_size);
            alloc_count_ += 2;
            total_allocated_ += ctrl_size + slots_size;
            split_alloc_count_++;
            return SplitResult{ctrl_ptr, slots_ptr, true};
        }

        // Small table: unified allocation
        void* p = nullptr;
        if (posix_memalign(&p, sa, total) != 0) {
            throw std::bad_alloc();
        }
        alloc_count_++;
        total_allocated_ += total;
        return SplitResult{p, static_cast<char*>(p) + ctrl_padded, false};
    }

    void deallocate_split(const SplitResult& sr,
                          size_t ctrl_size, size_t slots_size) override {
        if (sr.is_split) {
            free(sr.ctrl_ptr);
            free(sr.slots_ptr);
            dealloc_count_ += 2;
            total_allocated_ -= (ctrl_size + slots_size);
            split_dealloc_count_++;
        } else {
            free(sr.ctrl_ptr);
            dealloc_count_++;
            total_allocated_ -= (ctrl_size + slots_size);
        }
    }

    size_t alloc_count() const { return alloc_count_; }
    size_t dealloc_count() const { return dealloc_count_; }
    size_t split_alloc_count() const { return split_alloc_count_; }
    size_t split_dealloc_count() const { return split_dealloc_count_; }
    size_t outstanding() const { return alloc_count_ - dealloc_count_; }
    int64_t total_allocated() const { return total_allocated_; }

private:
    size_t split_threshold_;
    size_t alloc_count_;
    size_t dealloc_count_;
    size_t split_alloc_count_;
    size_t split_dealloc_count_;
    int64_t total_allocated_;
};

TEST(SwissTableSplitTest, BasicInsertFindWithSplit) {
    // Threshold 512B — even the initial 16-slot table (~288B ctrl+slots) may
    // not trigger split, but growth to 32+ slots will.
    ForceSplitAllocator alloc(512);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    // Insert enough to trigger growth past the split threshold
    for (int i = 0; i < 100; ++i) {
        table.insert_or_assign(i, static_cast<int64_t>(i) * 10);
    }

    // Verify all values accessible
    for (int i = 0; i < 100; ++i) {
        int64_t* v = table.find(i);
        ASSERT_NE(v, nullptr);
        ASSERT_EQ(*v, static_cast<int64_t>(i) * 10);
    }

    // At least one split allocation should have occurred during growth
    ASSERT_GT(alloc.split_alloc_count(), 0u);
}

TEST(SwissTableSplitTest, EraseWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    for (int i = 0; i < 50; ++i) {
        table.insert_or_assign(i, i * 100);
    }

    // Erase half
    for (int i = 0; i < 25; ++i) {
        ASSERT_TRUE(table.erase(i));
    }
    ASSERT_EQ(table.size(), 25u);

    // Remaining entries intact
    for (int i = 25; i < 50; ++i) {
        ASSERT_NE(table.find(i), nullptr);
        ASSERT_EQ(*table.find(i), i * 100);
    }
    // Erased entries not found
    for (int i = 0; i < 25; ++i) {
        ASSERT_EQ(table.find(i), nullptr);
    }
}

TEST(SwissTableSplitTest, ClearWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    for (int i = 0; i < 100; ++i) {
        table.insert_or_assign(i, i);
    }
    ASSERT_GT(alloc.split_alloc_count(), 0u);

    table.clear();
    ASSERT_EQ(table.size(), 0u);

    // Re-insert after clear
    for (int i = 0; i < 50; ++i) {
        table.insert_or_assign(i, i * 2);
    }
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(*table.find(i), i * 2);
    }
}

TEST(SwissTableSplitTest, GrowthTransitionUnifiedToSplit) {
    // Start with a high threshold so initial table is unified,
    // then lower-equivalent: use threshold that the initial 16-slot table fits
    // but 256+ slot table does not.
    ForceSplitAllocator alloc(2048);  // 16-slot: ~288B unified; 256-slot: ~4.3KB split
    SwissTable<int64_t, int64_t> table(16, &alloc);

    ASSERT_EQ(alloc.split_alloc_count(), 0u);  // Initial table should be unified

    // Insert enough to grow past 128 slots (threshold ~2KB)
    for (int i = 0; i < 200; ++i) {
        table.insert_or_assign(i, i);
    }

    // Should have transitioned to split during growth
    ASSERT_GT(alloc.split_alloc_count(), 0u);

    // All data intact after transition
    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(*table.find(i), i);
    }
}

TEST(SwissTableSplitTest, RehashWithTombstonesInSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    // Fill, delete, refill to create tombstones and trigger rehash
    for (int round = 0; round < 5; ++round) {
        for (int i = 0; i < 30; ++i) {
            table.insert_or_assign(i, round * 100 + i);
        }
        for (int i = 0; i < 30; ++i) {
            table.erase(i);
        }
    }

    ASSERT_TRUE(table.empty());

    // Insert again — should work after tombstone reclamation
    for (int i = 0; i < 30; ++i) {
        table.insert_or_assign(i, i * 10);
    }
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(*table.find(i), i * 10);
    }
}

TEST(SwissTableSplitTest, MoveConstructionWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table1(16, &alloc);

    for (int i = 0; i < 100; ++i) {
        table1.insert_or_assign(i, i * 10);
    }
    ASSERT_GT(alloc.split_alloc_count(), 0u);

    size_t allocs_before = alloc.alloc_count();
    SwissTable<int64_t, int64_t> table2(std::move(table1));

    // No new allocations from move
    ASSERT_EQ(alloc.alloc_count(), allocs_before);

    // table1 is empty
    ASSERT_EQ(table1.size(), 0u);
    ASSERT_EQ(table1.capacity(), 0u);

    // table2 has all data
    ASSERT_EQ(table2.size(), 100u);
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(*table2.find(i), i * 10);
    }
}

TEST(SwissTableSplitTest, MoveAssignmentWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table1(16, &alloc);
    SwissTable<int64_t, int64_t> table2(16, &alloc);

    for (int i = 0; i < 100; ++i) {
        table1.insert_or_assign(i, i);
    }
    table2.insert_or_assign(999, 999);

    table2 = std::move(table1);

    ASSERT_EQ(table2.size(), 100u);
    ASSERT_EQ(*table2.find(0), 0);
    ASSERT_EQ(*table2.find(99), 99);
    ASSERT_EQ(table2.find(999), nullptr);
}

TEST(SwissTableSplitTest, StringValuesWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, std::string> table(16, &alloc);

    for (int i = 0; i < 50; ++i) {
        table.insert_or_assign(static_cast<int64_t>(i),
            std::string("value_") + std::to_string(i));
    }

    for (int i = 0; i < 50; ++i) {
        auto* v = table.find(static_cast<int64_t>(i));
        ASSERT_NE(v, nullptr);
        ASSERT_EQ(*v, std::string("value_") + std::to_string(i));
    }

    // Erase and verify cleanup
    for (int i = 0; i < 25; ++i) {
        table.erase(static_cast<int64_t>(i));
    }
    ASSERT_EQ(table.size(), 25u);
}

TEST(SwissTableSplitTest, IterationWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    for (int i = 0; i < 100; ++i) {
        table.insert_or_assign(i, i * 10);
    }

    std::set<int64_t> keys;
    table.for_each([&keys](const int64_t& k, int64_t& v) {
        keys.insert(k);
        ASSERT_EQ(v, k * 10);
    });
    ASSERT_EQ(keys.size(), 100u);
}

TEST(SwissTableSplitTest, NoMemoryLeaks) {
    ForceSplitAllocator alloc(256);
    {
        SwissTable<int64_t, int64_t> table(16, &alloc);
        for (int i = 0; i < 500; ++i) {
            table.insert_or_assign(i, i);
        }
        // Multiple growth cycles with split allocations
        ASSERT_GT(alloc.split_alloc_count(), 0u);
    }
    // After destruction, all allocations should be freed
    ASSERT_EQ(alloc.outstanding(), 0u);
    ASSERT_EQ(alloc.total_allocated(), 0);
    ASSERT_EQ(alloc.split_alloc_count(), alloc.split_dealloc_count());
}

TEST(SwissTableSplitTest, StressSplitManyEntries) {
    ForceSplitAllocator alloc(512);
    SwissTable<int64_t, int64_t> table(16, &alloc);
    const int N = 10000;

    for (int i = 0; i < N; ++i) {
        table.insert_or_assign(i, static_cast<int64_t>(i) * 1000);
    }
    ASSERT_EQ(table.size(), static_cast<size_t>(N));

    // Verify all entries
    for (int i = 0; i < N; ++i) {
        int64_t* v = table.find(i);
        ASSERT_NE(v, nullptr);
        ASSERT_EQ(*v, static_cast<int64_t>(i) * 1000);
    }

    // Delete half, then re-insert
    for (int i = 0; i < N / 2; ++i) {
        table.erase(i);
    }
    ASSERT_EQ(table.size(), static_cast<size_t>(N / 2));

    for (int i = 0; i < N / 2; ++i) {
        table.insert_or_assign(i, static_cast<int64_t>(i) * 2000);
    }
    ASSERT_EQ(table.size(), static_cast<size_t>(N));

    for (int i = 0; i < N / 2; ++i) {
        ASSERT_EQ(*table.find(i), static_cast<int64_t>(i) * 2000);
    }
    for (int i = N / 2; i < N; ++i) {
        ASSERT_EQ(*table.find(i), static_cast<int64_t>(i) * 1000);
    }
}

TEST(SwissTableSplitTest, EmplaceWithSplit) {
    ForceSplitAllocator alloc(256);
    SwissTable<int64_t, int64_t> table(16, &alloc);

    for (int i = 0; i < 100; ++i) {
        auto [ptr, ins] = table.emplace(i, i * 10);
        ASSERT_TRUE(ins);
        ASSERT_EQ(*ptr, i * 10);
    }

    // Emplace existing keys — should not overwrite
    for (int i = 0; i < 100; ++i) {
        auto [ptr, ins] = table.emplace(i, 999);
        ASSERT_FALSE(ins);
        ASSERT_EQ(*ptr, i * 10);
    }
}
