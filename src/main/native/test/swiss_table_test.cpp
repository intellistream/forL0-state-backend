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
