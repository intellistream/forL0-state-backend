// SwissTable unit tests — covers insert/find/delete/rehash/iterate and boundary conditions.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "swiss_table.h"
#include "namespace_cache.h"
#include "type_layout.h"  // FixedRow, FixedRowHash

#include <map>
#include <set>
#include <string>
#include <vector>

using namespace forl0;

struct CountIntegralComparisons {
    static inline size_t calls = 0;
    bool operator()(int64_t left, int64_t right) const { ++calls; return left == right; }
};

struct CountWindowHashes {
    static inline size_t calls = 0;
    size_t operator()(int64_t) const { ++calls; return (15u << 7) | 17u; }
};

struct RelocationValue {
    static inline int live = 0;
    static inline int copies_left = -1;
    int value;
    explicit RelocationValue(int n) : value(n) { ++live; }
    RelocationValue(const RelocationValue& other) : value(other.value) {
        if (copies_left == 0) throw std::runtime_error("injected copy failure");
        if (copies_left > 0) --copies_left;
        ++live;
    }
    RelocationValue(RelocationValue&& other) noexcept : value(other.value) { ++live; }
    ~RelocationValue() { --live; }
};

TEST(SwissTableTest, FailedGroupedRelocationDestroysOnlyConstructedRowsAndKeepsProbeIndex) {
    for (size_t capacity : {1u, 4u, 8u, 16u, 256u}) {
        CountingAllocator target(1 << 20), index(1 << 20);
        ProbeIndexUsage usage;
        SwissTable<int64_t, RelocationValue> table(capacity, &DefaultAllocator::instance(), 0, .875, true);
        const int rows = std::max(1, static_cast<int>(capacity / 2));
        for (int i = 0; i < rows; ++i) table.emplace(i, i + 10);
        for (int i = 1; i < rows; i += 3) table.erase(i);
        if (capacity >= kGroupWidth)
            table.attach_probe_index(index.allocate(ctrl_bytes(capacity), 16), index, usage);
        const int before = static_cast<int>(table.size());
        RelocationValue::copies_left = before / 2;
        ASSERT_THROW(table.relocate(&target), std::runtime_error);
        ASSERT_EQ(RelocationValue::live, before);
        ASSERT_EQ(target.used_bytes(), 0u);
        ASSERT_EQ(table.has_probe_index(), capacity >= kGroupWidth);
        RelocationValue::copies_left = -1;
        table.relocate(&target);
        ASSERT_EQ(index.used_bytes(), 0u);
        ASSERT_EQ(RelocationValue::live, before);
        for (int i = 0; i < rows; ++i) {
            auto* found = table.find(i);
            if (i % 3 == 1) ASSERT_EQ(found, nullptr);
            else { ASSERT_NE(found, nullptr); ASSERT_EQ(found->value, i + 10); }
        }
        table.clear();
        ASSERT_EQ(RelocationValue::live, 0);
    }
}

TEST(SwissTableTest, SingletonProbesFullKeysAndExpandsWithoutLosingTheFirstRow) {
    SwissTable<int64_t, int64_t> table(1, &DefaultAllocator::instance(), 0, .875, true);
    ASSERT_EQ(table.capacity(), 1u);
    ASSERT_EQ(table.packed_backing_bytes(), 32u);
    ASSERT_TRUE(table.find(101) == nullptr);
    table.insert_or_assign(101, 1);
    ASSERT_TRUE(table.find(98) == nullptr);
    ASSERT_EQ(*table.find(101), 1);
    ASSERT_TRUE(table.install_membership(DefaultAllocator::instance(), 64));
    bool rejected = false;
    ASSERT_EQ(*table.find_with_membership(101, rejected), 1);
    for (int64_t i = 102; i < 1200; ++i) ASSERT_TRUE(table.find_with_membership(i, rejected) == nullptr);
    table.relocate(&DefaultAllocator::instance());
    ASSERT_EQ(*table.find(101), 1);
    ASSERT_TRUE(table.erase(101));
    table.emplace(99, 4);
    table.emplace(100, 5);
    ASSERT_GE(table.capacity(), 4u);
    ASSERT_EQ(*table.find(99), 4);
    ASSERT_EQ(*table.find(100), 5);
}

TEST(SwissTableTest, ResidentControlViewFollowsBackingAndNamespaceOwnership) {
    ResidentControls view{}, second{};
    SwissTable<int64_t, int64_t> first(4, &DefaultAllocator::instance(), 0, .875, true);
    first.publish_controls(view, 2, 10, 0);
    ASSERT_EQ(view.address, reinterpret_cast<uintptr_t>(first.ctrl_data()));
    view.slots = reinterpret_cast<uintptr_t>(first.slot_data());
    view.slot_layout = ResidentControls::writable_bit | 16;
    first.publish_controls(second, 3, 10, 20);
    ASSERT_EQ(view.address, 0u);
    ASSERT_EQ(view.slots, 0u);
    ASSERT_EQ(view.slot_layout, 0u);
    for (int i = 0; i < 50; ++i) first.emplace(i, -i);
    ASSERT_EQ(second.address, 0u);
    first.publish_controls(view, 3, 10, 20);
    SwissTable<int64_t, int64_t> next(std::move(first));
    ASSERT_EQ(view.address, 0u);
    next.publish_controls(view, 2, 23, 0);
    {
        SwissTable<int64_t, int64_t> other;
        other.publish_controls(view, 2, 24, 0);
        next.withdraw_controls();
        ASSERT_EQ(view.address, reinterpret_cast<uintptr_t>(other.ctrl_data()));
    }
    ASSERT_EQ(view.address, 0u);
    for (int i = 0; i < 50; ++i) ASSERT_EQ(*next.find(i), -i);
}

TEST(SwissTableTest, PairedPositionsRespectFullHashCollisionsAndStructuralChanges) {
    using Table = SwissTable<int64_t, int64_t, CountWindowHashes>;
    ResidentControls view;
    Table table(32), other(32);
    table.publish_controls(view, 2, -5, 0);
    // Both keys have exactly the same H1 and H2. At this point the group is empty.
    view.absent_hash = CountWindowHashes{}(1);
    view.edit_position = ~int64_t(15);
    ASSERT_TRUE(table.emplace(1, 101).second);
    ASSERT_EQ(view.paired_inserts, 1u);
    auto slot = table.position_of(*table.find(1));
    ASSERT_EQ(slot, 15u);
    view.edit_position = slot + 1;
    ASSERT_EQ(table.take_read_slot(2), nullptr);
    ASSERT_TRUE(table.emplace(2, 202).second);
    view.edit_position = slot + 1;
    ASSERT_EQ(*table.take_read_slot(1), 101);
    ASSERT_EQ(view.paired_updates, 1u);
    table.clear();
    view.absent_hash = CountWindowHashes{}(3);
    view.edit_position = ~int64_t(15);
    // A colliding insertion consumes the miss, even if the following key differs.
    table.emplace(9, 909);
    ASSERT_EQ(view.edit_position, 0);
    table.insert_or_assign(3, 303);
    ASSERT_EQ(*table.find(9), 909);
    ASSERT_EQ(*table.find(3), 303);
    view.edit_position = table.position_of(*table.find(9)) + 1;
    ASSERT_TRUE(table.erase(9));
    ASSERT_EQ(view.edit_position, 0);
    view.edit_position = table.position_of(*table.find(3)) + 1;
    other.publish_controls(view, 2, 6, 0);
    ASSERT_EQ(view.edit_position, 0);
    other.emplace(3, -303);
    view.edit_position = other.position_of(*other.find(3)) + 1;
    ASSERT_EQ(table.take_read_slot(3), nullptr);
    ASSERT_EQ(*other.take_read_slot(3), -303);
    for (int i = 10; i < 80; ++i) other.emplace(i, i);
    ASSERT_EQ(view.address, 0u);
    ASSERT_EQ(*table.find(3), 303);
}

TEST(SwissTableTest, LiveSlotScansUseTheSameOrderAndMarksWithHolesAndProbeCopies) {
    CountingAllocator pool(1 << 20);
    for (size_t capacity : {1u, 4u, 8u, 16u, 64u, 256u}) {
        for (bool copy : {false, true}) {
            ProbeIndexUsage usage;
            SwissTable<int64_t, int64_t> table(capacity, &DefaultAllocator::instance(), 0, .875, true);
            const size_t entries = std::max<size_t>(1, capacity * 3 / 4);
            for (size_t i = 0; i < entries; ++i) table.emplace(i, i + 333);
            if (copy) table.attach_probe_index(pool.allocate(ctrl_bytes(capacity), 16), pool, usage);
            for (size_t i = 1; i < entries; i += 3) table.erase(i);
            std::vector<int64_t> expected, seen;
            size_t dirty = 0;
            const uint64_t marks = UINT64_C(0x9bde33124ab907f8);
            for (size_t i = 0; i < table.capacity(); ++i) {
                if (table.ctrl_data()[i] < 0) continue;
                expected.push_back(table.slot_data()[i].first);
                dirty += (marks >> (i % 64)) & 1;
            }
            const auto& frozen = table;
            frozen.for_each([&](int64_t key, int64_t value) { ASSERT_EQ(value, key + 333); seen.push_back(key); });
            ASSERT_TRUE(expected == seen);
            seen.clear();
            size_t dirty_seen = 0;
            auto current = [&](int64_t key, int64_t) { seen.push_back(key); };
            auto changed = [&](int64_t key, int64_t value) { ++dirty_seen; current(key, value); };
            ASSERT_EQ(frozen.for_each_marked(&marks, 0, current, changed), dirty);
            ASSERT_EQ(dirty_seen, dirty);
            ASSERT_TRUE(expected == seen);
        }
    }
}

TEST(SwissTableTest, PublishedCompactSlotsFollowTheirOwningNamespace) {
    ResidentControls view{};
    using Table = SwissTable<int64_t, int64_t>;
    Table first(8, &DefaultAllocator::instance(), 0, .875, true);
    Table other(4, &DefaultAllocator::instance(), 0, .875, true);
    auto check = [&](const Table& table) {
        uint64_t expected = ResidentControls::compact_slots_tag;
        for (size_t i = 0; i < table.capacity(); ++i) {
            if (table.ctrl_data()[i] >= 0) expected |= uint64_t{1} << (32 + i);
        }
        ASSERT_EQ(view.last_root_row, expected);
    };
    first.emplace(1, 11);
    first.publish_controls(view, 2, 10, 0);
    check(first);
    for (int i = 2; i <= 6; ++i) { first.emplace(i, i); check(first); }
    first.erase(3);
    check(first);
    other.publish_controls(view, 2, 11, 0);
    other.emplace(9, 90);
    check(other);
    first.clear();
    check(other);
    first.publish_controls(view, 2, 10, 0);
    check(first);
    first.emplace(7, 77);
    check(first);
    first.clear();
    check(first);
    for (int i = 0; i < 16; ++i) first.emplace(i, i);
    ASSERT_EQ(view.address, 0u);
}

TEST(SwissTableTest, ActiveProbeControlsSurviveIndexReclamationAndRetireAtResize) {
    CountingAllocator index_pool(1 << 20);
    ProbeIndexUsage usage;
    SwissTable<int64_t, int64_t> original(64);
    ResidentControls view{};
    for (int i = 0; i < 32; ++i) original.emplace(i, -i);
    const auto* dram_controls = original.ctrl_data();
    const size_t bytes = ctrl_bytes(original.capacity());
    const std::vector<int8_t> before(dram_controls, dram_controls + bytes);
    original.attach_probe_index(index_pool.allocate(bytes, 16), index_pool, usage);
    original.publish_controls(view, 1, 0, 0);
    ASSERT_EQ(view.address, reinterpret_cast<uintptr_t>(original.ctrl_data()));
    ASSERT_NE(original.ctrl_data(), dram_controls);
    SwissTable<int64_t, int64_t> table(std::move(original));
    ASSERT_EQ(view.address, 0u);
    ASSERT_EQ(usage.bytes, bytes);
    for (int i = 0; i < 32; ++i) {
        ASSERT_TRUE(table.erase(i));
        ASSERT_EQ(table.find(i), nullptr);
        table.emplace(i + 100, i);
        ASSERT_EQ(*table.find(i + 100), i);
    }
    ASSERT_EQ(std::memcmp(dram_controls, before.data(), bytes), 0);
    const std::vector<int8_t> after(table.ctrl_data(), table.ctrl_data() + bytes);
    table.publish_controls(view, 1, 0, 0);
    table.drop_probe_index();
    ASSERT_EQ(view.address, 0u);
    ASSERT_EQ(table.ctrl_data(), dram_controls);
    ASSERT_EQ(std::memcmp(dram_controls, after.data(), bytes), 0);
    ASSERT_EQ(usage.bytes, 0u);
    for (int i = 0; i < 32; ++i) {
        ASSERT_EQ(table.find(i), nullptr);
        ASSERT_EQ(*table.find(i + 100), i);
    }

    table.attach_probe_index(index_pool.allocate(bytes, 16), index_pool, usage);
    table.clear();
    ASSERT_TRUE(table.has_probe_index());
    ASSERT_TRUE(table.empty());
    ASSERT_EQ(table.find(100), nullptr);
    table.publish_controls(view, 1, 0, 0);
    for (int i = 0; i < 200; ++i) table.emplace(i, i * 3);
    ASSERT_EQ(usage.bytes, 0u);
    ASSERT_EQ(usage.releases, 2u);
    ASSERT_EQ(view.address, 0u);
    ASSERT_EQ(index_pool.used_bytes(), 0u);
    for (int i = 0; i < 200; ++i) ASSERT_EQ(*table.find(i), i * 3);
}

TEST(SwissTableTest, CompactLiveSlotsSurviveRelocationCollisionsAndRecycling) {
    using Table = SwissTable<int64_t, int64_t, CountWindowHashes>;
    CountingAllocator other(1 << 20);
    Table table(4, &DefaultAllocator::instance(), 0, .875, true);
    table.emplace(11, 21);
    table.emplace(12, 22);
    table.relocate(&other);
    table.emplace(13, 23);
    ASSERT_TRUE(table.erase(11));
    CountWindowHashes::calls = 0;
    ASSERT_EQ(*table.find(13), 23);
    ASSERT_EQ(table.find(99), nullptr);
    ASSERT_EQ(*table.insert_or_assign(12, 122).first, 122);
    ASSERT_FALSE(table.emplace(12, 0).second);
    ASSERT_EQ(CountWindowHashes::calls, 0u);
    ASSERT_TRUE(table.erase(13));
    for (int64_t k = 100; k < 2000; ++k) {
        table.emplace(k, k + 1);
        ASSERT_EQ(*table.find(k), k + 1);
        ASSERT_TRUE(table.erase(k));
    }
    ASSERT_EQ(table.capacity(), 4u);
    for (int64_t k = 100; k < 143; ++k) table.emplace(k, k + 1);
    ASSERT_TRUE(table.capacity() > kGroupWidth);
    for (int64_t k = 100; k < 143; ++k) ASSERT_EQ(*table.find(k), k + 1);
    ASSERT_EQ(*table.find(12), 122);
    table.clear();
    ASSERT_EQ(table.find(12), nullptr);
}

TEST(SwissTableTest, SingleGroupRecyclesWrappedSlotsAtConfiguredCapacity) {
    SwissTable<int64_t, int64_t, CountWindowHashes> table(16, &DefaultAllocator::instance(), 16);
    for (int64_t k = 0; k < 14; ++k) table.emplace(k, k);
    for (int64_t k = 0; k < 4096; ++k) {
        ASSERT_TRUE(table.erase(k));
        table.emplace(k + 14, k + 14);
        ASSERT_EQ(*table.find(k + 1), k + 1);
        ASSERT_EQ(*table.find(k + 14), k + 14);
        ASSERT_EQ(table.find(k), nullptr);
    }
    ASSERT_EQ(table.capacity(), 16u);
    ASSERT_EQ(table.size(), 14u);
}

TEST(SwissTableTest, IntegralHighBitsDoNotCollapseIntoOneProbeChain) {
    constexpr int64_t count = 4096;
    SwissTable<int64_t, int64_t, SwissKeyHash<int64_t>, CountIntegralComparisons> table(count * 2);
    for (int64_t i = 1; i <= count; ++i) table.insert_or_assign(i << 32, i);
    CountIntegralComparisons::calls = 0;
    for (int64_t i = 1; i <= count; ++i) {
        auto* value = table.find(i << 32);
        ASSERT_NE(value, nullptr);
        ASSERT_EQ(*value, i);
        ASSERT_EQ(table.find((i + count) << 32), nullptr);
    }
    // Identity hashing needs millions of comparisons for this valid key shape.
    // Bound comparisons rather than time so the regression is machine-independent.
    ASSERT_LT(CountIntegralComparisons::calls, size_t(count * 8));
}

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

TEST(SwissTableTest, SerializedStringViewsSurviveGrowthAndTombstones) {
    SwissTable<std::string, int64_t> table;
    std::vector<std::string> keys;
    for (int64_t i = 0; i < 4096; ++i) {
        std::string key(36, '\0');
        std::memcpy(key.data() + 4, &i, sizeof(i));
        key.back() = static_cast<char>(i);
        table.insert_or_assign(key, i);
        keys.push_back(std::move(key));
    }
    for (int64_t i = 0; i < 4096; i += 3) table.erase(keys[i]);
    for (int64_t i = 0; i < 4096; ++i) {
        // Views need neither a terminator nor an allocation of their own.
        std::string padded = "prefix" + keys[i] + "suffix";
        std::string_view key(padded.data() + 6, keys[i].size());
        auto* value = table.find_string_view(key);
        ASSERT_EQ(value, table.find(keys[i]));
        if (i % 3) { ASSERT_NE(value, nullptr); ASSERT_EQ(*value, i); }
        else ASSERT_EQ(value, nullptr);
        ASSERT_EQ(table.find_string_view(key.substr(0, key.size() - 1)), nullptr);
    }
    table.insert_or_assign(std::string(), -1);
    ASSERT_EQ(*table.find_string_view(std::string_view()), -1);
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

namespace {
struct CollidingMembershipHash {
    size_t operator()(int key) const noexcept { return static_cast<size_t>(key & 1023); }
};
}

TEST(SwissTableTest, NamespaceMembershipIsConservativeAcrossGrowthCollisionEraseAndMove) {
    CountingAllocator index(4096);
    SwissTable<int, int, CollidingMembershipHash> table(4, &DefaultAllocator::instance(), 0, .875, true);
    for (int i = 0; i < 50; ++i) table.insert_or_assign(i, i + 1);
    ASSERT_TRUE(table.install_membership(index, 2048));
    size_t rejected = 0;
    for (int i = 100; i < 400; ++i) {
        bool absent = false;
        ASSERT_EQ(table.find_with_membership(i, absent), nullptr);
        rejected += absent;
        table.emplace(i, i + 1);
        ASSERT_EQ(*table.find_with_membership(i, absent), i + 1);
        ASSERT_FALSE(absent);
    }
    ASSERT_GT(rejected, 200u);
    bool absent = true;
    ASSERT_EQ(table.find_with_membership(1024 + 12, absent), nullptr);
    ASSERT_FALSE(absent);
    table.insert_or_assign(1024 + 12, 117);
    ASSERT_EQ(*table.find_with_membership(1024 + 12, absent), 117);
    ASSERT_TRUE(table.erase(12));
    ASSERT_EQ(table.find_with_membership(12, absent), nullptr);
    ASSERT_EQ(*table.find_with_membership(1024 + 12, absent), 117);
    auto moved = std::move(table);
    ASSERT_EQ(*moved.find_with_membership(115, absent), 116);
    moved.clear();
    ASSERT_EQ(moved.find_with_membership(115, absent), nullptr);
    ASSERT_TRUE(absent);
    moved.emplace(115, 9);
    ASSERT_EQ(*moved.find_with_membership(115, absent), 9);
}

TEST(SwissTableTest, ProvenAbsentHashCanPublishOnlyUntilAnotherKeyWithThatHashIsInserted) {
    CountingAllocator index(4096);
    SwissTable<int, int, CollidingMembershipHash> table(16);
    ASSERT_TRUE(table.install_membership(index, 1024));
    bool rejected = false;
    ASSERT_EQ(table.find_with_membership(12, rejected), nullptr);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(table.emplace(12 + 1024, 71).second);
    ASSERT_EQ(table.membership_write_skips(), 1u);
    ASSERT_EQ(table.find_with_membership(12, rejected), nullptr);
    ASSERT_FALSE(rejected);
    ASSERT_TRUE(table.insert_or_assign(12, 19).second);
    ASSERT_FALSE(table.insert_or_assign(12 + 1024, 29).second);
    ASSERT_EQ(table.size(), 2u);
    ASSERT_EQ(*table.find(12), 19);
    ASSERT_EQ(*table.find(12 + 1024), 29);

    ASSERT_EQ(table.find_with_membership(15, rejected), nullptr);
    ASSERT_TRUE(rejected);
    table.emplace(30, 8);
    table.emplace(15, 6);
    ASSERT_EQ(table.membership_write_skips(), 1u);
    ASSERT_EQ(*table.find(15), 6);
    table.clear();
    ASSERT_TRUE(table.emplace(12, 5).second);
    ASSERT_EQ(table.size(), 1u);
}

TEST(SwissTableTest, ProvenMissingKeysPrepareAWriteOnlyAfterRepeatedPairs) {
    CountingAllocator index(32768);
    SwissTable<int, int, CollidingMembershipHash> table(256);
    ASSERT_TRUE(table.install_membership(index, 16384));
    bool absent = false;
    for (int i = 0; i < 96; ++i) {
        ASSERT_EQ(table.find_with_membership(i, absent), nullptr);
        ASSERT_EQ(table.prepared_writes(), 0u);
    }
    for (int i = 100; i < 380; ++i) {
        ASSERT_EQ(table.find_with_membership(i, absent), nullptr);
        ASSERT_TRUE(table.emplace(i, i * 2).second);
    }
    ASSERT_GT(table.capacity(), 256u);
    const auto prepared = table.prepared_writes();
    ASSERT_GT(prepared, 70u);
    // A different write, even with the same hash, consumes the prepared position.
    ASSERT_EQ(table.find_with_membership(500, absent), nullptr);
    ASSERT_TRUE(absent);
    ASSERT_TRUE(table.emplace(1524, 1).second);
    ASSERT_TRUE(table.emplace(500, 2).second);
    ASSERT_EQ(*table.find(1524), 1);
    ASSERT_EQ(*table.find(500), 2);
    ASSERT_EQ(table.find_with_membership(501, absent), nullptr);
    ASSERT_TRUE(table.emplace(502, 3).second);
    ASSERT_TRUE(table.emplace(501, 4).second);
    for (int i = 100; i < 380; ++i) ASSERT_EQ(*table.find(i), i * 2);
    table.clear();
    ASSERT_EQ(table.find(500), nullptr);
    ASSERT_TRUE(table.emplace(500, 5).second);
    ASSERT_EQ(*table.find(500), 5);
}

TEST(SwissTableTest, RepeatingAnAbsenceSurvivesOtherInsertsAndExpiresOnFullHashCollision) {
    auto manager = std::make_shared<HotCacheManager>(256 * sizeof(HotSet),
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena pool(manager, 256 * sizeof(HotSet));
    RootMembershipView view;
    SwissTable<int, int, CollidingMembershipHash> table(256);
    ASSERT_TRUE(table.install_membership(pool, 4096));
    table.share_membership(view, pool.pool_base());
    ASSERT_NE(view.word_mask & RootMembershipView::supports_repeated_absence, 0u);
    ASSERT_EQ(view.word_mask & RootMembershipView::valid_absence, 0u);
    view.last_absent_hash = 7;
    view.word_mask |= RootMembershipView::valid_absence;
    ++view.skipped_reads;
    ASSERT_TRUE(table.emplace(8, 5).second);
    ASSERT_NE(view.word_mask & RootMembershipView::valid_absence, 0u);
    ++view.skipped_reads;
    ASSERT_TRUE(table.emplace(1031, 42).second);
    ASSERT_EQ(view.word_mask & RootMembershipView::valid_absence, 0u);
    ASSERT_TRUE(table.emplace(7, 17).second);
    ASSERT_EQ(*table.find(1031), 42);
    ASSERT_EQ(*table.find(7), 17);
    table.drop_membership();
    ASSERT_EQ(view.bitmap_offset_plus_one, 0u);
}

TEST(SwissTableTest, SharedNegativeProofExpiresOnPublicationDropAndRelocation) {
    auto manager = std::make_shared<HotCacheManager>(256 * sizeof(HotSet),
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena data(manager, 256 * sizeof(HotSet));
    RootMembershipView view;
    SwissTable<int, int, CollidingMembershipHash> table(256);
    ASSERT_TRUE(table.install_membership(data, 4096));
    const void* pool = data.pool_base();
    table.share_membership(view, pool);
    ASSERT_NE(view.bitmap_offset_plus_one, 0u);
    // Simulate a task-thread check for an empty filter. Equal full hashes
    // must still resolve separately after another key is published.
    view.last_absent_hash = 7;
    ++view.skipped_reads;
    ASSERT_TRUE(table.emplace(1031, 99).second);
    ASSERT_TRUE(table.emplace(7, 70).second);
    ASSERT_FALSE(table.emplace(1031, 0).second);
    ASSERT_EQ(*table.find(7), 70);
    ASSERT_EQ(*table.find(1031), 99);
    table.enable_shared_membership(false);
    ASSERT_EQ(view.bitmap_offset_plus_one, 0u);
    table.enable_shared_membership(true);
    ASSERT_NE(view.bitmap_offset_plus_one, 0u);
    table.relocate(&data);
    ASSERT_EQ(view.bitmap_offset_plus_one, 0u);
    ASSERT_EQ(*table.find(1031), 99);
    ASSERT_TRUE(table.install_membership(data, 1024));
    table.share_membership(view, pool);
    table.drop_membership();
    ASSERT_EQ(view.bitmap_offset_plus_one, 0u);
}
