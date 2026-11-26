// TimeWindow namespace tests — verifies TW namespace operations in StateTable.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "state_engine.h"

#include <map>
#include <vector>

using namespace forl0;

TEST(TimeWindowNamespaceTest, DemotedPayloadPagesReturnIndependentlyAndPreserveAlignment) {
    CountingAllocator memory(1024 * 1024);
    NamespaceDramAllocator dram(&memory);
    constexpr size_t count = 200;
    std::array<void*, count> blocks{};
    std::array<size_t, count> sizes{};
    std::array<uintptr_t, count> page_numbers{};
    std::map<uintptr_t, size_t> pages;
    dram.pack_copy([&] {
        for (size_t i = 0; i < count; ++i) {
            sizes[i] = 80 + i % 13;
            const size_t alignment = size_t{8} << (i % 4);
            blocks[i] = dram.allocate(sizes[i], alignment);
            const auto address = reinterpret_cast<uintptr_t>(blocks[i]);
            ASSERT_EQ(address % alignment, 0u);
            page_numbers[i] = address / 4096;
            ++pages[page_numbers[i]];
            std::memset(blocks[i], static_cast<int>(i), sizes[i]);
        }
    });
    ASSERT_TRUE(pages.size() > 2 && pages.size() < count / 8);
    void* ordinary = dram.allocate(56, 8);
    const size_t initial = memory.used_bytes();
    const auto first_page = page_numbers.front();
    for (size_t i = count; i-- > 0;) {
        if (page_numbers[i] == first_page) continue;
        ASSERT_EQ(static_cast<unsigned char*>(blocks[i])[sizes[i] - 1], i);
        dram.deallocate(blocks[i], sizes[i]);
    }
    ASSERT_TRUE(initial - memory.used_bytes() >= (pages.size() - 1) * 4096);
    for (size_t i = 0; i < count; ++i) {
        if (page_numbers[i] == first_page)
            dram.deallocate(blocks[i], sizes[i]);
    }
    ASSERT_EQ(memory.used_bytes(), 56u);
    dram.deallocate(ordinary, 56);
    ASSERT_EQ(memory.used_bytes(), 0u);
    ASSERT_EQ(dram.bytes(), 0u);
}

TEST(TimeWindowNamespaceTest, DemotionCanUseIndividualAllocationsUnderTheNativeLimit) {
    CountingAllocator memory(96);
    NamespaceDramAllocator dram(&memory);
    ASSERT_THROW(dram.pack_copy([&] {
        std::pmr::string owned(52, 'a', &dram);
        ASSERT_EQ(owned.size(), 52u);
        std::pmr::string exceeds_limit(72, 'b', &dram);
    }), std::bad_alloc);
    ASSERT_EQ(memory.used_bytes(), 0u);
    ASSERT_EQ(dram.bytes(), 0u);
    void* small = dram.allocate(56, 8);
    ASSERT_EQ(memory.used_bytes(), 56u);
    dram.deallocate(small, 56);
}

TEST(TimeWindowNamespaceTest, OrdinaryAllocationsOutliveSuccessiveWholeTableCopies) {
    CountingAllocator memory(1024 * 1024);
    NamespaceDramAllocator dram(&memory);
    auto* original = dram.allocate(80, 16);
    for (unsigned round = 0; round < 3; ++round) {
        void *first = nullptr, *second = nullptr;
        dram.pack_copy([&] { first = dram.allocate(128, 16); });
        auto* ordinary = dram.allocate(88, 16);
        ASSERT_THROW(dram.pack_copy([&] {
            std::pmr::string partial(96, 'z', &dram);
            throw std::runtime_error("copy stopped");
        }), std::runtime_error);
        dram.pack_copy([&] { second = dram.allocate(96, 32); });
        std::memset(first, 7, 128);
        dram.deallocate(second, 96);
        ASSERT_EQ(static_cast<char*>(first)[127], 7);
        dram.deallocate(first, 128);
        ASSERT_EQ(memory.used_bytes(), 80u + 88u);
        dram.deallocate(ordinary, 88);
    }
    dram.deallocate(original, 80);
    ASSERT_EQ(dram.bytes(), 0u);
    ASSERT_EQ(memory.used_bytes(), 0u);
}

TEST(TimeWindowNamespaceTest, GrowthUsesTheMutatingTableAfterReadsSwitchGroups) {
    constexpr size_t primary = 64 * 1024;
    for (bool negative_only : {false, true}) {
        auto manager = std::make_shared<HotCacheManager>(16 * primary, [](L0LibBindings*, std::string*) { return false; });
        StateTable<int64_t, NamespaceRow> state(0, 2, true);
        state.enable_namespace_storage(manager, primary);
        auto* views = state.resident_control_views();
        state.put(0, 7, NamespaceRow("hot", 3));
        state.put(1, 7, NamespaceRow("neighbor", 8));
        const auto* arena = state.namespace_arena();
        if (negative_only) {
            ASSERT_EQ(state.get(0, 90), nullptr);
            ASSERT_NE(views[0].address, 0u);
            views[0].skipped += 128;  // Exact misses published by the owning task thread.
        } else {
            for (int i = 0; i < 64; ++i) ASSERT_NE(state.get(0, 7), nullptr);
        }
        const auto* neighbor = state.get(1, 7);
        ASSERT_TRUE(arena->contains(neighbor));
        ASSERT_TRUE(state.modify_in_place(0, int64_t{7}, [&](auto& row) {
            row.resize(primary + 2048);
            std::memcpy(row.data(), neighbor->data(), neighbor->size());
        }));
        const auto* hot = state.get(0, 7);
        ASSERT_TRUE(arena->contains(hot));
        ASSERT_TRUE(arena->contains(hot->data()));
        ASSERT_TRUE(std::string_view(hot->data(), 8) == "neighbor");
        ASSERT_TRUE(arena->contains(state.get(1, 7)));
        const auto stats = state.namespace_cache_stats();
        ASSERT_GT(stats.interleaved_growth_loans, 0u);
        ASSERT_EQ(stats.negative_growth_loans > 0, negative_only);
    }
}

TEST(TimeWindowNamespaceTest, NumericWindowListPayloadStaysWithTheTableAndPreservesBorrowedAppends) {
    constexpr size_t budget = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(2 * budget, [](L0LibBindings*, std::string*) { return false; });
    StateTable<WindowStateKey, ElementList> state(0, 1, false);
    state.enable_namespace_storage(manager, budget);
    const WindowStateKey key{17, 0};
    state.update_or_create(0, int64_t{11}, key, [](auto& list) { list.append("first-element"); });
    auto* list = state.get(0, int64_t{11}, key);
    ASSERT_NE(list, nullptr);
    const auto* arena = state.namespace_arena();
    ASSERT_TRUE(arena->contains(list->storage().data()));
    state.prepare_snapshot();
    for (unsigned i = 0; i < 64; ++i) {
        state.update_or_create(0, int64_t{11}, key, [](auto& value) { value.append(value.payload().substr(0, 13)); });
    }
    list = state.get(0, int64_t{11}, key);
    ASSERT_EQ(list->size(), 65u);
    ASSERT_TRUE(arena->contains(list->storage().data()));
    ASSERT_TRUE(list->payload().substr(13) == std::string_view(list->payload()).substr(0, 64 * 13));
    state.for_each_snapshot_in_key_group_ns<int64_t>(0, [&](int64_t ns, const auto& k, const auto& frozen) {
        ASSERT_EQ(ns, 11);
        ASSERT_TRUE(k == key);
        ASSERT_TRUE(frozen.payload() == "first-element");
    });
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, FrozenMutationsPreserveErasedReinsertedAndFailedRows) {
    constexpr size_t budget = 256 * sizeof(HotSet);
    const TimeWindow window{10, 20};
    for (bool cached : {false, true}) {
        auto manager = std::make_shared<HotCacheManager>(2 * budget,
                [](L0LibBindings*, std::string*) { return false; });
        StateTable<int64_t, NamespaceBlob> state(0, 2, true);
        if (cached) state.enable_namespace_storage(manager, budget);
        for (int g = 0; g < 2; ++g) {
            for (int64_t k = 0; k < 60; ++k) {
                state.put(g, k, NamespaceBlob(40 + k, char('a' + g)));
                state.put(g, window, k, NamespaceBlob(60 + k, char('A' + g)));
            }
        }
        state.prepare_snapshot();
        for (int g = 0; g < 2; ++g) {
            for (int64_t k = 0; k < 90; ++k) {
                auto grow = [](auto& row) { row.assign(128, 'X'); };
                state.get(g, k);
                state.modify_in_place(g, k, grow);
                state.get(g, window, k);
                state.modify_in_place(g, window, k, grow);
                state.update_or_create(g, k, grow);
                state.update_or_create(g, window, k, grow);
                if (k % 2 == 0) {
                    state.remove(g, k);
                    state.modify_or_remove_in_place(g, window, k, [](auto& v) { v.clear(); });
                    auto fail = [](auto& row) { row.assign(49, 'F'); throw std::runtime_error("abort insert"); };
                    ASSERT_THROW(state.update_or_create(g, k, fail), std::runtime_error);
                    ASSERT_THROW(state.update_or_create(g, window, k, fail), std::runtime_error);
                    ASSERT_EQ(state.get(g, k), nullptr);
                    ASSERT_EQ(state.get(g, window, k), nullptr);
                    state.put(g, k, NamespaceBlob(48, 'r'));
                    state.put(g, window, k, NamespaceBlob(64, 'R'));
                    if (k % 4 == 0) {
                        state.modify_or_remove_in_place(g, k, [](auto& v) { v.clear(); });
                        state.remove(g, window, k);
                    }
                }
            }
        }
        for (int g = 0; g < 2; ++g) {
            std::map<int64_t, NamespaceBlob> root, named;
            state.for_each_snapshot_in_key_group(g, [&](int64_t k, const auto& v) {
                ASSERT_TRUE(root.emplace(k, v).second);
            });
            state.for_each_snapshot_in_key_group_ns<TimeWindow>(g, [&](const auto& ns, int64_t k, const auto& v) {
                ASSERT_TRUE(ns == window);
                ASSERT_TRUE(named.emplace(k, v).second);
            });
            ASSERT_EQ(root.size(), 60u);
            ASSERT_EQ(named.size(), 60u);
            for (int64_t k = 0; k < 60; ++k) {
                ASSERT_TRUE(root.at(k) == NamespaceBlob(40 + k, char('a' + g)));
                ASSERT_TRUE(named.at(k) == NamespaceBlob(60 + k, char('A' + g)));
            }
        }
        state.release_snapshot();
    }
}

TEST(TimeWindowNamespaceTest, WholeTableResidencyAndEvictionPreserveSnapshot) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto unavailable = [](L0LibBindings*, std::string*) { return false; };
    auto manager = std::make_shared<HotCacheManager>(quota * 2, unavailable);
    StateTable<WindowStateKey, NamespaceBlob> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    ASSERT_NE(arena, nullptr);
    const auto start = reinterpret_cast<uintptr_t>(arena->data());
    auto resident = [&](const void* ptr) {
        const auto address = reinterpret_cast<uintptr_t>(ptr);
        return address >= start && address - start < arena->capacity();
    };

    const WindowStateKey first{7, 0}, nullable{7, 256};
    // Leave room for two populated windows, with less than an empty backing
    // left over. Slot sizes differ between standard library implementations.
    const size_t backing = SwissTable<WindowStateKey, NamespaceBlob>::initial_backing_bytes(16, false);
    ASSERT_LT(2 * backing, quota);
    const NamespaceBlob row((quota - 2 * backing) / 5, 'a');
    const TimeWindow first_window{10, 20}, second_window{20, 30}, third_window{30, 40};
    ASSERT_EQ(state.get(0, first_window, first), nullptr);
    auto populate = [&](int group, const TimeWindow& window) {
        state.put(group, window, first, row);
        state.put(group, window, nullable, row);
    };
    populate(0, first_window);
    populate(1, second_window);
    auto* first_slot = state.get(0, first_window, first);
    ASSERT_NE(first_slot, nullptr);
    ASSERT_TRUE(resident(first_slot));
    ASSERT_TRUE(resident(first_slot->data()));
    ASSERT_TRUE(resident(state.get(0, first_window, nullable)->data()));
    ASSERT_NE(state.get(1, second_window, first), nullptr);
    ASSERT_EQ(state.get(0, first_window, first), first_slot);

    state.prepare_snapshot();
    populate(0, third_window);
    ASSERT_EQ(state.namespace_cache_stats().evictions, 0u);
    ASSERT_EQ(state.namespace_cache_stats().admission_deferrals, 1u);
    ASSERT_FALSE(resident(state.get(0, third_window, first)->data()));
    for (int i = 0; i < 16; ++i) {
        ASSERT_TRUE(*state.get(0, third_window, first) == row);
        state.put(0, third_window, first, row);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_FALSE(resident(state.get(1, second_window, first)->data()));
    ASSERT_TRUE(resident(state.get(0, first_window, first)->data()));
    ASSERT_TRUE(resident(state.get(0, third_window, nullable)->data()));
    const NamespaceBlob oversized(quota * 2, 'b');
    state.put(0, first_window, first, oversized);
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 1u);
    ASSERT_FALSE(resident(state.get(0, first_window, nullable)->data()));
    ASSERT_TRUE(*state.get(0, first_window, first) == oversized);
    ASSERT_TRUE(*state.get(1, second_window, nullable) == row);

    size_t original_count = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0, [&](const auto& ns, const auto&, const auto& value) {
        ASSERT_TRUE(ns == first_window);
        ASSERT_TRUE(value == row);
        ++original_count;
    });
    ASSERT_EQ(original_count, 2u);
    state.release_snapshot();
    ASSERT_TRUE(state.remove(0, third_window, first));
    ASSERT_TRUE(state.remove(0, third_window, nullable));
    ASSERT_EQ(state.get(0, third_window, first), nullptr);
    ASSERT_EQ(arena->free_bytes(), arena->capacity());
}

TEST(TimeWindowNamespaceTest, WholeTableOwnsControlsKeysAndValuesWithTransactionalRelocation) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    SwissTable<NamespaceBlob, NamespaceBlob> table;
    const NamespaceBlob key(240, 'k'), row(160, 'r'), oversized(quota * 2, 'x');
    table.insert_or_assign(key, oversized);
    ASSERT_THROW(table.relocate(&arena), NamespaceCacheFull);
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
    ASSERT_TRUE(*table.find(key) == oversized);
    table.insert_or_assign(key, row);
    table.relocate(&arena);
    ASSERT_TRUE(arena.contains(table.ctrl_data()));
    ASSERT_TRUE(arena.contains(table.slot_data()));
    ASSERT_TRUE(*table.find(key) == row);
    table.for_each([&](const NamespaceBlob& stored_key, const NamespaceBlob& stored_row) {
        ASSERT_TRUE(arena.contains(stored_key.data()));
        ASSERT_TRUE(arena.contains(stored_row.data()));
    });
    table.relocate(&DefaultAllocator::instance());
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
    ASSERT_TRUE(*table.find(key) == row);
}

TEST(TimeWindowNamespaceTest, StableSmallWindowsUseOneCompactProbeGroup) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false, &DefaultAllocator::instance(), 64);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t time = 0; time < 4; ++time) {
        TimeWindow window{time,time+1};
        state.put(0, window, 0, time);
        ASSERT_TRUE(state.remove(0, window, 0));
    }
    const TimeWindow window{10, 20};
    for (int64_t key = 0; key < 3; ++key) state.put(0, window, key, key * 2);
    ASSERT_EQ(arena->free_bytes(), quota - 96);
    ASSERT_EQ(state.get(0, window, 10), nullptr);
    ASSERT_TRUE(state.remove(0, window, 1));
    ASSERT_EQ(state.get(0, window, 1), nullptr);
    state.put(0, window, 3, 6);
    state.put(0, window, 4, 8);
    ASSERT_EQ(arena->free_bytes(), quota - 160);
    ASSERT_EQ(*state.get(0, window, 3), 6);
    ASSERT_EQ(*state.get(0, window, 4), 8);
    ASSERT_EQ(*state.get(0, window, 0), 0);
}

TEST(TimeWindowNamespaceTest, CachedWindowStartingCapacityLearnsFromGrowth) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false, &DefaultAllocator::instance(), 64);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const TimeWindow first{0, 10}, second{10, 20};
    state.put(0, first, 0, 99);
    ASSERT_EQ(arena->free_bytes(), quota - 576);
    for (int64_t key = 1; key < 29; ++key) state.put(0, first, key, key);
    ASSERT_EQ(arena->free_bytes(), quota - 1152);
    state.put(0, second, 0, 7);
    ASSERT_EQ(arena->free_bytes(), quota - 2 * 1152);
    state.put(1, second, 0, 8);
    ASSERT_EQ(arena->free_bytes(), quota - 2 * 1152 - 576);
    ASSERT_EQ(*state.get(0, first, 28), 28);
    ASSERT_EQ(*state.get(0, second, 0), 7);
    ASSERT_EQ(*state.get(1, second, 0), 8);
}

TEST(TimeWindowNamespaceTest, FullSpanSkipsWindowPlacementAndReusesReleasedSpace) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const size_t backing = SwissTable<int64_t, int64_t>::initial_backing_bytes(16, false);
    const int64_t windows = static_cast<int64_t>(quota / backing);
    for (int64_t ns = 0; ns < windows; ++ns) {
        ASSERT_TRUE(arena->contains(state.put(0, ns, 0, ns)));
    }
    ASSERT_EQ(arena->free_bytes(), quota % backing);
    ASSERT_TRUE(!arena->contains(state.put(0, windows, 0, windows)));
    ASSERT_EQ(*state.get(0, windows, 0), windows);
    ASSERT_TRUE(state.remove(0, int64_t{0}, 0));
    ASSERT_TRUE(arena->contains(state.put(0, windows + 1, 0, windows + 1)));
    ASSERT_EQ(*state.get(0, windows, 0), windows);
    ASSERT_EQ(state.namespace_cache_stats().resident_tables, static_cast<size_t>(windows));
}

TEST(TimeWindowNamespaceTest, ASpilledRootTracksPayloadGrowthAndReadmitsAfterTheWorkingSetShrinks) {
    constexpr size_t quota = 128 * 1024;
    auto manager = std::make_shared<HotCacheManager>(2 * quota, [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceRow> state(0, 1, true, &DefaultAllocator::instance(), 16);
    state.enable_namespace_storage(manager, quota);
    auto* arena = state.namespace_arena();
    ASSERT_NE(arena, nullptr);
    const NamespaceRow stable(16 * 1024, 's'), small("small", 5), overflow(3 * quota, 'x');
    state.put(0, 1, stable);
    for (int i = 0; i < 8; ++i) { state.get(0, 1); state.put(0, 2, small); }
    ASSERT_TRUE(arena->contains(state.get(0, 1)->data()));
    state.put(0, 2, overflow);
    ASSERT_FALSE(arena->contains(state.get(0, 1)->data()));
    for (int i = 0; i < 160; ++i) {
        ASSERT_EQ(state.get(0, 1)->size(), stable.size());
        state.update_or_create(0, int64_t{2}, [&](NamespaceRow& row) {
            row.resize(overflow.size() + 64 * (i + 1));
        });
    }
    auto stats = state.namespace_cache_stats();
    ASSERT_GT(stats.growth_reentry_deferrals, 0u);
    ASSERT_FALSE(arena->contains(state.get(0, 1)->data()));
    state.put(0, 2, small);
    state.prepare_snapshot();
    for (int i = 0; i < 4096 && !arena->contains(state.get(0, 1)->data()); ++i) {
        state.put(0, 2, small);
    }
    ASSERT_TRUE(arena->contains(state.get(0, 1)->data()));
    ASSERT_EQ(std::string_view(*state.get(0, 2)), "small");
    ASSERT_TRUE(state.remove(0, 2));
    size_t count = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const NamespaceRow& value) {
        ASSERT_EQ(value.size(), key == 1 ? stable.size() : small.size());
        ++count;
    });
    ASSERT_EQ(count, 2u);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, ReuseAWindowDirectoryBurstAcrossGroupsWithoutRevivingItsRows) {
    auto manager = std::make_shared<HotCacheManager>(256 * 1024);
    StateTable<int64_t, int64_t> state(0, 2, false, &DefaultAllocator::instance(), 8);
    state.enable_namespace_storage(manager, 128 * 1024);
    auto wave = [&](auto namespace_at) {
        for (int i = 0; i < 32; ++i) state.put(0, namespace_at(i), i, int64_t{100} + i);
        state.prepare_snapshot();
        for (int i = 0; i < 32; ++i) ASSERT_TRUE(state.remove(0, namespace_at(i), i));
        const auto before = state.namespace_cache_stats().directory_node_reuses;
        for (int i = 0; i < 32; ++i) {
            ASSERT_EQ(state.get(1, namespace_at(100 + i), i), nullptr);
            state.put(1, namespace_at(100 + i), i, int64_t{200} + i);
            ASSERT_EQ(state.get(0, namespace_at(i), i), nullptr);
        }
        ASSERT_EQ(state.namespace_cache_stats().directory_node_reuses - before, 16u);
        size_t frozen = 0;
        using N = decltype(namespace_at(0));
        state.template for_each_snapshot_in_key_group_ns<N>(0, [&](const N& ns, int64_t key, int64_t value) {
            ASSERT_TRUE(ns == namespace_at(key));
            ASSERT_EQ(value, 100 + key);
            ++frozen;
        });
        ASSERT_EQ(frozen, 32u);
        state.release_snapshot();
        for (int i = 0; i < 32; ++i) ASSERT_TRUE(state.remove(1, namespace_at(100 + i), i));
    };
    wave([](int i){ return int64_t{i}; });
    wave([](int i){ return TimeWindow{i, 1000 + i}; });
    wave([](int i){ return std::to_string(i); });
    ASSERT_EQ(state.total_size(), 0u);
}

TEST(TimeWindowNamespaceTest, FixedWidthWindowRowsKeepTheirBytesAcrossMovesAndOverlappingReplacement) {
    std::pmr::unsynchronized_pool_resource first, second;
    const std::string bytes = "0123456789abcdefghijklmnopqrstuvwxyz";
    auto verify = [&](auto row, size_t width) {
        using Row = decltype(row);
        for (size_t n = 0; n <= bytes.size(); ++n) {
            row.assign(bytes.data(), n);
            Row copy(row, &second);
            ASSERT_EQ(copy.size(), n);
            ASSERT_EQ(std::string_view(copy), std::string_view(bytes).substr(0, n));
            ASSERT_EQ(copy.external_bytes(), n == 0 || n == width ? 0u : n + sizeof(size_t));
            Row moved(std::move(copy), &second);
            ASSERT_EQ(copy.size(), 0u);
            if (n > width) moved.assign(moved.data() + n - width, width);
            if (n > width) ASSERT_EQ(std::string_view(moved), std::string_view(bytes).substr(n - width, width));
            copy = std::move(moved);
            row = std::move(copy);
            ASSERT_EQ(row.size(), std::min(n, width));
            for (size_t next : {size_t(0), width, width + 1, width + 2, width}) {
                row.overwrite(next, [&](char* dest) { std::memcpy(dest, bytes.data(), next); });
                ASSERT_EQ(std::string_view(row), std::string_view(bytes).substr(0, next));
            }
        }
    };
    verify(FixedWindowRow<8>(&first), 8);
    verify(FixedWindowRow<16>(&first), 16);
}

TEST(TimeWindowNamespaceTest, PackedRowLengthsNeverConsumePayloadBytesAcrossMovesAndResizes) {
    std::pmr::unsynchronized_pool_resource one, two;
    std::string bytes(96, ' ');
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = char(255 - i);
    for (size_t from = 0; from <= 48; ++from) {
        for (size_t to = 0; to <= 48; ++to) {
            NamespaceRow row(bytes.data(), from, &one);
            NamespaceRow independent(row, &two);
            row.resize(to, true);
            std::string expected = bytes.substr(0, from);
            expected.resize(to, 0);
            ASSERT_TRUE(std::string_view(row) == expected);
            if (to) row.data()[to - 1] = char(0xf1);
            ASSERT_EQ(row.size(), to);
            NamespaceRow moved(std::move(row), &one);
            ASSERT_TRUE(row.empty());
            ASSERT_EQ(moved.size(), to);
            ASSERT_TRUE(std::string_view(independent) == bytes.substr(0, from));
            independent = std::move(moved);
            ASSERT_EQ(independent.size(), to);
            if (to) ASSERT_EQ(independent.data()[to - 1], char(0xf1));
        }
    }
}

TEST(TimeWindowNamespaceTest, InlineBinaryRowPreservesAliasedWritesAndTierOwnership) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    const std::string large(80, 'k');
    NamespaceRow row(large.data(), large.size(), &arena);
    ASSERT_TRUE(arena.contains(row.data()));
    row.data()[8] = '\0';
    const std::string saved(row.data() + 6, 24);
    row.assign(row.data() + 6, 24);
    ASSERT_EQ(arena.free_bytes(), quota);
    ASSERT_TRUE(std::string_view(row) == saved);
    const auto offset = reinterpret_cast<uintptr_t>(row.data()) - reinterpret_cast<uintptr_t>(&row);
    ASSERT_TRUE(offset + row.size() <= sizeof(row));
    row.resize(25);
    ASSERT_TRUE(arena.contains(row.data()));
    ASSERT_EQ(row.data()[24], 0);
    NamespaceRow copy(row);
    ASSERT_TRUE(!arena.contains(copy.data()));
    ASSERT_TRUE(std::string_view(copy) == std::string_view(row));
    ASSERT_THROW(row.resize(2 * quota), NamespaceCacheFull);
    NamespaceRow moved(std::move(row), &arena);
    ASSERT_EQ(row.size(), 0u);
    ASSERT_TRUE(std::string_view(moved) == std::string_view(copy));
    moved.resize(0);
    ASSERT_EQ(arena.free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, InlineBinaryRowsSurviveSnapshotAndWholeTableSpill) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<WindowStateKey, NamespaceRow> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const int64_t window = 15;
    const WindowStateKey key{17, 0};
    const std::string small(24, 's'), oversized(quota * 2, 'v');
    auto* stored = state.put(0, window, key, NamespaceRow(small.data(), small.size()));
    ASSERT_TRUE(state.namespace_arena()->contains(stored->data()));
    state.prepare_snapshot();
    ASSERT_TRUE(state.modify_in_place(0, window, key, [&](NamespaceRow& value) {
        value.assign(oversized.data(), oversized.size());
    }));
    auto* result = state.get(0, window, key);
    ASSERT_TRUE(!state.namespace_arena()->contains(result->data()));
    ASSERT_TRUE(std::string_view(*result) == oversized);
    size_t rows = 0;
    state.for_each_snapshot_in_key_group_ns<int64_t>(0, [&](int64_t ns, const auto& k, const auto& value) {
        ASSERT_EQ(ns, window);
        ASSERT_TRUE(k == key);
        ASSERT_TRUE(std::string_view(value) == small);
        ++rows;
    });
    state.release_snapshot();
    ASSERT_EQ(rows, 1u);
    ASSERT_EQ(state.namespace_arena()->free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, CompactTablesFillTheSpanWithoutInterTablePadding) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false, &DefaultAllocator::instance(), 64);
    state.enable_namespace_storage(manager, quota);
    auto* arena = state.namespace_arena();
    for (int64_t ns = -4; ns < 0; ++ns) {
        state.put(0, ns, 0, ns);
        ASSERT_TRUE(state.remove(0, ns, 0));
    }
    using Table = SwissTable<int64_t, int64_t>;
    const auto bytes = Table::initial_backing_bytes(4, true);
    ASSERT_EQ(bytes, 96u);
    ASSERT_EQ(Table::initial_backing_alignment(4, true), 16u);
    ASSERT_EQ(Table::initial_backing_alignment(16, false), 64u);
    for (int64_t ns = 0; ns < static_cast<int64_t>(quota / bytes); ++ns) {
        ASSERT_TRUE(arena->contains(state.put(0, ns, 0, ns)));
    }
    ASSERT_EQ(arena->free_bytes(), 0u);
    for (int64_t ns = 0; ns < static_cast<int64_t>(quota / bytes); ++ns) {
        ASSERT_EQ(*state.get(0, ns, 0), ns);
        ASSERT_TRUE(state.remove(0, ns, 0));
    }
    ASSERT_EQ(arena->free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, RecycledRowsReturnTheirWholeAlignedExtentUnderPressure) {
    constexpr size_t bytes = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(bytes,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, bytes);
    auto* all = static_cast<char*>(arena.allocate(bytes, 64));
    std::fill_n(all, bytes, 'v');
    for (size_t i = 0; i < 8; ++i) arena.deallocate(all + i * 32, 32);
    ASSERT_EQ(arena.free_bytes(), 256u);
    for (size_t i = 8; i; --i) {
        auto* row = static_cast<char*>(arena.try_allocate_metadata(31, 16));
        ASSERT_EQ(row, all + (i - 1) * 32);
        std::fill_n(row, 31, 'r');
    }
    ASSERT_EQ(arena.recycled_allocations(), 8u);
    ASSERT_EQ(arena.free_bytes(), 0u);
    for (size_t i = 0; i < 8; ++i) arena.deallocate(all + i * 32, 31);
    ASSERT_TRUE(arena.has_backing_space(256, 64));
    ASSERT_EQ(arena.try_allocate_metadata(256, 64), all);
    ASSERT_EQ(arena.free_bytes(), 0u);
    ASSERT_TRUE(std::all_of(all + 256, all + bytes, [](char c) { return c == 'v'; }));
    arena.deallocate(all, 256);
    arena.deallocate(all + 256, bytes - 256);
    ASSERT_EQ(arena.try_allocate_metadata(bytes, 64), all);
    arena.deallocate(all, bytes);
}

TEST(TimeWindowNamespaceTest, InteriorRunsJoinAcrossBitmapPagesAndHonorAlignment) {
    constexpr size_t bytes = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(bytes,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, bytes);
    auto* start = static_cast<char*>(arena.try_allocate_metadata(bytes, 64));
    ASSERT_NE(start, nullptr);
    const size_t units = bytes / 16;
    for (size_t i = 0; i < units; i += 2) arena.deallocate(start + 16 * i, 16);
    ASSERT_EQ(arena.free_bytes(), bytes / 2);
    ASSERT_FALSE(arena.has_backing_space(32, 16));

    // This run straddles a 4096-quantum boundary. Only 65 quanta remain
    // after the first aligned starting position.
    constexpr size_t first = 4062, last = 4128;
    for (size_t i = first + 1; i < last; i += 2) arena.deallocate(start + 16 * i, 16);
    ASSERT_FALSE(arena.has_backing_space(66 * 16, 64));
    auto* cross = arena.try_allocate_metadata(65 * 16, 64);
    ASSERT_EQ(cross, start + 16 * 4064);
    ASSERT_TRUE(arena.has_backing_space(32, 16));
    arena.deallocate(cross, 65 * 16);
    for (size_t i = 1; i < units; i += 2) {
        if (i < first || i > last) arena.deallocate(start + 16 * i, 16);
    }
    ASSERT_EQ(arena.free_bytes(), bytes);
    ASSERT_EQ(arena.try_allocate_metadata(bytes, 64), start);
    arena.deallocate(start, bytes);
}

TEST(TimeWindowNamespaceTest, FragmentedSpanMatchesEveryAlignedGapInAnIndependentMap) {
    constexpr size_t quota = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    auto* base = static_cast<const unsigned char*>(arena.data());
    std::vector<bool> busy(arena.capacity() / 16);
    struct Block { unsigned char* data; size_t bytes, first, units; unsigned char value; };
    std::vector<Block> live;
    uint32_t prng = 918273;
    auto next = [&] { prng = prng * 1664525 + 1013904223; return prng; };
    auto release = [&](size_t i) {
        const auto b = live[i];
        ASSERT_EQ(b.data[0], b.value);
        ASSERT_EQ(b.data[b.bytes - 1], b.value);
        for (size_t k = b.first; k < b.first + b.units; ++k) { ASSERT_TRUE(busy[k]); busy[k] = false; }
        arena.deallocate(b.data, b.bytes);
        live[i] = live.back();
        live.pop_back();
    };
    const size_t sizes[] = {1, 15, 17, 64, 129, 257, 511, 1008, 1024, 1040, 4113, 65520, 65552};
    for (size_t op = 0; op < 5000; ++op) {
        if (!live.empty() && next() % 5 < 2) { release(next() % live.size()); continue; }
        const size_t bytes = sizes[next() % (sizeof(sizes) / sizeof(sizes[0]))];
        const size_t align = size_t{16} << (next() % 3);
        const size_t units = (bytes + 15) / 16;
        size_t consecutive = 0; bool fits = false;
        for (size_t k = 0; k < busy.size(); ++k) {
            if (busy[k]) consecutive = 0;
            else if (consecutive || !(k % (align / 16))) ++consecutive;
            if (consecutive == units) { fits = true; break; }
        }
        ASSERT_EQ(arena.has_backing_space(bytes, align), fits);
        auto* data = static_cast<unsigned char*>(arena.try_allocate_metadata(bytes, align));
        ASSERT_EQ(data != nullptr, fits);
        if (!data) continue;
        const size_t first = size_t(data - base) / 16;
        ASSERT_EQ(reinterpret_cast<uintptr_t>(data) % align, 0u);
        ASSERT_TRUE(first + units <= busy.size());
        for (size_t k = first; k < first + units; ++k) { ASSERT_FALSE(busy[k]); busy[k] = true; }
        const auto value = static_cast<unsigned char>(next());
        std::fill_n(data, bytes, value);
        live.push_back({data, bytes, first, units, value});
    }
    while (!live.empty()) release(live.size() - 1);
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
    ASSERT_TRUE(arena.has_backing_space(arena.capacity(), 64));
}

TEST(TimeWindowNamespaceTest, WholeTableReleaseCoalescesOnlyFreedRangesAcrossInterleavedOwners) {
    constexpr size_t quota = 192u << 10;
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    constexpr size_t bytes = 192;
    const size_t count = arena.capacity() / bytes;
    std::vector<unsigned char*> rows;
    for (size_t i = 0; i < count; ++i) {
        auto* row = static_cast<unsigned char*>(arena.try_allocate_metadata(bytes, 64));
        ASSERT_NE(row, nullptr);
        std::fill_n(row, bytes, static_cast<unsigned char>(i));
        rows.push_back(row);
    }
    {
        NamespaceCacheArena::ReleaseBatch releasing(arena);
        for (size_t i = 0; i < count; i += 2) arena.deallocate(rows[i], bytes);
        ASSERT_FALSE(arena.has_backing_space(2 * bytes, 64));
        // Reuse and retire a block while the same release is still in progress.
        auto* reused = arena.try_allocate_metadata(bytes, 64);
        ASSERT_NE(reused, nullptr);
        {
            NamespaceCacheArena::ReleaseBatch nested(arena);
            arena.deallocate(reused, bytes);
        }
        for (size_t i = 1; i < count; i += 2) {
            ASSERT_EQ(rows[i][0], static_cast<unsigned char>(i));
            ASSERT_EQ(rows[i][bytes - 1], static_cast<unsigned char>(i));
            arena.deallocate(rows[i], bytes);
        }
    }
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
    auto* whole = arena.try_allocate_metadata(arena.capacity(), 64);
    ASSERT_EQ(whole, arena.data());
    arena.deallocate(whole, arena.capacity());
}

TEST(TimeWindowNamespaceTest, PayloadRunReturnsOnlyUnusedBytesUnderPressure) {
    constexpr size_t quota = 64u << 10;
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    std::array<bool, 8> owners{};
    std::vector<char*> blocks;
    for (int n = 0; n < 3; ++n) {
        for (size_t i = 0; i < owners.size(); ++i) {
            NamespaceCacheArena::Mutation scope(arena, owners[i]);
            for (int j = 0; j < 13; ++j) {
                auto* data = static_cast<char*>(arena.allocate(64, 16));
                ASSERT_TRUE(arena.contains(data));
                std::fill_n(data, 64, char(i + 1));
                blocks.push_back(data);
            }
        }
    }
    ASSERT_TRUE(arena.payload_run_allocations() > arena.payload_runs_reserved());
    ASSERT_TRUE(arena.payload_run_slack() > 0);
    const size_t capacity = arena.capacity();
    ASSERT_EQ(arena.free_bytes() + arena.payload_run_slack() + blocks.size() * 64, capacity);
    for (size_t i = 0; i < blocks.size(); ++i) {
        ASSERT_TRUE(std::all_of(blocks[i], blocks[i] + 64, [i](char c) { return c == char(i / 13 % 8 + 1); }));
        arena.deallocate(blocks[i], 64);
    }
    void* whole = arena.allocate(capacity, 64);
    ASSERT_TRUE(arena.contains(whole));
    ASSERT_EQ(arena.payload_run_slack(), 0u);
    arena.deallocate(whole, capacity);
    for (auto& owner : owners) arena.forget_payload_owner(&owner);
    ASSERT_EQ(arena.free_bytes(), capacity);
}

TEST(TimeWindowNamespaceTest, ReuseFreedPayloadBeforeConsumingAnOpenRun) {
    constexpr size_t quota = 64u << 10;
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    std::array<void*, 12> rows{};
    bool spilled = false;
    {
        NamespaceCacheArena::Mutation mutation(arena, spilled);
        for (auto& row : rows) row = arena.allocate(64, 16);
        const auto remaining = arena.payload_run_slack();
        ASSERT_TRUE(remaining >= 64);
        arena.deallocate(rows.front(), 64);
        const auto recycled = arena.recycled_allocations();
        void* replacement = arena.allocate(63, 16);
        ASSERT_EQ(replacement, rows.front());
        ASSERT_EQ(arena.recycled_allocations(), recycled + 1);
        ASSERT_EQ(arena.payload_run_slack(), remaining);
        arena.deallocate(replacement, 63);
        for (size_t i = 1; i < rows.size(); ++i) arena.deallocate(rows[i], 64);
    }
    arena.forget_payload_owner(&spilled);
    ASSERT_FALSE(spilled);
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
}

TEST(TimeWindowNamespaceTest, AppendedWindowPayloadsStayWithTheirTableAndSnapshot) {
    constexpr size_t quota = 256u << 10;
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t k = 0; k < 96; ++k) {
        for (int64_t ns = 1; ns <= 2; ++ns) {
            state.put(0, ns, k, NamespaceBlob(63, char(ns + k)));
        }
    }
    // A burst in one namespace must be sufficient even if the two stable
    // headers happen to compete for the same run descriptor.
    for (int64_t k = 96; k < 128; ++k) state.put(0, int64_t{1}, k, NamespaceBlob(63, char(1 + k)));
    const auto stats = state.namespace_cache_stats();
    ASSERT_TRUE(stats.payload_run_allocations > 0);
    ASSERT_EQ(stats.resident_tables, 2u);
    state.prepare_snapshot();
    for (int64_t k = 0; k < 128; ++k) {
        ASSERT_TRUE(arena->contains(state.get(0, int64_t{1}, k)->data()));
        ASSERT_TRUE(state.remove(0, int64_t{1}, k));
    }
    for (int64_t k = 0; k < 96; ++k) {
        ASSERT_EQ(*state.get(0, int64_t{2}, k), NamespaceBlob(63, char(2 + k)));
    }
    size_t rows = 0;
    state.for_each_snapshot_in_key_group_ns<int64_t>(0, [&](auto ns, auto k, const auto& value) {
        ASSERT_EQ(value, NamespaceBlob(63, char(ns + k)));
        ++rows;
    });
    ASSERT_EQ(rows, 224u);
    state.release_snapshot();
    for (int64_t k = 0; k < 96; ++k) ASSERT_TRUE(state.remove(0, int64_t{2}, k));
    ASSERT_EQ(state.namespace_cache_stats().payload_run_slack, 0u);
    ASSERT_EQ(state.namespace_cache_stats().resident_bytes, 0u);
}

TEST(TimeWindowNamespaceTest, GrowthKeepsNestedPayloadAddressesWhenTheLargerBackingAloneFits) {
    using Table = SwissTable<int64_t, NamespaceBlob>;
    constexpr size_t quota = 128 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    const size_t old_bytes = Table::initial_backing_bytes(16, false);
    const size_t new_bytes = Table::initial_backing_bytes(32, false);
    void* area = arena.allocate(new_bytes, 64);
    const size_t anchor_size = arena.capacity() - new_bytes - 512;
    void* anchor = arena.allocate(anchor_size, 64);
    arena.deallocate(area, new_bytes);
    {
        Table table(16, &arena);
        ASSERT_EQ(table.ctrl_data(), area);
        void* held = arena.allocate(new_bytes - old_bytes, 64);
        bool spilled = false;
        NamespaceCacheArena::Mutation mutation(arena, spilled);
        for (int64_t k = 0; k < 14; ++k) table.emplace(k).first->assign(k == 0 ? 99 : 1, char(64 + k));
        const auto* payload = table.find(0)->data();
        ASSERT_TRUE(arena.contains(payload));
        arena.deallocate(held, new_bytes - old_bytes);
        ASSERT_FALSE(arena.has_backing_space(new_bytes, 64));
        table.emplace(14).first->assign(1, char(78));
        ASSERT_FALSE(spilled);
        ASSERT_EQ(arena.recovered_resizes(), 1u);
        ASSERT_EQ(table.capacity(), 32u);
        ASSERT_TRUE(arena.contains(table.ctrl_data()));
        ASSERT_TRUE(arena.contains(table.slot_data()));
        ASSERT_TRUE(payload == table.find(0)->data());
        for (int64_t k = 0; k < 15; ++k) ASSERT_EQ(*table.find(k), NamespaceBlob(k == 0 ? 99 : 1, char(64 + k)));
    }
    arena.deallocate(anchor, anchor_size);
    ASSERT_EQ(arena.free_bytes(), arena.capacity());
}

TEST(TimeWindowNamespaceTest, AnotherSpilledPayloadPreventsPartialResizeRecovery) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    void* anchor = arena.allocate(quota, 64);
    bool spilled = false;
    NamespaceCacheArena::Mutation mutation(arena, spilled);
    auto backing = arena.allocate_split(64, 64, 448, 64);
    ASSERT_TRUE(backing.temporary);
    auto* other = arena.allocate(96, 16);
    arena.deallocate(anchor, quota);
    ASSERT_EQ(arena.recover_split(backing, 64, 64, 448, 64).ctrl_ptr, nullptr);
    ASSERT_TRUE(spilled);
    ASSERT_EQ(arena.recovered_resizes(), 0u);
    arena.deallocate_split(backing, 64, 448);
    arena.deallocate(other, 96);
    ASSERT_EQ(arena.free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, PackedCopyReclaimsSlackAndFailedCopies) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    SwissTable<NamespaceBlob, NamespaceBlob> table;
    const NamespaceBlob key(40, 'k'), value(100, 'v'), large(quota, 'w');
    table.insert_or_assign(key, large);
    {
        NamespaceCacheArena::Packing packing(arena, quota);
        ASSERT_TRUE(packing.active());
        ASSERT_THROW(table.relocate(&arena), NamespaceCacheFull);
    }
    ASSERT_EQ(arena.free_bytes(), quota);
    ASSERT_EQ(*table.find(key), large);

    table.insert_or_assign(key, value);
    {
        NamespaceCacheArena::Packing packing(arena, quota);
        ASSERT_TRUE(packing.active());
        table.relocate(&arena);
        ASSERT_TRUE(arena.contains(table.ctrl_data()));
        ASSERT_TRUE(arena.contains(table.find(key)->data()));
    }
    ASSERT_TRUE(arena.free_bytes() > 0);
    ASSERT_EQ(*table.find(key), value);
    table.relocate(&DefaultAllocator::instance());
    ASSERT_EQ(arena.free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, SerializedListNamespacesOwnTheirKeysAndNestedPayloads) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<NamespaceBlob, ElementList> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    state.set_serialized_namespace(std::make_unique<TypeLayout>(TypeId::INT64));
    const auto* arena = state.namespace_arena();

    auto encode = [](int64_t window) {
        WriteBuffer key;
        key.write_long(window);
        NamespaceBlob bytes(reinterpret_cast<const char*>(key.data()), key.size());
        bytes.append(100, 'k');
        return bytes;
    };
    const auto first = encode(10), second = encode(20);
    ElementList one{NamespaceBlob(100, 'a')};
    ElementList two{NamespaceBlob(100, 'b')};
    state.put(0, first, one);
    state.put(0, first, one);
    state.put(0, second, two);
    state.put(0, second, two);
    const auto* resident = state.get(0, first);
    ASSERT_TRUE(arena->contains(resident));
    ASSERT_TRUE(arena->contains(resident->payload().data()));
    ASSERT_EQ(state.get(0, second)->payload(), two.payload());

    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, first));
    size_t seen = 0;
    state.for_each_snapshot_in_key_group(0, [&](const NamespaceBlob& serialized_key, const ElementList& value) {
        ASSERT_TRUE(serialized_key == first || serialized_key == second);
        ASSERT_EQ(value.payload(), serialized_key == first ? one.payload() : two.payload());
        ++seen;
    });
    ASSERT_EQ(seen, 2u);
    ASSERT_EQ(state.get(0, first), nullptr);
    ASSERT_EQ(state.get(0, second)->payload(), two.payload());
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, RepeatedGrowthRequiresReadReuseBeforeFurtherAdmission) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t ns = 0; ns < 4; ++ns) {
        state.put(0, ns, 7, NamespaceBlob(quota * 2, 'x'));
    }
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 4u);
    const int64_t later = 10;
    const NamespaceBlob small(50, 's');
    state.put(0, later, 7, small);
    state.put(0, later, 7, small);
    ASSERT_FALSE(arena->contains(state.get(0, later, 7)->data()));
    ASSERT_FALSE(arena->contains(state.get(0, later, 7)->data()));
    state.put(0, later, 7, small);
    state.put(0, later, 7, small);
    ASSERT_TRUE(arena->contains(state.get(0, later, 7)->data()));
}

TEST(TimeWindowNamespaceTest, LargeFrequentlyEditedRootIndexesControlsInSpareSpace) {
    constexpr size_t budget = 32 * 1024;
    auto manager = std::make_shared<HotCacheManager>(budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, true);
    state.enable_namespace_storage(manager, budget);
    const NamespaceBlob value(70, 'p');
    for (int i = 0; i < 1700; ++i) state.put(0, i, value);
    for (int i = 0; i < 5000; ++i) state.put(0, i % 1700, value);
    const auto* arena = state.namespace_arena();
    const auto* root = state.get_table(0);
    ASSERT_TRUE(root->has_probe_index());
    ASSERT_TRUE(arena->contains(root->ctrl_data()));
    ASSERT_FALSE(arena->contains(state.get(0, 9)->data()));
    state.prepare_snapshot();
    for (int i = 0; i < 1700; i += 2) ASSERT_TRUE(state.remove(0, i));
    size_t frozen = 0;
    state.for_each_snapshot_in_key_group(0, [&](auto k, const auto& prior) {
        ASSERT_TRUE(k >= 0 && k < 1700);
        ASSERT_EQ(prior, value);
        ++frozen;
    });
    state.release_snapshot();
    ASSERT_EQ(frozen, 1700u);
    ASSERT_TRUE(state.namespace_cache_stats().probe_indexes_built > 0);
    for (int i = 0; i < 1700; ++i) ASSERT_EQ(state.get(0, i) != nullptr, bool(i % 2));
}

TEST(TimeWindowNamespaceTest, HotWindowBorrowsForOwnedGrowthWithoutMovingOtherNamespaces) {
    constexpr size_t quota = 32 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 16,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const TimeWindow other{0, 10}, hot{10, 20};
    const NamespaceBlob initial(111, 'h');
    state.put(0, other, 0, initial);
    state.put(0, hot, 0, initial);
    const auto* neighbor = state.get(0, other, 0);
    ASSERT_TRUE(arena->contains(neighbor->data()));
    int64_t keys = 1;
    state.prepare_snapshot();
    while (!state.namespace_cache_stats().growth_loans && keys < 2048) {
        ASSERT_EQ(*state.get(0, hot, 0), initial);
        state.put(0, hot, keys++, *neighbor);
    }
    ASSERT_TRUE(state.namespace_cache_stats().growth_loans > 0);
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 0u);
    ASSERT_TRUE(arena->capacity() > quota);
    ASSERT_EQ(state.get(0, other, 0), neighbor);
    ASSERT_TRUE(manager->free_sets() >= manager->total_sets() / 4);
    for (int64_t key = 0; key < keys; ++key) {
        const auto* value = state.get(0, hot, key);
        ASSERT_EQ(*value, initial);
        ASSERT_TRUE(arena->contains(value));
        ASSERT_TRUE(arena->contains(value->data()));
    }
    size_t original = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0, [&](auto ns, auto k, const auto& previous) {
        ASSERT_TRUE(ns == other || ns == hot);
        ASSERT_EQ(k, 0);
        ASSERT_EQ(previous, initial);
        ++original;
    });
    ASSERT_EQ(original, 2u);
    state.release_snapshot();
    for (int64_t key = 0; key < keys; ++key) ASSERT_TRUE(state.remove(0, hot, key));
    ASSERT_EQ(arena->capacity(), quota);
}

TEST(TimeWindowNamespaceTest, FirstRootBudgetFollowsTouchedGroupsAndBoundsTheirTotalBacking) {
    constexpr size_t quota = 64 * 1024;
    constexpr int groups = 128;
    auto manager = std::make_shared<HotCacheManager>(2 * quota, [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, InnerMapLongLong> state(0, groups, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    ASSERT_EQ(arena->free_bytes(), arena->capacity());
    state.update_or_create(groups - 1, 12, [](auto& map) { map[9] = 123; });
    ASSERT_TRUE(arena->contains(state.get(groups - 1, 12)));
    const auto first_bytes = arena->capacity() - arena->free_bytes();
    ASSERT_GT(first_bytes * groups, quota / 8);
    for (int kg = 0; kg < groups - 1; ++kg)
        state.update_or_create(kg, 12, [](auto& map) { map[9] = 123; });
    size_t resident = 0;
    for (int kg = 0; kg < groups; ++kg) {
        const auto* map = state.get(kg, 12);
        ASSERT_EQ(map->at(9), 123);
        resident += arena->contains(map);
    }
    ASSERT_GT(resident, 0u);
    ASSERT_EQ(resident, state.namespace_cache_stats().initial_root_tables);
    ASSERT_TRUE(resident * first_bytes <= quota / 8);
}

TEST(TimeWindowNamespaceTest, FirstRootWritePlacesOnlyTheActiveGroupAndPreservesSnapshot) {
    constexpr size_t quota = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(8, 4, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    ASSERT_EQ(arena->free_bytes(), arena->capacity());
    ASSERT_EQ(state.get(9, 15), nullptr);
    ASSERT_EQ(state.namespace_cache_stats().initial_root_tables, 0u);
    state.prepare_snapshot();
    const NamespaceBlob first(97, 'x');
    state.put(9, 15, first);
    ASSERT_TRUE(arena->contains(state.get(9,15)->data()));
    ASSERT_EQ(state.namespace_cache_stats().initial_root_tables, 1u);
    size_t old = 0;
    state.for_each_snapshot_in_key_group(9,[&](auto, const auto&) { ++old; });
    ASSERT_EQ(old, 0u);
    state.release_snapshot();
    state.prepare_snapshot();
    state.put(9,15,NamespaceBlob(quota * 2,'y'));
    state.put(11,15,first);
    ASSERT_FALSE(arena->contains(state.get(9,15)->data()));
    ASSERT_TRUE(arena->contains(state.get(11,15)->data()));
    ASSERT_EQ(state.namespace_cache_stats().initial_root_tables, 2u);
    state.for_each_snapshot_in_key_group(9,[&](auto key,const auto& value) {
        ASSERT_EQ(key,15);
        ASSERT_EQ(value,first);
        ++old;
    });
    ASSERT_EQ(old,1u);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, RootsStartingWithMissesEarnResidencyThroughReuse) {
    constexpr size_t quota = 128 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 8, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();

    for (int group = 0; group < 8; ++group) {
        ASSERT_EQ(state.get(group, 19), nullptr);
        state.put(group, 19, group);
    }
    const auto initial = state.namespace_cache_stats();
    ASSERT_EQ(initial.initial_root_tables, 2u);
    ASSERT_EQ(initial.initial_miss_root_deferrals, 6u);
    ASSERT_FALSE(arena->contains(state.get(7, 19)));

    state.prepare_snapshot();
    for (int value = 10; value < 18; ++value) {
        ASSERT_EQ(*state.get(7, 19), value == 10 ? 7 : value - 1);
        state.put(7, 19, value);
    }
    ASSERT_TRUE(arena->contains(state.get(7, 19)));
    ASSERT_EQ(*state.get(7, 19), 17);
    bool found = false;
    state.for_each_snapshot_in_key_group(7, [&](int64_t key, int64_t value) {
        if (key == 19) found = value == 7;
    });
    ASSERT_TRUE(found);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, TwoFieldKeysKeepTheWholeRootInItsOriginalBudget) {
    constexpr size_t budget = 64 * 1024;
    auto manager = std::make_shared<HotCacheManager>(budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<FixedPair, int64_t> compact(0, 1, true);
    compact.enable_namespace_storage(manager, budget);
    for (int64_t i = 0; i < 800; ++i) compact.put(0, FixedPair{{i, -i}}, i);
    compact.put(0, FixedPair{{0, 0}, 256}, -1);
    ASSERT_TRUE(compact.namespace_arena()->contains(compact.get(0, FixedPair{{0, 0}, 256})));
    ASSERT_EQ(*compact.get(0, FixedPair{{0, 0}, 256}), -1);
    ASSERT_EQ(*compact.get(0, FixedPair{{0, 0}}), 0);
    ASSERT_EQ(compact.namespace_cache_stats().growth_demotions, 0u);

    auto reference_manager = std::make_shared<HotCacheManager>(budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<FixedRow, int64_t> wide(0, 1, true);
    wide.enable_namespace_storage(reference_manager, budget);
    for (int64_t i = 0; i < 800; ++i) {
        const int64_t fields[]{i, -i};
        wide.put(0, FixedRow(fields, 2), i);
    }
    const FixedRow zero(2);
    ASSERT_FALSE(wide.namespace_arena()->contains(wide.get(0, zero)));
    ASSERT_EQ(*wide.get(0, zero), 0);
}

TEST(TimeWindowNamespaceTest, ExistingGrowingPartitionWaitsForObservedReadReuse) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, true);
    state.put(0, 0, 0);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t key = 1; key < 50; ++key) state.put(0, key, key * 2);
    ASSERT_EQ(state.namespace_cache_stats().promotions, 0u);
    ASSERT_FALSE(arena->contains(state.get(0, 49)));

    for (int i = 0; i < 300; ++i) {
        ASSERT_EQ(*state.get(0, 49), 98);
        state.put(0, 49, 98);
    }
    ASSERT_EQ(state.namespace_cache_stats().promotions, 1u);
    ASSERT_TRUE(arena->contains(state.get(0, 49)));
    ASSERT_EQ(*state.get(0, 48), 96);
}

TEST(TimeWindowNamespaceTest, PartitionReplacementRequiresRepeatedAccess) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const int64_t key = 7;
    for (int group = 0; group < 3; ++group) {
        state.put(group, key, 100);
        state.put(group, key, 100);
    }
    for (int i = 0; i < 32; ++i) state.put(2, key, i);
    ASSERT_EQ(state.namespace_cache_stats().resident_tables, 2u);
    ASSERT_EQ(state.namespace_cache_stats().evictions, 0u);

    for (int i = 0; i < 32; ++i) {
        ASSERT_TRUE(arena->contains(state.get(0, key)));
    }
    for (int i = 0; i < 260; ++i) {
        ASSERT_NE(state.get(2, key), nullptr);
        state.put(2, key, 100 + i);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_TRUE(arena->contains(state.get(0, key)));
    ASSERT_FALSE(arena->contains(state.get(1, key)));
    ASSERT_TRUE(arena->contains(state.get(2, key)));
    ASSERT_EQ(*state.get(2, key), 359);
}

TEST(TimeWindowNamespaceTest, InfeasiblePartitionDoesNotRepeatedlyScanForAdmission) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    const NamespaceBlob payload(quota * 2, 'p');
    for (int i = 0; i < 16000; ++i) state.put(0, 1, payload);
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.promotions, 0u);
    ASSERT_TRUE(stats.rejected > 1 && stats.rejected < 16);
    ASSERT_EQ(*state.get(0, 1), payload);
}

TEST(TimeWindowNamespaceTest, PartitionEvictionSpendsCreditFromResidentHits) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const int64_t key = 7;
    for (int group = 0; group < 3; ++group) {
        state.put(group, key, 1 + group);
        state.put(group, key, 1 + group);
    }
    for (int i = 0; i < 145; ++i) {
        ASSERT_EQ(*state.get(2, key), 3);
        state.put(2, key, 3);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 0u);
    ASSERT_FALSE(arena->contains(state.get(2, key)));
    for (int i = 0; i < 33; ++i) ASSERT_EQ(*state.get(0, key), 1);
    for (int i = 0; i < 260; ++i) {
        ASSERT_EQ(*state.get(2, key), 3);
        state.put(2, key, 3);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_TRUE(arena->contains(state.get(0, key)));
    ASSERT_FALSE(arena->contains(state.get(1, key)));
    ASSERT_TRUE(arena->contains(state.get(2, key)));
}

TEST(TimeWindowNamespaceTest, AReplacedRootEarnsItsOwnCreditBeforeDisplacingAnother) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    constexpr int64_t key = 7;
    for (int group = 0; group < 3; ++group) {
        state.put(group, key, group + 1);
        state.put(group, key, group + 1);
    }
    for (int i = 0; i < 64; ++i) ASSERT_EQ(*state.get(0, key), 1);
    for (int i = 0; i < 300; ++i) {
        ASSERT_EQ(*state.get(2, key), 3);
        state.put(2, key, 3);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_FALSE(arena->contains(state.get(1, key)));

    // Other groups replenish the shared budget. A recently displaced root
    // stays on DRAM until its own accesses pay for both table copies.
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(*state.get(0, key), 1);
        ASSERT_EQ(*state.get(2, key), 3);
        ASSERT_EQ(*state.get(1, key), 2);
        state.put(1, key, 2);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_FALSE(arena->contains(state.get(1, key)));
    const auto credit_before = state.namespace_cache_stats().dram_reuse_credit_spent;
    for (int i = 0; i < 800; ++i) {
        ASSERT_EQ(*state.get(1, key), 2);
        state.put(1, key, 2);
    }
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.evictions, 2u);
    ASSERT_GT(stats.dram_reuse_credit_spent, credit_before);
    ASSERT_TRUE(arena->contains(state.get(1, key)));
    ASSERT_EQ(*state.get(0, key), 1);
    ASSERT_EQ(*state.get(2, key), 3);
}

TEST(TimeWindowNamespaceTest, AlignedControlDirectoryUsesOnlyAvailablePoolSpace) {
    for (size_t quota : {size_t{768}, size_t{1} << 20}) {
        auto manager = std::make_shared<HotCacheManager>(quota * 8,
                [](L0LibBindings*, std::string*) { return false; });
        StateTable<int64_t, int64_t> state(0, 4, false);
        state.enable_namespace_storage(manager, quota);
        auto* views = state.resident_control_views();
        ASSERT_NE(views, nullptr);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(views) % 64, 0u);
        const bool pooled = quota > 768;
        ASSERT_EQ(state.namespace_arena()->contains(views), pooled);
        ASSERT_EQ(state.namespace_cache_stats().control_view_bytes, pooled ? 16 * sizeof(ResidentControls) : 0u);
        ASSERT_EQ(state.namespace_cache_stats().resident_bytes, 0u);
        state.put(1, int64_t{7}, int64_t{19}, int64_t{42});
        ASSERT_EQ(*state.get(1, int64_t{7}, int64_t{19}), 42);
        ASSERT_EQ(state.resident_control_views(), views);
    }
}

TEST(TimeWindowNamespaceTest, ColdRegistrationsLeaveRoomForAnActiveNamespaceToGrow) {
    constexpr size_t requested = 1u << 20;
    auto manager = std::make_shared<HotCacheManager>(15 * requested,
            [](L0LibBindings*, std::string*) { return false; });
    using State = StateTable<int64_t, int64_t>;
    std::vector<std::unique_ptr<State>> states;
    for (int i = 0; i < 24; ++i) {
        auto state = std::make_unique<State>(0, 1, true);
        state->enable_namespace_storage(manager, requested);
        ASSERT_NE(state->namespace_arena(), nullptr);
        states.emplace_back(std::move(state));
    }
    auto& state = *states.back();
    const auto* arena = state.namespace_arena();
    ASSERT_TRUE(manager->used_bytes() < manager->capacity_bytes() / 2);
    ASSERT_TRUE(arena->maximum_table_capacity() >= 7 * requested);
    for (int64_t i = 0; i < 12000; ++i) state.put(0, i, i + 7);
    ASSERT_EQ(state.namespace_cache_stats().initial_share_expansions, 1u);
    ASSERT_TRUE(arena->capacity() > arena->primary_capacity());
    for (int64_t i = 0; i < 12000; ++i) {
        const auto* value = state.get(0, i);
        ASSERT_TRUE(arena->contains(value));
        ASSERT_EQ(*value, i + 7);
    }
    const size_t grown = manager->used_bytes();
    states.pop_back();
    ASSERT_TRUE(manager->used_bytes() + requested / 2 < grown);
}

TEST(TimeWindowNamespaceTest, ActiveDramPartitionCanDisplaceAnIdleResidentWithoutResidentTraffic) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int group = 0; group < 3; ++group) {
        state.put(group, 11, group + 20);
        state.put(group, 11, group + 20);
    }
    ASSERT_FALSE(arena->contains(state.get(2, 11)));
    // Missing keys are plentiful in joins. They must never earn copy credit.
    for (int i = 0; i < 3000; ++i) ASSERT_EQ(state.get(2, 42), nullptr);
    state.put(2, 11, 22);
    ASSERT_EQ(state.namespace_cache_stats().evictions, 0u);
    for (int i = 0; i < 3000; ++i) {
        ASSERT_EQ(*state.get(2, 11), 22);
        state.put(2, 11, 22);
    }
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.evictions, 1u);
    ASSERT_TRUE(stats.dram_reuse_credit_spent > 0);
    ASSERT_TRUE(stats.charged_root_copy_bytes >= stats.dram_reuse_credit_spent);
    ASSERT_TRUE(arena->contains(state.get(2, 11)));
    for (int group = 0; group < 3; ++group) ASSERT_EQ(*state.get(group, 11), group + 20);
}

TEST(TimeWindowNamespaceTest, FinalLoanCanCoverSustainedDemandWithinTheArenaCap) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 16,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    auto* primary = arena.allocate(quota, 16);
    ASSERT_TRUE(arena.try_borrow(64, 64));
    const size_t first_loan = arena.free_bytes();
    ASSERT_EQ(first_loan, quota / 2);
    auto* first = arena.allocate(first_loan, 16);
    ASSERT_TRUE(arena.try_borrow(64, 64));
    ASSERT_TRUE(arena.free_bytes() > first_loan);
    ASSERT_TRUE(arena.capacity() <= arena.maximum_capacity());
    auto* second = arena.allocate(64, 16);
    ASSERT_TRUE(arena.contains(second));

    arena.deallocate(second, 64);
    arena.deallocate(first, first_loan);
    arena.deallocate(primary, quota);
    arena.release_empty_loans();
    ASSERT_EQ(arena.capacity(), quota);
    ASSERT_EQ(arena.free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, SharedPoolLoanKeepsLiveNamespacesAndReturnsEmptySpan) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 8,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false);
    StateTable<int64_t, int64_t> neighbor(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    neighbor.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const TimeWindow first{0, 10}, second{10, 20}, third{20, 30};
    neighbor.put(0, first, 7, 70);
    state.put(0, first, 7, 71);
    state.put(0, second, 7, 72);
    const auto* initially_resident = state.put(0, third, 7, 73);
    ASSERT_TRUE(arena->contains(initially_resident));
    ASSERT_EQ(*initially_resident, 73);
    ASSERT_EQ(state.namespace_cache_stats().loans_granted, 1u);
    ASSERT_EQ(state.namespace_cache_stats().evictions, 0u);
    ASSERT_EQ(state.namespace_cache_stats().resident_tables, 3u);
    ASSERT_EQ(arena->primary_capacity(), quota);
    ASSERT_TRUE(arena->capacity() > quota);
    ASSERT_TRUE(arena->contains(state.get(0, third, 7)));
    ASSERT_TRUE(neighbor.namespace_arena()->contains(neighbor.get(0, first, 7)));
    ASSERT_EQ(*neighbor.get(0, first, 7), 70);
    const size_t leased_bytes = manager->used_bytes();

    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, third, 7));
    ASSERT_EQ(arena->capacity(), quota);
    ASSERT_TRUE(manager->used_bytes() < leased_bytes);
    size_t saved = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0, [&](const auto& ns, int64_t, int64_t value) {
        if (ns != third) return;
        ASSERT_EQ(value, 73);
        ++saved;
    });
    ASSERT_EQ(saved, 1u);
    state.release_snapshot();
}

// ============================================================================
//  Basic TimeWindow Operations
// ============================================================================

TEST(TimeWindowNamespaceTest, PutAndGet) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{2000, 3000};

    st.put<TimeWindow>(0, tw1, 100, 10);
    st.put<TimeWindow>(0, tw2, 100, 20);

    ASSERT_EQ(*st.get<TimeWindow>(0, tw1, 100), 10);
    ASSERT_EQ(*st.get<TimeWindow>(0, tw2, 100), 20);
}

TEST(TimeWindowNamespaceTest, SameKeyDifferentWindows) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw1{0, 1000};
    TimeWindow tw2{1000, 2000};
    TimeWindow tw3{2000, 3000};

    st.put<TimeWindow>(0, tw1, 42, 100);
    st.put<TimeWindow>(0, tw2, 42, 200);
    st.put<TimeWindow>(0, tw3, 42, 300);

    ASSERT_EQ(*st.get<TimeWindow>(0, tw1, 42), 100);
    ASSERT_EQ(*st.get<TimeWindow>(0, tw2, 42), 200);
    ASSERT_EQ(*st.get<TimeWindow>(0, tw3, 42), 300);
}

TEST(TimeWindowNamespaceTest, NonExistentWindowReturnsNull) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw_exists{1000, 2000};
    TimeWindow tw_missing{5000, 6000};

    st.put<TimeWindow>(0, tw_exists, 1, 100);

    ASSERT_NE(st.get<TimeWindow>(0, tw_exists, 1), nullptr);
    ASSERT_EQ(st.get<TimeWindow>(0, tw_missing, 1), nullptr);
}

// ============================================================================
//  Remove
// ============================================================================

TEST(TimeWindowNamespaceTest, Remove) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw{1000, 2000};
    st.put<TimeWindow>(0, tw, 1, 100);
    st.put<TimeWindow>(0, tw, 2, 200);

    bool removed = st.remove<TimeWindow>(0, tw, 1);
    ASSERT_TRUE(removed);
    ASSERT_EQ(st.get<TimeWindow>(0, tw, 1), nullptr);
    ASSERT_NE(st.get<TimeWindow>(0, tw, 2), nullptr);
}

TEST(TimeWindowNamespaceTest, RemoveLastEntryRemovesNamespace) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw{1000, 2000};
    st.put<TimeWindow>(0, tw, 1, 100);

    // Remove the only entry — namespace table should be cleaned up
    st.remove<TimeWindow>(0, tw, 1);

    // Verify no lingering state
    ASSERT_EQ(st.get<TimeWindow>(0, tw, 1), nullptr);
}

// ============================================================================
//  Iteration
// ============================================================================

TEST(TimeWindowNamespaceTest, IterateAllNamespaces) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{2000, 3000};

    st.put<TimeWindow>(0, tw1, 10, 100);
    st.put<TimeWindow>(0, tw1, 20, 200);
    st.put<TimeWindow>(0, tw2, 10, 300);

    // Collect all entries grouped by namespace
    std::map<TimeWindow, std::vector<std::pair<int64_t, int64_t>>> collected;
    st.for_each_in_key_group_ns<TimeWindow>(0,
        [&collected](const TimeWindow& tw, const int64_t& k, const int64_t& v) {
            collected[tw].push_back({k, v});
        });

    ASSERT_EQ(collected.size(), 2u);
    ASSERT_EQ(collected[tw1].size(), 2u);
    ASSERT_EQ(collected[tw2].size(), 1u);
}

TEST(TimeWindowNamespaceTest, EmptyIterationNoCallback) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    int count = 0;
    st.for_each_in_key_group_ns<TimeWindow>(0,
        [&count](const TimeWindow&, const int64_t&, const int64_t&) {
            ++count;
        });
    ASSERT_EQ(count, 0);
}

// ============================================================================
//  Update (overwrite)
// ============================================================================

TEST(TimeWindowNamespaceTest, Update) {
    StateTable<int64_t, int64_t> st(0, 1, false);

    TimeWindow tw{1000, 2000};
    st.put<TimeWindow>(0, tw, 1, 100);
    st.put<TimeWindow>(0, tw, 1, 200);  // overwrite

    ASSERT_EQ(*st.get<TimeWindow>(0, tw, 1), 200);
}

// ============================================================================
//  Multiple Key Groups with TimeWindow
// ============================================================================

TEST(TimeWindowNamespaceTest, MultipleKeyGroups) {
    StateTable<int64_t, int64_t> st(0, 3, false);

    TimeWindow tw{1000, 2000};

    st.put<TimeWindow>(0, tw, 1, 100);
    st.put<TimeWindow>(1, tw, 1, 200);
    st.put<TimeWindow>(2, tw, 1, 300);

    ASSERT_EQ(*st.get<TimeWindow>(0, tw, 1), 100);
    ASSERT_EQ(*st.get<TimeWindow>(1, tw, 1), 200);
    ASSERT_EQ(*st.get<TimeWindow>(2, tw, 1), 300);
}

// ============================================================================
//  TimeWindow with String Values
// ============================================================================

TEST(TimeWindowNamespaceTest, StringValues) {
    StateTable<int64_t, std::string> st(0, 1, false);

    TimeWindow tw{1000, 2000};
    st.put<TimeWindow>(0, tw, 1, std::string("hello"));
    st.put<TimeWindow>(0, tw, 2, std::string("world"));

    ASSERT_EQ(*st.get<TimeWindow>(0, tw, 1), "hello");
    ASSERT_EQ(*st.get<TimeWindow>(0, tw, 2), "world");
}

// ============================================================================
//  TimeWindow with MapState (InnerMap)
// ============================================================================

TEST(TimeWindowNamespaceTest, InnerMapValue) {
    StateTable<int64_t, InnerMap> st(0, 1, false);

    TimeWindow tw{1000, 2000};
    InnerMap map{{"k1", "v1"}, {"k2", "v2"}};
    st.put<TimeWindow>(0, tw, 1, std::move(map));

    auto* m = st.get<TimeWindow>(0, tw, static_cast<int64_t>(1));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 2u);
    ASSERT_EQ((*m)["k1"], "v1");
}

// ============================================================================
//  TimeWindow Hash and Equality
// ============================================================================

TEST(TimeWindowNamespaceTest, HashDistinguishesWindows) {
    TimeWindowHash hash;
    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{1000, 3000};
    TimeWindow tw3{2000, 3000};

    // Different windows should generally produce different hashes
    // (not guaranteed, but should be true for these inputs)
    ASSERT_NE(hash(tw1), hash(tw2));
    ASSERT_NE(hash(tw1), hash(tw3));
    ASSERT_NE(hash(tw2), hash(tw3));

    // Same window should produce same hash
    TimeWindow tw1_copy{1000, 2000};
    ASSERT_EQ(hash(tw1), hash(tw1_copy));
}

TEST(TimeWindowNamespaceTest, EqualityOperator) {
    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{1000, 2000};
    TimeWindow tw3{1000, 3000};

    ASSERT_EQ(tw1, tw2);
    ASSERT_NE(tw1, tw3);
}

// ---- Namespace COW Snapshot Tests ----

TEST(TimeWindowNamespaceTest, COWSnapshotPutOverwrite) {
    // Verify that overwriting a value after snapshot preserves old value in snapshot
    StateTable<int64_t, int64_t> st(0, 1, false);
    TimeWindow tw{1000, 2000};

    st.put(0, tw, 42L, 100L);
    st.prepare_snapshot();

    // Overwrite after snapshot
    st.put(0, tw, 42L, 999L);

    // Current value should be new
    ASSERT_EQ(*st.get(0, tw, 42L), 999);

    // Snapshot iteration should return old value
    std::vector<std::tuple<TimeWindow, int64_t, int64_t>> snapshot;
    st.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
        [&](const TimeWindow& ns, const int64_t& k, const int64_t& v) {
            snapshot.emplace_back(ns, k, v);
        });
    ASSERT_EQ(snapshot.size(), 1u);
    ASSERT_EQ(std::get<2>(snapshot[0]), 100);

    st.release_snapshot();
}

TEST(TimeWindowNamespaceTest, COWSnapshotDelete) {
    // Verify that deleting after snapshot preserves entry in snapshot
    StateTable<int64_t, int64_t> st(0, 1, false);
    TimeWindow tw{1000, 2000};

    st.put(0, tw, 42L, 100L);
    st.put(0, tw, 43L, 200L);
    st.prepare_snapshot();

    // Delete key 42 after snapshot
    st.remove(0, tw, 42L);
    ASSERT_EQ(st.get(0, tw, 42L), nullptr);

    // Snapshot should still have both entries
    std::vector<std::tuple<TimeWindow, int64_t, int64_t>> snapshot;
    st.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
        [&](const TimeWindow& ns, const int64_t& k, const int64_t& v) {
            snapshot.emplace_back(ns, k, v);
        });
    ASSERT_EQ(snapshot.size(), 2u);

    st.release_snapshot();
}

TEST(TimeWindowNamespaceTest, COWSnapshotAddAfter) {
    // Entries added after snapshot should NOT appear in snapshot
    StateTable<int64_t, int64_t> st(0, 1, false);
    TimeWindow tw{1000, 2000};

    st.put(0, tw, 42L, 100L);
    st.prepare_snapshot();

    // Add new entry after snapshot
    st.put(0, tw, 99L, 500L);

    std::vector<int64_t> keys;
    st.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
        [&](const TimeWindow&, const int64_t& k, const int64_t&) {
            keys.push_back(k);
        });
    ASSERT_EQ(keys.size(), 1u);
    ASSERT_EQ(keys[0], 42);

    st.release_snapshot();
}

TEST(TimeWindowNamespaceTest, COWSnapshotOverwriteThenDelete) {
    // Overwrite, then delete — snapshot should have original value
    StateTable<int64_t, int64_t> st(0, 1, false);
    TimeWindow tw{1000, 2000};

    st.put(0, tw, 42L, 100L);
    st.prepare_snapshot();

    st.put(0, tw, 42L, 200L);  // overwrite
    st.remove(0, tw, 42L);     // then delete

    std::vector<std::tuple<TimeWindow, int64_t, int64_t>> snapshot;
    st.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
        [&](const TimeWindow& ns, const int64_t& k, const int64_t& v) {
            snapshot.emplace_back(ns, k, v);
        });
    ASSERT_EQ(snapshot.size(), 1u);
    ASSERT_EQ(std::get<2>(snapshot[0]), 100);  // original value

    st.release_snapshot();
}

TEST(TimeWindowNamespaceTest, COWSnapshotMultipleWindows) {
    // COW across multiple TimeWindow namespaces
    StateTable<int64_t, int64_t> st(0, 1, false);
    TimeWindow tw1{1000, 2000};
    TimeWindow tw2{2000, 3000};

    st.put(0, tw1, 1L, 10L);
    st.put(0, tw2, 1L, 20L);
    st.prepare_snapshot();

    // Modify one window, delete from another
    st.put(0, tw1, 1L, 99L);
    st.remove(0, tw2, 1L);

    std::map<int64_t, int64_t> snapshot_tw1, snapshot_tw2;
    st.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
        [&](const TimeWindow& ns, const int64_t& k, const int64_t& v) {
            if (ns == tw1) snapshot_tw1[k] = v;
            else if (ns == tw2) snapshot_tw2[k] = v;
        });

    ASSERT_EQ(snapshot_tw1.size(), 1u);
    ASSERT_EQ(snapshot_tw1[1], 10);  // old value
    ASSERT_EQ(snapshot_tw2.size(), 1u);
    ASSERT_EQ(snapshot_tw2[1], 20);  // deleted but in snapshot

    st.release_snapshot();
}

TEST(TimeWindowNamespaceTest, LastRootPositionRevalidatesCollisionsErasureGrowthAndSnapshot) {
    constexpr size_t budget = 16 * 1024;
    auto manager = std::make_shared<HotCacheManager>(budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<FixedPair, int64_t> state(0, 2, true);
    state.enable_namespace_storage(manager, budget);
    auto* views = state.resident_control_views();
    ASSERT_NE(views, nullptr);
    for (int group = 0; group < 2; ++group) {
        for (int64_t i = 0; i < 1000; ++i) state.put(group, FixedPair{{i, -i}}, i + 10000 * group);
    }
    const FixedPair key{{700, -700}};
    const FixedPair collision{{700, -700}, 256};
    ASSERT_FALSE(state.namespace_arena()->contains(state.get(0, key)));
    ASSERT_NE(views[0].last_root_row, 0u);
    ASSERT_EQ(static_cast<uint64_t>(views[0].second), views[0].last_root_row);
    const auto before = state.namespace_cache_stats().repeated_root_reads;
    ASSERT_EQ(*state.get(0, key), 700);
    ASSERT_GT(state.namespace_cache_stats().repeated_root_reads, before);
    ASSERT_EQ(state.get(0, collision), nullptr);
    state.put(0, collision, -8);
    ASSERT_EQ(views[0].second, 0);
    ASSERT_EQ(*state.get(0, collision), -8);
    ASSERT_NE(views[0].second, 0);
    ASSERT_EQ(*state.get(1, key), 10700);
    const auto before_write = state.namespace_cache_stats();
    state.put(1, key, 10701);
    const auto after_write = state.namespace_cache_stats();
    ASSERT_EQ(after_write.repeated_root_writes, before_write.repeated_root_writes + 1);
    ASSERT_EQ(after_write.read_position_writes, before_write.read_position_writes);
    state.prepare_snapshot();
    state.remove(0, collision);
    ASSERT_EQ(views[0].second, 0);
    ASSERT_EQ(state.get(0, collision), nullptr);
    state.put(0, key, 71);
    for (int64_t i = 1000; i < 5000; ++i) state.put(0, FixedPair{{i, -i}}, i);
    ASSERT_EQ(*state.get(0, key), 71);
    ASSERT_EQ(*state.get(1, key), 10701);
    bool saw_old_key = false, saw_deleted = false;
    state.for_each_snapshot_in_key_group(0, [&](const FixedPair& k, const int64_t& value) {
        if (k == key) saw_old_key = value == 700;
        if (k == collision) saw_deleted = value == -8;
    });
    ASSERT_TRUE(saw_old_key && saw_deleted);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, NegativeHeavyRootUsesBoundedSummaryAndRestoresFullState) {
    constexpr size_t quota = 64 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    for (int i = 0; i < 1400; ++i) {
        ASSERT_EQ(state.get(0, i), nullptr);
        state.put(0, i, 1000 + i);
    }
    ASSERT_EQ(*state.get(0, 1200), 2200);
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.memberships_built, 1u);
    ASSERT_GT(stats.membership_skips, 400u);
    ASSERT_GT(stats.membership_probes, stats.membership_skips);
    ASSERT_EQ(state.namespace_arena()->backing()->hits(), 0u);
    ASSERT_FALSE(state.namespace_arena()->contains(state.get(0, 1300)));
    state.prepare_snapshot();
    state.put(0, 1300, 0);
    state.remove(0, 42);
    state.put(0, 1500, 17);
    std::map<int64_t, int64_t> saved;
    state.for_each_snapshot_in_key_group(0, [&](const auto& key, const auto& value) { saved[key] = value; });
    state.release_snapshot();
    ASSERT_EQ(saved.size(), 1400u);
    ASSERT_EQ(saved.at(1300), 2300);
    ASSERT_EQ(saved.at(42), 1042);
    ASSERT_EQ(*state.get(0, 1500), 17);
}

TEST(TimeWindowNamespaceTest, RepeatedSharedMissThenInsertCanUseTheExactRootControls) {
    constexpr size_t quota = 64 * 1024;
    auto manager = std::make_shared<HotCacheManager>(2 * quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    auto* membership = state.root_membership_views();
    auto* controls = state.resident_control_views();
    ASSERT_NE(membership, nullptr);
    ASSERT_NE(controls, nullptr);
    size_t paired = 0;
    for (int64_t key = 0; key < 24576 && !state.namespace_cache_stats().probe_indexes_built; ++key) {
        bool absent = false;
        if (membership->bitmap_offset_plus_one) {
            const auto hash = SwissKeyHash<int64_t>{}(key);
            const auto mixed = SwissKeyHash<uint64_t>{}(hash);
            const uint64_t word = (mixed >> 24) & ((uint64_t{1} << 40) - 1) & membership->word_mask;
            const uint64_t mask = (uint64_t{1} << (mixed & 63)) | (uint64_t{1} << ((mixed >> 6) & 63))
                    | (uint64_t{1} << ((mixed >> 12) & 63));
            uint64_t bits;
            const auto* base = reinterpret_cast<const uint8_t*>(state.namespace_arena()->pool_base());
            std::memcpy(&bits, base + membership->bitmap_offset_plus_one - 1 + 8 * word, sizeof(bits));
            absent = (bits & mask) != mask;
            if (absent) {
                membership->last_absent_hash = hash;
                membership->word_mask |= RootMembershipView::valid_absence;
                ++membership->skipped_reads;
                ++paired;
            }
        }
        if (!absent) ASSERT_EQ(state.get(0, key), nullptr);
        state.put(0, key, key + 7);
    }
    ASSERT_GT(paired, 128u);
    ASSERT_GT(state.namespace_cache_stats().probe_indexes_built, 0u);
    ASSERT_EQ(membership->bitmap_offset_plus_one, 0u);
    ASSERT_EQ(*state.get(0, 99), 106);
    state.prepare_snapshot();
    state.put(0, 99, 333);
    bool found = false;
    state.for_each_snapshot_in_key_group(0, [&](int64_t k, int64_t v) { if (k == 99) found = v == 106; });
    ASSERT_TRUE(found);
    state.release_snapshot();
    ASSERT_EQ(*state.get(0, 99), 333);
}

TEST(TimeWindowNamespaceTest, MembershipBacksOffWhenTheNamespaceStartsReturningExistingKeys) {
    constexpr size_t quota = 64 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    for (int i = 0; i < 1400; ++i) {
        state.get(0, i);
        state.put(0, i, 2 * i);
    }
    auto start = state.namespace_cache_stats();
    for (int i = 0; i < 5000; ++i) ASSERT_EQ(*state.get(0, i % 1300), 2 * (i % 1300));
    auto changed = state.namespace_cache_stats();
    ASSERT_GT(changed.membership_bypassed - start.membership_bypassed, 4100u);
    state.put(0, 2000, 47);
    ASSERT_EQ(*state.get(0, 2000), 47);
    for (int i = 0; i < 5000; ++i) ASSERT_EQ(state.get(0, 10000 + i), nullptr);
    auto final = state.namespace_cache_stats();
    ASSERT_GT(final.membership_skips - changed.membership_skips, 2700u);
    ASSERT_EQ(*state.get(0, 2000), 47);
}

TEST(TimeWindowNamespaceTest, WholeWindowClaimsSpaceFromNegativeLookupIndex) {
    constexpr size_t quota = 64 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t i = 0; i < 1400; ++i) {
        ASSERT_EQ(state.get(0, i), nullptr);
        state.put(0, i, i + 17);
    }
    const auto before = state.namespace_cache_stats();
    ASSERT_GT(before.membership_bytes, 0u);
    ASSERT_EQ(before.resident_bytes, 0u);
    ASSERT_EQ(before.membership_bytes, arena->capacity() - arena->free_bytes());
    const size_t backing = SwissTable<int64_t, int64_t>::initial_backing_bytes(16, false);
    const int64_t windows = static_cast<int64_t>(quota / backing);
    state.prepare_snapshot();
    size_t windows_created = 0;
    while (state.namespace_cache_stats().memberships_released == 0) {
        const auto w = static_cast<int64_t>(windows_created);
        ASSERT_TRUE(w < windows);
        ASSERT_TRUE(arena->contains(state.put(0, TimeWindow{w, w + 1}, 7, w)));
        ++windows_created;
    }
    const auto after = state.namespace_cache_stats();
    ASSERT_EQ(after.memberships_released, 1u);
    ASSERT_EQ(after.membership_bytes, 0u);
    ASSERT_EQ(after.resident_bytes, arena->capacity() - arena->free_bytes());
    ASSERT_EQ(after.resident_tables, windows_created);
    ASSERT_EQ(after.evictions, 0u);
    for (int64_t i = 0; i < 100; ++i) {
        ASSERT_EQ(*state.get(0, i), i + 17);
        ASSERT_EQ(state.get(0, 10000 + i), nullptr);
        state.put(0, i, -i);
    }
    ASSERT_EQ(state.namespace_cache_stats().memberships_built, before.memberships_built);
    std::map<int64_t, int64_t> snapshot;
    state.for_each_snapshot_in_key_group(0, [&](auto k, auto v) { snapshot[k] = v; });
    ASSERT_EQ(snapshot.size(), 1400u);
    ASSERT_EQ(snapshot.at(71), 88);
    state.release_snapshot();
    ASSERT_EQ(*state.get(0, 71), -71);
}

TEST(TimeWindowNamespaceTest, ResidentValueGrowthAlsoReclaimsAnOptionalIndex) {
    constexpr size_t quota = 64 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t i = 0; i < 1400; ++i) {
        state.get(0, i);
        state.put(0, i, NamespaceBlob("root"));
    }
    ASSERT_GT(state.namespace_cache_stats().membership_bytes, 0u);
    const TimeWindow window{10, 20};
    ASSERT_TRUE(arena->contains(state.put(0, window, 1, NamespaceBlob("start"))));
    // Leave less than one 2 KiB payload at the end of the arena. The
    // released 4 KiB index at its start can hold the complete allocation.
    for (int64_t w = 0; arena->free_bytes() >= 2048; ++w) {
        ASSERT_TRUE(arena->contains(state.put(0, TimeWindow{w+100, w+101}, 1, NamespaceBlob("x"))));
    }
    ASSERT_EQ(state.namespace_cache_stats().memberships_released, 0u);
    NamespaceBlob larger(2048, 'y');
    auto* value = state.put(0, window, 1, larger);
    ASSERT_EQ(*value, larger);
    ASSERT_TRUE(arena->contains(value));
    ASSERT_TRUE(arena->contains(value->data()));
    ASSERT_EQ(state.namespace_cache_stats().membership_bytes, 0u);
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 0u);
    ASSERT_EQ(*state.get(0, 85), NamespaceBlob("root"));
}

TEST(TimeWindowNamespaceTest, UpdatesCanEarnWholeRootPlacementWithoutPayloadReads) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(2 * quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    for (int g = 0; g < 3; ++g) {
        state.put(g, 7, g);
        state.put(g, 7, g);
    }
    const auto* arena = state.namespace_arena();
    for (int i = 0; i < 512; ++i) state.update_or_create(0, 7, [](auto& v) { ++v; });
    state.prepare_snapshot();
    for (int i = 0; i < 512; ++i) state.update_or_create(2, 7, [](auto& v) { ++v; });
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.evictions, 1u);
    ASSERT_GT(stats.resident_root_updates, 512u);
    ASSERT_GT(stats.dram_root_updates, 32u);
    ASSERT_EQ(arena->backing()->lookups(), 0u);
    ASSERT_EQ(arena->backing()->hits(), 0u);
    ASSERT_TRUE(arena->contains(state.get(2, 7)));
    ASSERT_FALSE(arena->contains(state.get(1, 7)));
    int64_t saved = -1;
    state.for_each_snapshot_in_key_group(2, [&](auto, auto v) { saved = v; });
    ASSERT_EQ(saved, 2);
    state.release_snapshot();
    ASSERT_EQ(*state.get(2, 7), 514);
}

TEST(TimeWindowNamespaceTest, FreedWindowBlocksCanBeCoalescedForAnEntireTable) {
    constexpr size_t quota = 8 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    auto* a = arena.allocate(64, 64);
    auto* b = arena.allocate(128, 64);
    auto* tail = arena.allocate(quota - 192, 64);
    arena.deallocate(a, 64);
    ASSERT_EQ(arena.allocate(64, 64), a);
    arena.deallocate(a, 64);
    arena.deallocate(b, 128);
    ASSERT_EQ(arena.free_bytes(), 192u);
    ASSERT_TRUE(arena.has_backing_space(192, 64));
    auto* joined = arena.allocate(192, 64);
    ASSERT_EQ(joined, a);
    ASSERT_EQ(arena.free_bytes(), 0u);
    arena.deallocate(joined, 192);
    arena.deallocate(tail, quota - 192);
    ASSERT_TRUE(arena.has_backing_space(quota, 64));
    auto* all = arena.allocate(quota, 64);
    ASSERT_EQ(all, a);
    arena.deallocate(all, quota);
    ASSERT_EQ(arena.free_bytes(), quota);
}

TEST(TimeWindowNamespaceTest, NextSmallAllocationReusesTheMostRecentlyFreedMatchingBlock) {
    constexpr size_t quota = 64 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    auto* first = arena.allocate(80, 16);
    auto* middle = arena.allocate(96, 16);
    auto* last = arena.allocate(80, 16);
    arena.deallocate(first, 80);
    arena.deallocate(last, 80);
    auto* newest = arena.allocate(80, 16);
    ASSERT_EQ(newest, last);
    arena.deallocate(newest, 80);
    arena.deallocate(middle, 96);
    ASSERT_EQ(arena.free_bytes(), quota);
    ASSERT_TRUE(arena.has_backing_space(quota, 64));
}

TEST(TimeWindowNamespaceTest, FragmentedNamedWindowsDeclineBeforeCopyingAnyRows) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const size_t backing = SwissTable<int64_t, NamespaceBlob>::initial_backing_bytes(16, false);
    const NamespaceBlob longer(127, 'c');
    int64_t windows = 0;
    while (arena->free_bytes() >= backing + 256) {
        const TimeWindow ns{windows, windows + 1};
        ASSERT_TRUE(arena->contains(state.put(0, ns, 0, NamespaceBlob("keep"))));
        ASSERT_TRUE(arena->contains(state.put(0, ns, 1, longer)));
        ++windows;
    }
    for (int64_t i = 0; arena->free_bytes() >= backing; ++i) {
        auto* padding = state.put(0, TimeWindow{-1000 - i, -999 - i}, 0, NamespaceBlob("pad"));
        if (!arena->contains(padding)) break;
    }
    for (int64_t i = 0; i + 1 < windows; ++i) {
        ASSERT_TRUE(state.remove(0, TimeWindow{i, i+1}, 1));
    }
    ASSERT_GT(arena->free_bytes(), 2 * backing);
    const auto before = state.namespace_cache_stats();
    const TimeWindow fresh{10000, 10100};
    ASSERT_FALSE(arena->contains(state.put(0, fresh, 7, NamespaceBlob("seven"))));
    state.put(0, fresh, 8, NamespaceBlob("eight"));
    const auto after = state.namespace_cache_stats();
    ASSERT_GT(after.blocked_backings, before.blocked_backings);
    ASSERT_EQ(after.failed_relocations, before.failed_relocations);
    ASSERT_FALSE(arena->contains(state.get(0, fresh, 8)));
    ASSERT_EQ(*state.get(0, fresh, 7), NamespaceBlob("seven"));
    ASSERT_EQ(*state.get(0, TimeWindow{0, 1}, 0), NamespaceBlob("keep"));
}

TEST(TimeWindowNamespaceTest, LocalReadPositionReusesOnlyTheCurrentLiveRow) {
    constexpr size_t quota = 4096 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 2, true);
    state.enable_namespace_storage(manager, quota);
    const NamespaceBlob original(80, 'O'), edited(80, 'E');
    for (int group = 0; group < 2; ++group) {
        for (int i = 0; i < 96; ++i) state.put(group, i, original);
    }
    for (int rep = 0; rep < 256; ++rep) {
        const int k = rep % 96;
        ASSERT_TRUE(state.get(0, k) != nullptr);
        state.put(0, k, edited);
    }
    const auto before = state.namespace_cache_stats();
    ASSERT_GT(before.read_position_writes, 100u);
    ASSERT_GT(before.read_position_bytes, 0u);
    ASSERT_TRUE(state.get(0, 12) != nullptr);
    ASSERT_TRUE(state.modify_in_place(0, 13, [](auto& v) { v[0] = 'X'; }));
    ASSERT_EQ(state.get(0, 12)->front(), 'E');
    ASSERT_EQ(state.get(1, 13)->front(), 'O');
    ASSERT_EQ(state.get(0, 13)->front(), 'X');
    state.prepare_snapshot();
    state.update_or_create(0, 13, [](auto& v) { v[0] = 'Y'; });
    ASSERT_TRUE(state.get(0, 13) != nullptr);
    ASSERT_TRUE(state.remove(0, 13));
    state.put(0, 13, original);
    for (int k = 96; k < 2600; ++k) state.put(0, k, original);
    ASSERT_EQ(state.get(0, 13)->front(), 'O');
    size_t frozen = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, const auto& v) {
        if (key == 13) ASSERT_EQ(v.front(), 'X');
        ++frozen;
    });
    ASSERT_EQ(frozen, 96u);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, ADisjointReadWriteStreamReleasesTheCursorAndRecurringPairsRecoverIt) {
    constexpr size_t quota = 4096 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, true);
    state.enable_namespace_storage(manager, quota);
    for (int g = 0; g < 2; ++g) for (int k = 0; k < 128; ++k) state.put(g, k, k);
    for (int n = 0; n < 200; ++n) {
        const auto k = n % 128;
        ASSERT_TRUE(state.get(n % 2, k) != nullptr);
        state.put(n % 2, (k + 1) % 128, 2000 + n);
    }
    auto paused = state.namespace_cache_stats();
    ASSERT_EQ(paused.read_position_writes, 0u);
    ASSERT_EQ(paused.read_position_pauses, 1u);
    ASSERT_EQ(paused.read_position_bytes, 0u);
    for (int n = 0; n < 2400; ++n) {
        const int g = n % 2, k = (n / 2) % 128;
        ASSERT_TRUE(state.get(g, k) != nullptr);
        state.put(g, k, 10000 + n);
    }
    const auto resumed = state.namespace_cache_stats();
    ASSERT_TRUE(resumed.read_position_writes > 1300);
    ASSERT_TRUE(resumed.read_position_bytes <= 64);
    for (int n = 2144; n < 2400; ++n) ASSERT_EQ(*state.get(n % 2, (n / 2) % 128), 10000 + n);
}

TEST(TimeWindowNamespaceTest, SparseSnapshotMarksSurviveRehashRemovalReinsertionAndNestedScans) {
    constexpr size_t quota = 1u << 18;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, true);
    state.enable_namespace_storage(manager, quota);
    for (int key = 0; key < 890; ++key) state.put(0, key, key + 100);
    state.prepare_snapshot();
    for (int key = 0; key < 14; ++key) state.put(0, key, -key);
    for (int key = 890; key < 920; ++key) state.put(0, key, -key);
    for (int key = 900; key < 915; ++key) ASSERT_TRUE(state.remove(0, key));
    ASSERT_TRUE(state.remove(0, 10));
    state.put(0, 10, -10);
    ASSERT_TRUE(state.remove(0, 30));
    auto verify = [](size_t& rows, int64_t key, int64_t value) {
        ASSERT_TRUE(key >= 0 && key < 890);
        ASSERT_EQ(value, key + 100);
        ++rows;
    };
    size_t outer = 0, nested = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t key, int64_t value) {
        if (!outer) state.for_each_snapshot_in_key_group(0, [&](int64_t k, int64_t v) { verify(nested, k, v); });
        verify(outer, key, value);
    });
    ASSERT_EQ(outer, 890u); ASSERT_EQ(nested, 890u);
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.snapshot_index_scans, 1u);
    ASSERT_TRUE(stats.snapshot_lookup_skips >= 850u);
    state.release_snapshot();
    ASSERT_EQ(*state.get(0, 10), -10);
    ASSERT_TRUE(state.get(0, 30) == nullptr);
}

TEST(TimeWindowNamespaceTest, FoldedSnapshotMarksRemainScopedToTheirWindowAndGroup) {
    // Small scratch wraps across many live slot blocks in each namespace.
    constexpr size_t quota = 48 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    for (int kg = 0; kg < 2; ++kg) for (int64_t w = 1; w <= 2; ++w) {
        for (int key = 0; key < 2200; ++key) state.put(kg, w, key, 10000 * kg + 3000 * w + key);
    }
    state.prepare_snapshot();
    for (int kg = 0; kg < 2; ++kg) {
        for (int key = 5; key < 2050; key += 137) state.put(kg, int64_t{1}, key, -100);
        ASSERT_TRUE(state.remove(kg, int64_t{2}, 99));
        state.put(kg, int64_t{1}, 3100, -1);
        size_t rows = 0;
        state.for_each_snapshot_in_key_group_ns<int64_t>(kg, [&](int64_t w, int64_t key, int64_t value) {
            ASSERT_TRUE(key < 2200);
            ASSERT_EQ(value, 10000 * kg + 3000 * w + key);
            ++rows;
        });
        ASSERT_EQ(rows, 4400u);
    }
    const auto stats = state.namespace_cache_stats();
    ASSERT_TRUE(stats.snapshot_index_bytes <= 64);
    ASSERT_EQ(stats.snapshot_index_scans, 2u);
    ASSERT_TRUE(stats.snapshot_lookup_skips > 3800);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, CollidingNamespacePositionsCannotChangeAnotherWindowsRow) {
    constexpr size_t quota = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 1, false);
    state.enable_namespace_storage(manager, quota);
    for (int64_t ns = 0; ns < 48; ++ns) {
        state.put(0, ns, 1, ns); state.put(0, ns, 2, -1);
    }
    for (int i = 0; i < 64; ++i) ASSERT_TRUE(state.get(0, int64_t{0}, 1) != nullptr);
    for (int64_t ns = 0; ns < 48; ++ns) {
        ASSERT_EQ(*state.get(0, ns, 1), ns);
        ASSERT_TRUE(state.get(0, (ns + 7) % 48, 2) != nullptr);
        state.put(0, ns, 1, 100 + ns);
    }
    for (int64_t ns = 0; ns < 48; ++ns) {
        ASSERT_EQ(*state.get(0, ns, 1), 100 + ns);
        ASSERT_EQ(*state.get(0, ns, 2), -1);
        ASSERT_TRUE(state.remove(0, ns, 1));
        ASSERT_TRUE(state.remove(0, ns, 2));
        state.put(0, ns + 100, 1, ns + 100);
    }
    for (int64_t ns = 0; ns < 48; ++ns) {
        ASSERT_TRUE(state.get(0, ns, 1) == nullptr);
        ASSERT_EQ(*state.get(0, ns + 100, 1), ns + 100);
    }
}

TEST(TimeWindowNamespaceTest, UnusedPayloadReservationReturnsItsGapOnRollback) {
    constexpr size_t quota = 16 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, quota);
    void* backing = nullptr;
    {
        NamespaceCacheArena::Packing space(arena, 128);
        ASSERT_TRUE(space.active());
        ASSERT_TRUE(space.reserve_payload(1000, 16));
        backing = arena.allocate(128, 16);
        void* small = arena.allocate(32, 16);
        ASSERT_FALSE(space.used_payload());
        ASSERT_THROW(arena.allocate(quota, 64), NamespaceCacheFull);
        arena.deallocate(small, 32);
    }
    ASSERT_EQ(arena.free_bytes(), quota - 128);
    arena.deallocate(backing, 128);
    ASSERT_TRUE(arena.has_backing_space(quota, 64));
}

TEST(TimeWindowNamespaceTest, FragmentedWholeTableHoldsOneGapForALateLargeValue) {
    constexpr size_t quota = 128 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, true);
    const NamespaceBlob small(127, 's'), large(1023, 'L');
    for (int64_t k = 0; k < 8; ++k) state.put(0, k, small);
    int64_t last_key = -1;
    state.for_each_in_key_group(0, [&](int64_t k, const auto&) { last_key = k; });
    state.put(0, last_key, large);
    state.enable_namespace_storage(manager, quota);
    auto& arena = *const_cast<NamespaceCacheArena*>(state.namespace_arena());
    std::vector<std::pair<void*, size_t>> anchors, gaps;
    auto allocate = [&](auto& owner, size_t bytes, size_t align) {
        owner.emplace_back(arena.allocate(bytes, align), bytes);
    };
    allocate(gaps, state.get_table(0)->packed_backing_bytes(), 64);
    allocate(anchors, 64, 16);
    allocate(gaps, large.size() + 1, 16);
    allocate(anchors, 64, 16);
    for (int i = 0; i < 7; ++i) {
        allocate(gaps, small.size() + 1, 16);
        allocate(anchors, 64, 16);
    }
    allocate(anchors, arena.free_bytes(), 16);
    for (const auto& [ptr, size] : gaps) arena.deallocate(ptr, size);

    for (int i = 0; i < 256 && !state.namespace_cache_stats().promotions; ++i) {
        ASSERT_TRUE(state.get(0, last_key) != nullptr);
        ASSERT_TRUE(state.modify_in_place(0, last_key, [](auto& value) { value.front() = 'L'; }));
    }
    const auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.failed_relocations, 1u);
    ASSERT_EQ(stats.reserved_payload_promotions, 1u);
    ASSERT_EQ(stats.promotions, 1u);
    state.prepare_snapshot();
    for (int64_t k = 0; k < 8; ++k) {
        auto* value = state.get(0, k);
        ASSERT_TRUE(arena.contains(value));
        ASSERT_TRUE(arena.contains(value->data()));
        ASSERT_EQ(*value, k == last_key ? large : small);
        ASSERT_TRUE(state.remove(0, k));
    }
    size_t frozen = 0;
    state.for_each_snapshot_in_key_group(0, [&](int64_t k, const auto& value) {
        ASSERT_EQ(value, k == last_key ? large : small);
        ++frozen;
    });
    ASSERT_EQ(frozen, 8u);
    state.release_snapshot();
    for (const auto& [ptr, size] : anchors) arena.deallocate(ptr, size);
    // The empty root still owns its controls and slots. Every nested
    // allocation, including the protected payload, has been returned.
    const size_t remaining = NamespaceCacheArena::allocation_bytes(state.get_table(0)->packed_backing_bytes());
    ASSERT_EQ(arena.free_bytes(), quota - remaining);
}

TEST(TimeWindowNamespaceTest, SustainedLargeTableBorrowsSparePoolAndReturnsItWhenEmpty) {
    constexpr size_t primary = 8 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(32 * primary,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 1, true);
    state.enable_namespace_storage(manager, primary);
    const auto* arena = state.namespace_arena();
    NamespaceBlob large(5 * primary, 'L');
    ASSERT_TRUE(large.size() > arena->maximum_capacity());
    state.put(0, 7, large);
    ASSERT_FALSE(arena->contains(state.get(0, 7)));
    ASSERT_EQ(state.namespace_cache_stats().whole_table_loans, 0u);
    for (int i = 0; i < 4000 && !arena->has_table_loan(); ++i) {
        state.update_or_create(0, 7, [](auto& v) { v[1] = 'h'; });
    }
    ASSERT_EQ(state.namespace_cache_stats().whole_table_loans, 1u);
    auto* value = state.get(0, 7);
    ASSERT_TRUE(arena->contains(value));
    ASSERT_TRUE(arena->contains(value->data()));
    ASSERT_EQ((*value)[1], 'h');
    ASSERT_TRUE(manager->free_sets() >= manager->total_sets() / 4);
    StateTable<int64_t, int64_t> neighbor(0, 1, true);
    neighbor.enable_namespace_storage(manager, primary);
    ASSERT_EQ(neighbor.namespace_arena()->primary_capacity(), primary);
    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, 7));
    ASSERT_EQ(state.namespace_cache_stats().empty_root_returns, 1u);
    ASSERT_EQ(arena->capacity(), primary);
    ASSERT_EQ(state.get(0, 7), nullptr);
    size_t rows = 0;
    state.for_each_snapshot_in_key_group(0, [&](auto k, const auto& prev) {
        ASSERT_EQ(k, 7); ASSERT_EQ(prev.size(), large.size()); ASSERT_EQ(prev[1], 'h'); ++rows;
    });
    ASSERT_EQ(rows, 1u);
    state.release_snapshot();
    state.put(0, 8, NamespaceBlob("again"));
    ASSERT_EQ(*state.get(0, 8), NamespaceBlob("again"));
}

TEST(TimeWindowNamespaceTest, NegativeRootReadsDoNotDisplaceARepeatedlyUsedResident) {
    constexpr size_t quota = 4 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota * 2,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, true);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const int64_t key = 7;
    for (int group = 0; group < 3; ++group) {
        state.put(group, key, 100 + group);
        state.put(group, key, 100 + group);
    }
    for (int i = 0; i < 260; ++i) {
        ASSERT_EQ(*state.get(0, key), 100);
        ASSERT_EQ(state.get(1, int64_t{1000} + i), nullptr);
        ASSERT_EQ(*state.get(2, key), 102);
        state.put(2, key, 102);
    }
    ASSERT_EQ(state.namespace_cache_stats().evictions, 1u);
    ASSERT_TRUE(arena->contains(state.get(0, key)));
    ASSERT_FALSE(arena->contains(state.get(1, key)));
    ASSERT_TRUE(arena->contains(state.get(2, key)));
}

TEST(TimeWindowNamespaceTest, TwoProvenLargeNamespacesCanResideTogetherAndReleaseIndependently) {
    constexpr size_t primary = 8 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(primary * 32,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 2, true);
    state.enable_namespace_storage(manager, primary);
    auto* arena = state.namespace_arena();
    for (int g = 0; g < 2; ++g) {
        state.put(g, 7, NamespaceBlob(6 * primary, char('a' + g)));
        for (int i = 0; i < 30000 && !arena->contains(state.get(g, 7)->data()); ++i) {
            state.update_or_create(g, 7, [g](auto& value) { value[0] = char('a' + g); });
        }
    }
    ASSERT_EQ(state.namespace_cache_stats().resident_tables, 2u);
    ASSERT_EQ(arena->peak_table_loans(), 2u);
    for (int g = 0; g < 2; ++g) {
        auto* value = state.get(g, 7);
        ASSERT_TRUE(arena->contains(value)); ASSERT_TRUE(arena->contains(value->data()));
        ASSERT_EQ(value->back(), char('a' + g));
    }
    ASSERT_GE(manager->free_sets(), manager->total_sets() / 4);
    const size_t both = arena->capacity();
    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, 7));
    ASSERT_TRUE(arena->capacity() < both);
    ASSERT_TRUE(arena->contains(state.get(1, 7)->data()));
    state.for_each_snapshot_in_key_group(0, [&](auto key, const auto& old) {
        ASSERT_EQ(key, 7); ASSERT_EQ(old.size(), 6 * primary); ASSERT_EQ(old.back(), 'a');
    });
    state.release_snapshot();
    ASSERT_TRUE(state.remove(1, 7));
    ASSERT_EQ(arena->capacity(), primary);
    ASSERT_EQ(manager->free_sets(), manager->total_sets() - primary / sizeof(HotSet));
}

TEST(TimeWindowNamespaceTest, WindowDirectoryValidatesIdentityOnCollisionEmptyAndRecreation) {
    constexpr size_t quota = 1u << 20;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    for (int g = 0; g < 2; ++g) {
        for (int64_t end = 1; end <= 16; ++end) {
            const TimeWindow window{-1, end};
            ASSERT_EQ(state.get(g, window, int64_t{9}), nullptr);
            state.put(g, window, int64_t{9}, int64_t{100} * g + end);
        }
    }
    ASSERT_EQ(state.namespace_cache_stats().directories_built, 0u);
    for (int64_t end = 1; end <= 16; ++end) {
        ASSERT_EQ(*state.get(0, TimeWindow{-1, end}, int64_t{9}), end);
    }
    ASSERT_EQ(state.namespace_cache_stats().directories_built, 1u);
    ASSERT_TRUE(state.namespace_cache_stats().directory_bytes <= quota / 32);
    for (int g = 0; g < 2; ++g) {
        for (int64_t end = 1; end <= 16; ++end) {
            const TimeWindow window{-1, end};
            ASSERT_EQ(*state.get(g, window, int64_t{9}), 100 * g + end);
            ASSERT_TRUE(state.remove(g, window, int64_t{9}));
            ASSERT_EQ(state.get(g, window, int64_t{9}), nullptr);
            state.put(g, window, int64_t{9}, -end);
            ASSERT_EQ(*state.get(g, window, int64_t{9}), -end);
        }
    }
}

TEST(TimeWindowNamespaceTest, AResidentPayloadCanReclaimTheNamespaceDirectory) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const TimeWindow successor{-100, 40};
    ASSERT_EQ(state.get(1, successor, 11), nullptr);
    for (int64_t w = 0; w < 16; ++w) {
        state.put(0, TimeWindow{w, w + 1}, 7, NamespaceBlob("seed"));
    }
    for (int64_t w = 0; w < 16; ++w) {
        ASSERT_EQ(*state.get(0, TimeWindow{w, w + 1}, 7), NamespaceBlob("seed"));
    }
    const size_t bytes = state.namespace_cache_stats().directory_bytes;
    ASSERT_GT(bytes, 512u);
    state.put(1, successor, 11, NamespaceBlob("arrived while the wide directory was active"));
    const size_t backing = SwissTable<int64_t, NamespaceBlob>::initial_backing_bytes(16, false);
    for (int64_t w = 16; arena->free_bytes() >= backing; ++w) {
        ASSERT_TRUE(arena->contains(state.put(0, TimeWindow{w, w + 1}, 7, NamespaceBlob("fill"))));
    }
    ASSERT_EQ(state.namespace_cache_stats().directories_released, 0u);
    const NamespaceBlob row(bytes - 32, 'p');
    auto* current = state.put(0, TimeWindow{0, 1}, 7, row);
    ASSERT_EQ(*current, row);
    ASSERT_TRUE(arena->contains(current->data()));
    ASSERT_EQ(state.namespace_cache_stats().directories_released, 1u);
    ASSERT_EQ(state.namespace_cache_stats().directory_bytes, 0u);
    ASSERT_EQ(state.namespace_cache_stats().growth_demotions, 0u);
    auto* retained = state.get(1, successor, 11);
    ASSERT_NE(retained, nullptr);
    ASSERT_EQ(*retained, NamespaceBlob("arrived while the wide directory was active"));
}

TEST(TimeWindowNamespaceTest, NamespaceSpansUseTheirBudgetInUnitsOfOneAlignedSet) {
    constexpr size_t unit = sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(37 * unit,
            [](L0LibBindings*, std::string*) { return false; });
    auto first = manager->acquire_namespace_span(11);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->num_sets(), 11u);
    auto second = manager->borrow_namespace_span(7, 12);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(second->num_sets(), 7u);
    ASSERT_EQ(manager->free_sets(), 19u);
    ASSERT_EQ(manager->borrow_namespace_span(8, 12), nullptr);
    auto scalar = manager->acquire_ll(11);
    ASSERT_NE(scalar, nullptr);
    ASSERT_EQ(scalar->num_sets(), 8u);
    manager->release_ll(first.release());
    manager->release_ll(second.release());
    manager->release_ll(scalar.release());
    ASSERT_EQ(manager->free_sets(), 37u);
}

TEST(TimeWindowNamespaceTest, AQualifiedTableBorrowsOnlyTheLinearExtentItNeeds) {
    constexpr size_t unit = sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(100 * unit,
            [](L0LibBindings*, std::string*) { return false; });
    NamespaceCacheArena arena(manager, 6 * unit);
    ASSERT_EQ(arena.primary_capacity(), 6 * unit);
    ASSERT_EQ(arena.maximum_capacity(), 24 * unit);
    ASSERT_TRUE(arena.try_borrow_whole_table(27 * unit - 32));
    ASSERT_EQ(arena.capacity(), 33 * unit);
    auto* payload = arena.allocate(27 * unit - 32, 64);
    ASSERT_TRUE(arena.contains(payload));
    ASSERT_EQ(reinterpret_cast<uintptr_t>(payload) % 64, 0u);
    arena.deallocate(payload, 27 * unit - 32);
    arena.release_empty_loans();
    ASSERT_EQ(arena.capacity(), 6 * unit);
    ASSERT_EQ(manager->free_sets(), 94u);
}

TEST(TimeWindowNamespaceTest, RetiredWindowBackingHasOneNewOwnerAndNoOldRows) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceBlob> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    const NamespaceBlob payload(95, 'x');
    state.put(1, int64_t{17}, 7, NamespaceBlob("other"));
    state.put(0, int64_t{17}, 7, NamespaceBlob("overlap"));
    for (int64_t w = 0; w < 5; ++w) {
        state.put(0, TimeWindow{w, w + 1}, 7, payload);
        state.put(0, TimeWindow{w, w + 1}, 8, payload);
        ASSERT_TRUE(state.remove(0, TimeWindow{w, w + 1}, 7));
        ASSERT_TRUE(state.remove(0, TimeWindow{w, w + 1}, 8));
    }
    const auto idle = state.namespace_cache_stats();
    ASSERT_GT(idle.idle_window_bytes, 0u);
    ASSERT_TRUE(idle.idle_window_bytes <= quota / 16);
    ASSERT_EQ(idle.resident_bytes + idle.idle_window_bytes, quota - arena->free_bytes());
    ASSERT_EQ(idle.resident_tables, 2u);
    ASSERT_GT(idle.reused_windows, 0u);
    ASSERT_TRUE(arena->contains(state.put(0, std::string("next"), 9, payload)));
    ASSERT_EQ(state.namespace_cache_stats().reused_windows, idle.reused_windows + 1);
    ASSERT_EQ(state.namespace_cache_stats().idle_window_bytes, 0u);
    for (int64_t w = 0; w < 5; ++w) {
        ASSERT_EQ(state.get(0, TimeWindow{w, w + 1}, 7), nullptr);
        ASSERT_EQ(state.get(0, TimeWindow{w, w + 1}, 8), nullptr);
    }
    ASSERT_EQ(state.get(0, std::string("next"), 7), nullptr);
    ASSERT_EQ(*state.get(1, int64_t{17}, 7), NamespaceBlob("other"));
    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, std::string("next"), 9));
    state.put(0, std::string("next"), 9, NamespaceBlob("replacement"));
    state.put(0, std::string("later"), 1, payload);
    size_t observed = 0;
    state.for_each_snapshot_in_key_group_ns<std::string>(0, [&](const auto& ns, auto key, const auto& value) {
        ASSERT_EQ(ns, std::string("next")); ASSERT_EQ(key, 9); ASSERT_EQ(value, payload); ++observed;
    });
    ASSERT_EQ(observed, 1u);
    state.release_snapshot();
    ASSERT_EQ(*state.get(0, std::string("next"), 9), NamespaceBlob("replacement"));
}

TEST(TimeWindowNamespaceTest, RepeatedMediumWindowsTransferEmptyBackingsAcrossKeyGroups) {
    constexpr size_t budget = 512 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(budget,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, NamespaceRow> state(0, 3, false, &DefaultAllocator::instance(), 16);
    // Storage may be attached after a namespace already exists.
    const std::string text(100, 'x');
    const NamespaceRow row(text.data(), text.size());
    state.put(2, int64_t{-1}, 200, row);
    state.enable_namespace_storage(manager, budget);
    for (int group = 0; group < 2; ++group) {
        for (int64_t batch = 0; batch < 6; ++batch) {
            TimeWindow window{batch * 10, batch * 10 + 10};
            for (int key = 0; key < 20; ++key) {
                ASSERT_EQ(state.get(group, window, key), nullptr);
                state.put(group, window, key, row);
            }
            for (int key = 0; key < 20; ++key) {
                auto* current = state.get(group, window, key);
                ASSERT_TRUE(current != nullptr);
                ASSERT_TRUE(state.namespace_arena()->contains(current->data()));
                ASSERT_EQ(std::string_view(current->data(), current->size()), std::string_view(text));
                ASSERT_TRUE(state.remove(group, window, key));
            }
            ASSERT_EQ(state.get(group, window, 19), nullptr);
        }
    }
    auto stats = state.namespace_cache_stats();
    ASSERT_GE(stats.learned_window_sizes, 2u);
    ASSERT_GE(stats.shared_window_reuses, 4u);
    ASSERT_GT(stats.idle_window_bytes, 0u);
    ASSERT_TRUE(state.remove(2, int64_t{-1}, 200));
    ASSERT_EQ(state.namespace_cache_stats().idle_window_bytes, 0u);
    ASSERT_EQ(state.total_size(), 0u);
}

TEST(TimeWindowNamespaceTest, EmptyMediumWindowsAreReusedDuringAnActiveSnapshot) {
    constexpr size_t quota = 128 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    state.put(1, TimeWindow{-1, 0}, 0, -1);

    const auto add_window = [&](int64_t first, int64_t offset) {
        const TimeWindow window{first, first + 1};
        for (int64_t key = 0; key < 20; ++key) {
            ASSERT_TRUE(state.namespace_arena()->contains(state.put(0, window, key, offset + key)));
        }
    };
    const auto remove_window = [&](int64_t first) {
        for (int64_t key = 0; key < 20; ++key) {
            ASSERT_TRUE(state.remove(0, TimeWindow{first, first + 1}, key));
        }
    };
    for (int64_t window = 0; window < 4; ++window) {
        add_window(window, window * 100);
        remove_window(window);
    }
    for (int64_t window = 10; window < 14; ++window) add_window(window, window * 100);
    state.prepare_snapshot();
    const auto before = state.namespace_cache_stats().batched_window_reuses;
    for (int64_t window = 10; window < 14; ++window) remove_window(window);
    add_window(10, 3000);
    add_window(20, 4000);
    add_window(21, 5000);
    ASSERT_GT(state.namespace_cache_stats().batched_window_reuses, before);

    size_t visited = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
            [&](const auto& window, int64_t key, int64_t value) {
        ASSERT_TRUE(window.first >= 10 && window.first < 14);
        ASSERT_EQ(window.second, window.first + 1);
        ASSERT_EQ(value, window.first * 100 + key);
        ++visited;
    });
    ASSERT_EQ(visited, 80u);
    ASSERT_EQ(*state.get(0, TimeWindow{10, 11}, 7), 3007);
    ASSERT_EQ(*state.get(0, TimeWindow{21, 22}, 7), 5007);
    ASSERT_EQ(state.get(0, TimeWindow{11, 12}, 7), nullptr);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, ReusesEmptyBackingAcrossDifferentWindowSizes) {
    constexpr size_t quota = 128 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 3, false, &DefaultAllocator::instance(), 16);
    state.enable_namespace_storage(manager, quota);
    auto* arena = state.namespace_arena();
    const TimeWindow old_window{10, 20}, new_window{20, 30}, sentinel{0, 100};
    state.put(2, sentinel, 0, 99);
    for (int64_t key = 0; key < 20; ++key)
        ASSERT_TRUE(arena->contains(state.put(0, old_window, key, 100 + key)));
    state.prepare_snapshot();
    for (int64_t key = 0; key < 20; ++key)
        ASSERT_TRUE(state.remove(0, old_window, key));
    const auto before = state.namespace_cache_stats();
    ASSERT_GT(before.idle_window_bytes, 0u);
    auto free_bytes = arena->free_bytes();
    for (int64_t key = 0; key < 20; ++key)
        ASSERT_TRUE(arena->contains(state.put(1, new_window, key, 200 + key)));
    const auto after = state.namespace_cache_stats();
    ASSERT_EQ(arena->free_bytes(), free_bytes);
    ASSERT_EQ(after.batched_window_reuses, before.batched_window_reuses + 1);
    ASSERT_EQ(after.idle_window_bytes, 0u);
    ASSERT_EQ(state.get(0, old_window, 19), nullptr);
    ASSERT_EQ(*state.get(1, new_window, 19), 219);
    size_t saved = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
            [&](const TimeWindow& window, int64_t key, int64_t value) {
                ASSERT_EQ(window, old_window);
                ASSERT_EQ(value, 100 + key);
                ++saved;
            });
    ASSERT_EQ(saved, 20u);
    state.release_snapshot();
}

TEST(TimeWindowNamespaceTest, NewWholeTablesReclaimIdleWindowsBeforeLiveTables) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    auto* arena = state.namespace_arena();
    state.put(0, int64_t{-10}, 9, 99);
    for (int64_t w = 0; w < 5; ++w) {
        state.put(0, w, 7, w);
        ASSERT_TRUE(state.remove(0, w, 7));
    }
    const auto idle = state.namespace_cache_stats().idle_window_bytes;
    ASSERT_GT(idle, 0u);
    const size_t count = quota / idle - 1;
    for (size_t w = 0; w < count; ++w) {
        ASSERT_TRUE(arena->contains(state.put(1, static_cast<int64_t>(w), 9, w + 10)));
    }
    auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.idle_window_bytes, 0u);
    ASSERT_EQ(stats.released_idle_windows, 1u);
    ASSERT_EQ(stats.evictions, 0u);
    ASSERT_EQ(stats.resident_tables, count + 1);
    ASSERT_EQ(stats.resident_bytes, quota - arena->free_bytes());
    for (size_t w = 0; w < count; ++w) {
        ASSERT_EQ(*state.get(1, static_cast<int64_t>(w), 9), static_cast<int64_t>(w + 10));
    }
}

TEST(TimeWindowNamespaceTest, OverlapLearnsSmallWindowsBeforeAnyExpireAndRelearnsGrowth) {
    constexpr size_t quota = 256 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false, &DefaultAllocator::instance(), 64);
    state.enable_namespace_storage(manager, quota);
    const auto* arena = state.namespace_arena();
    for (int64_t w = 0; w < 8; ++w) {
        state.put(0, TimeWindow{w, w + 9}, 1, w);
    }
    ASSERT_EQ(state.namespace_cache_stats().overlap_compactions, 1u);
    using Table = SwissTable<int64_t, int64_t>;
    const size_t small = Table::initial_backing_bytes(4, true);
    const size_t original = Table::initial_backing_bytes(32, false);
    ASSERT_EQ(quota - arena->free_bytes(), 4 * original + 4 * small);
    auto before = arena->free_bytes();
    state.put(1, TimeWindow{100, 120}, 1, 2);
    ASSERT_EQ(before - arena->free_bytes(), original);
    for (int64_t key = 2; key <= 12; ++key) state.put(0, TimeWindow{7, 16}, key, key);
    before = arena->free_bytes();
    state.put(0, TimeWindow{20, 40}, 1, 100);
    ASSERT_EQ(before - arena->free_bytes(), Table::initial_backing_bytes(16, false));
    for (int64_t w = 0; w < 8; ++w) ASSERT_EQ(*state.get(0, TimeWindow{w, w + 9}, 1), w);
    for (int64_t key = 2; key <= 12; ++key) ASSERT_EQ(*state.get(0, TimeWindow{7, 16}, key), key);
}

TEST(TimeWindowNamespaceTest, SuccessiveBurstsReuseOnlyVacantWholeWindowTables) {
    constexpr size_t quota = 128 * 1024;
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false);
    state.enable_namespace_storage(manager, quota);
    for (int key = 0; key < 8; ++key) state.put(1, TimeWindow{-100, -1}, key, key);
    for (int64_t wave = 0; wave < 12; ++wave) {
        for (int64_t i = 0; i < 32; ++i) {
            const TimeWindow ns{wave, wave * 100 + i + 1};
            ASSERT_EQ(state.get(0, ns, i), nullptr);
            state.put(0, ns, i, 1000 + i);
            ASSERT_EQ(*state.get(0, ns, i), 1000 + i);
            ASSERT_EQ(*state.get(0, ns, i), 1000 + i);
        }
        if (wave == 5) state.prepare_snapshot();
        for (int64_t i = 0; i < 32; ++i) {
            const TimeWindow ns{wave, wave * 100 + i + 1};
            ASSERT_TRUE(state.remove(0, ns, i));
            ASSERT_EQ(state.get(0, ns, i), nullptr);
        }
        if (wave == 5) {
            int saved = 0;
            state.for_each_snapshot_in_key_group_ns<TimeWindow>(0,
                    [&](const TimeWindow& ns, const int64_t& key, const int64_t& value) {
                        ASSERT_EQ(ns.first, wave);
                        ASSERT_EQ(ns.second, wave * 100 + key + 1);
                        ASSERT_EQ(value, 1000 + key);
                        ++saved;
                    });
            ASSERT_EQ(saved, 32);
            state.release_snapshot();
        }
        ASSERT_TRUE(state.namespace_cache_stats().idle_window_bytes <= quota / 16);
    }
    ASSERT_GT(state.namespace_cache_stats().batched_window_reuses, 30u);
    for (int key = 0; key < 8; ++key) ASSERT_TRUE(state.remove(1, TimeWindow{-100, -1}, key));
    ASSERT_EQ(state.total_size(), 0u);
    ASSERT_EQ(state.namespace_cache_stats().idle_window_bytes, 0u);
}

TEST(TimeWindowNamespaceTest, DisjointTinyWindowsRecycleEmptyStorageWithoutPublishingOldKeys) {
    constexpr size_t quota = 1024 * sizeof(HotSet);
    auto manager = std::make_shared<HotCacheManager>(quota,
            [](L0LibBindings*, std::string*) { return false; });
    StateTable<int64_t, int64_t> state(0, 2, false, &DefaultAllocator::instance(), 64);
    state.enable_namespace_storage(manager, quota);
    for (int64_t i = 0; i < 64; ++i) {
        const int group = int(i % 2);
        const TimeWindow window{i * 17, i * 17 + 41};
        ASSERT_EQ(state.get(group, window, i - 2), nullptr);
        state.put(group, window, i, i + 31);
        ASSERT_EQ(*state.get(group, window, i), i + 31);
        ASSERT_TRUE(state.remove(group, window, i));
        ASSERT_EQ(state.total_size(), 0u);
    }
    auto stats = state.namespace_cache_stats();
    ASSERT_EQ(stats.resident_tables, 0u);
    ASSERT_EQ(stats.resident_bytes, 0u);
    ASSERT_GE(stats.idle_successors, 48u);
    ASSERT_GE(stats.shared_window_reuses, 48u);
    ASSERT_EQ(stats.directory_node_reuses, 63u);
    ASSERT_TRUE(stats.idle_window_bytes > 0 && stats.idle_window_bytes <= quota / 128);
    const TimeWindow last{-91, 7};
    state.put(0, last, 77, 44);
    state.prepare_snapshot();
    ASSERT_TRUE(state.remove(0, last, 77));
    state.put(0, TimeWindow{2, 3}, 77, 55);
    size_t frozen = 0;
    state.for_each_snapshot_in_key_group_ns<TimeWindow>(0, [&](const auto& ns, auto key, auto value) {
        ASSERT_EQ(ns, last); ASSERT_EQ(key, 77); ASSERT_EQ(value, 44); ++frozen;
    });
    ASSERT_EQ(frozen, 1u);
    state.release_snapshot();
}
