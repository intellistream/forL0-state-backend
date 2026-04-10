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
