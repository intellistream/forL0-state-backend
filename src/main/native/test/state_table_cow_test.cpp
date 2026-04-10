// StateTable COW (Copy-on-Write) snapshot tests.
// Verifies prepare_snapshot → put/remove → for_each_snapshot consistency.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "state_engine.h"

#include <map>
#include <set>

using namespace forl0;

// ============================================================================
//  Basic VoidNamespace Operations (no snapshot)
// ============================================================================

TEST(StateTableCOWTest, VoidNamespacePutGet) {
    StateTable<int64_t, int64_t> st(0, 4, true);

    int64_t* val = st.put(0, 100, 1000);
    ASSERT_EQ(*val, 1000);

    int64_t* retrieved = st.get(0, 100);
    ASSERT_NE(retrieved, nullptr);
    ASSERT_EQ(*retrieved, 1000);

    ASSERT_EQ(st.get(0, 999), nullptr);
}

TEST(StateTableCOWTest, VoidNamespaceRemove) {
    StateTable<int64_t, std::string> st(0, 4, true);

    st.put(0, 10, std::string("hello"));
    st.put(0, 20, std::string("world"));

    bool removed = st.remove(0, 10);
    ASSERT_TRUE(removed);
    ASSERT_EQ(st.get(0, 10), nullptr);
    ASSERT_NE(st.get(0, 20), nullptr);
}

TEST(StateTableCOWTest, VoidNamespaceForEach) {
    StateTable<int64_t, int64_t> st(0, 2, true);

    st.put(0, 1, 100);
    st.put(0, 2, 200);
    st.put(1, 3, 300);
    st.put(1, 4, 400);

    // Iterate all key groups
    int count_all = 0;
    st.for_each_void([&count_all](int, const int64_t&, const int64_t&) {
        ++count_all;
    });
    ASSERT_EQ(count_all, 4);

    // Iterate key group 0 only
    int count_kg0 = 0;
    st.for_each_in_key_group(0, [&count_kg0](const int64_t&, const int64_t&) {
        ++count_kg0;
    });
    ASSERT_EQ(count_kg0, 2);
}

// ============================================================================
//  COW Snapshot — Basic
// ============================================================================

TEST(StateTableCOWTest, SnapshotSeesOldValues) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    st.put(0, 100, 10);
    st.put(0, 200, 20);

    // Begin snapshot
    st.prepare_snapshot();

    // Modify after snapshot
    st.put(0, 100, 100);   // overwrite
    st.put(0, 300, 30);    // add new key

    // Snapshot iteration sees old values
    std::map<int64_t, int64_t> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const int64_t& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state.size(), 2u);
    ASSERT_EQ(snapshot_state[100], 10);   // old value
    ASSERT_EQ(snapshot_state[200], 20);
    ASSERT_EQ(snapshot_state.count(300), 0u);  // not in snapshot

    st.release_snapshot();
}

TEST(StateTableCOWTest, SnapshotIncludesDeleted) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    st.put(0, 1, 100);
    st.put(0, 2, 200);
    st.put(0, 3, 300);

    st.prepare_snapshot();

    // Delete entry after snapshot — should still appear in snapshot
    st.remove(0, 2);

    std::map<int64_t, int64_t> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const int64_t& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state.size(), 3u);
    ASSERT_EQ(snapshot_state[2], 200);  // deleted key still in snapshot

    // Current state should not have key 2
    ASSERT_EQ(st.get(0, 2), nullptr);

    st.release_snapshot();
}

TEST(StateTableCOWTest, SnapshotMultipleOverwrites) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    st.put(0, 1, 10);

    st.prepare_snapshot();

    // Overwrite multiple times — snapshot should see original value
    st.put(0, 1, 20);
    st.put(0, 1, 30);
    st.put(0, 1, 40);

    std::map<int64_t, int64_t> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const int64_t& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state[1], 10);  // original value from before snapshot

    // Current value should be latest
    ASSERT_EQ(*st.get(0, 1), 40);

    st.release_snapshot();
}

// ============================================================================
//  COW Snapshot — Release and Re-snapshot
// ============================================================================

TEST(StateTableCOWTest, ReleaseAndRePrepare) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    st.put(0, 1, 100);

    // First snapshot
    st.prepare_snapshot();
    st.put(0, 1, 200);  // overwrite

    std::map<int64_t, int64_t> snap1;
    st.for_each_snapshot_in_key_group(0, [&snap1](const int64_t& k, const int64_t& v) {
        snap1[k] = v;
    });
    ASSERT_EQ(snap1[1], 100);

    st.release_snapshot();

    // Second snapshot — should see current value (200)
    st.prepare_snapshot();
    st.put(0, 1, 300);

    std::map<int64_t, int64_t> snap2;
    st.for_each_snapshot_in_key_group(0, [&snap2](const int64_t& k, const int64_t& v) {
        snap2[k] = v;
    });
    ASSERT_EQ(snap2[1], 200);  // value at time of second prepare

    st.release_snapshot();
}

// ============================================================================
//  COW Snapshot — No Active Snapshot
// ============================================================================

TEST(StateTableCOWTest, NoSnapshotForEachEqualsNormal) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    st.put(0, 1, 100);
    st.put(0, 2, 200);

    // Without snapshot, for_each_snapshot = for_each_in_key_group
    std::map<int64_t, int64_t> result;
    st.for_each_snapshot_in_key_group(0, [&result](const int64_t& k, const int64_t& v) {
        result[k] = v;
    });

    ASSERT_EQ(result.size(), 2u);
    ASSERT_EQ(result[1], 100);
    ASSERT_EQ(result[2], 200);
}

// ============================================================================
//  COW Stress — Heavy Modifications During Snapshot
// ============================================================================

TEST(StateTableCOWTest, StressHeavyModifications) {
    StateTable<int64_t, int64_t> st(0, 1, true);

    // Baseline: 100 entries
    for (int i = 0; i < 100; ++i) {
        st.put(0, i, i * 10);
    }

    st.prepare_snapshot();

    // Massive modifications after snapshot
    for (int i = 0; i < 100; ++i) {
        st.put(0, i, i * 20);  // overwrite all
    }
    for (int i = 100; i < 200; ++i) {
        st.put(0, i, i * 10);  // add 100 new
    }
    for (int i = 0; i < 50; ++i) {
        st.remove(0, i);       // delete first 50
    }

    // Snapshot should see original 100 entries with original values
    std::map<int64_t, int64_t> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const int64_t& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state.size(), 100u);
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(snapshot_state[i], i * 10);
    }
    // No new keys in snapshot
    for (int i = 100; i < 200; ++i) {
        ASSERT_EQ(snapshot_state.count(i), 0u);
    }

    st.release_snapshot();
}

// ============================================================================
//  StateTable — Multiple Key Groups
// ============================================================================

TEST(StateTableCOWTest, MultipleKeyGroupsIndependent) {
    StateTable<int64_t, int64_t> st(10, 3, true);

    st.put(10, 1, 100);
    st.put(11, 1, 200);
    st.put(12, 1, 300);

    ASSERT_EQ(*st.get(10, 1), 100);
    ASSERT_EQ(*st.get(11, 1), 200);
    ASSERT_EQ(*st.get(12, 1), 300);

    // Remove from one key group doesn't affect others
    st.remove(11, 1);
    ASSERT_EQ(*st.get(10, 1), 100);
    ASSERT_EQ(st.get(11, 1), nullptr);
    ASSERT_EQ(*st.get(12, 1), 300);
}

TEST(StateTableCOWTest, TotalSize) {
    StateTable<int64_t, int64_t> st(0, 3, true);

    st.put(0, 1, 10);
    st.put(0, 2, 20);
    st.put(1, 3, 30);
    st.put(2, 4, 40);
    st.put(2, 5, 50);

    ASSERT_EQ(st.total_size(), 5u);

    st.remove(0, 1);
    ASSERT_EQ(st.total_size(), 4u);
}

// ============================================================================
//  StateEngine — Snapshot Coordination
// ============================================================================

TEST(StateTableCOWTest, StateEngineSnapshotCoordination) {
    StateEngine engine(0, 2, 4);

    int64_t id1 = engine.register_state<int64_t, int64_t>("val1", true);
    int64_t id2 = engine.register_state<int64_t, std::string>("val2", true);

    auto* t1 = engine.get_state_table<int64_t, int64_t>(id1);
    auto* t2 = engine.get_state_table<int64_t, std::string>(id2);

    // Baseline
    t1->put(0, 1, 100);
    t2->put(0, 1, std::string("before"));

    // Prepare snapshot across all states
    uint64_t version = engine.prepare_snapshot();
    ASSERT_EQ(version, 1u);

    // Modify after snapshot
    t1->put(0, 1, 200);
    t2->put(0, 1, std::string("after"));

    // Verify COW works for int table
    std::map<int64_t, int64_t> snap1;
    t1->for_each_snapshot_in_key_group(0, [&snap1](const int64_t& k, const int64_t& v) {
        snap1[k] = v;
    });
    ASSERT_EQ(snap1[1], 100);

    // Verify COW works for string table
    std::map<int64_t, std::string> snap2;
    t2->for_each_snapshot_in_key_group(0, [&snap2](const int64_t& k, const std::string& v) {
        snap2[k] = v;
    });
    ASSERT_EQ(snap2[1], "before");

    engine.release_snapshot();
}
