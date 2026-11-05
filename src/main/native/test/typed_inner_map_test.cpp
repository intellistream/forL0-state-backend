// TypedInnerMap tests — verifies MapState type specializations.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "state_engine.h"
#include "type_layout.h"
#include <map>
#include <limits>

using namespace forl0;

// ============================================================================
//  InnerMapLongLong
// ============================================================================

TEST(TypedInnerMapTest, LongLongBasicOps) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    map[1] = 100;
    map[2] = 200;
    map[3] = 300;
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 3u);
    ASSERT_EQ((*m)[1], 100);
    ASSERT_EQ((*m)[2], 200);
    ASSERT_EQ((*m)[3], 300);
}

TEST(TypedInnerMapTest, LongLongUpdate) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    map[1] = 100;
    st.put(0, 10, std::move(map));

    // Update inner map entry
    auto* m = st.get(0, static_cast<int64_t>(10));
    (*m)[1] = 999;
    (*m)[4] = 400;  // add new entry

    auto* m2 = st.get(0, static_cast<int64_t>(10));
    ASSERT_EQ((*m2)[1], 999);
    ASSERT_EQ((*m2)[4], 400);
}

TEST(TypedInnerMapTest, LongLongDelete) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map{{1, 100}, {2, 200}};
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    m->erase(1);

    ASSERT_EQ(m->size(), 1u);
    ASSERT_EQ(m->count(1), 0u);
    ASSERT_EQ((*m)[2], 200);
}

// ============================================================================
//  InnerMapLongString
// ============================================================================

TEST(TypedInnerMapTest, LongStringBasicOps) {
    StateTable<int64_t, InnerMapLongString> st(0, 1, true);

    InnerMapLongString map;
    map[1] = "hello";
    map[2] = "world";
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 2u);
    ASSERT_EQ((*m)[1], "hello");
    ASSERT_EQ((*m)[2], "world");
}

TEST(TypedInnerMapTest, LongStringEmptyValue) {
    StateTable<int64_t, InnerMapLongString> st(0, 1, true);

    InnerMapLongString map;
    map[1] = "";  // empty string value
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_EQ((*m)[1], "");
}

// ============================================================================
//  InnerMapStringLong
// ============================================================================

TEST(TypedInnerMapTest, StringLongBasicOps) {
    StateTable<int64_t, InnerMapStringLong> st(0, 1, true);

    InnerMapStringLong map;
    map["key1"] = 100;
    map["key2"] = 200;
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->size(), 2u);
    ASSERT_EQ((*m)["key1"], 100);
    ASSERT_EQ((*m)["key2"], 200);
}

// ============================================================================
//  InnerMap (generic string → string)
// ============================================================================

TEST(TypedInnerMapTest, GenericStringString) {
    StateTable<int64_t, InnerMap> st(0, 1, true);

    InnerMap map;
    map["uk1"] = "uv1";
    map["uk2"] = "uv2";
    st.put(0, 10, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(10));
    ASSERT_NE(m, nullptr);
    ASSERT_EQ((*m)["uk1"], "uv1");
    ASSERT_EQ((*m)["uk2"], "uv2");
}

// ============================================================================
//  Different MapState Types in Same StateEngine
// ============================================================================

TEST(TypedInnerMapTest, MultipleMapTypesInEngine) {
    StateEngine engine(0, 1, 4);

    int64_t id_ll = engine.register_state<int64_t, InnerMapLongLong>("ll_map", true);
    int64_t id_ls = engine.register_state<int64_t, InnerMapLongString>("ls_map", true);
    int64_t id_sl = engine.register_state<int64_t, InnerMapStringLong>("sl_map", true);
    int64_t id_ss = engine.register_state<int64_t, InnerMap>("ss_map", true);

    auto* t_ll = engine.get_state_table<int64_t, InnerMapLongLong>(id_ll);
    auto* t_ls = engine.get_state_table<int64_t, InnerMapLongString>(id_ls);
    auto* t_sl = engine.get_state_table<int64_t, InnerMapStringLong>(id_sl);
    auto* t_ss = engine.get_state_table<int64_t, InnerMap>(id_ss);

    ASSERT_NE(t_ll, nullptr);
    ASSERT_NE(t_ls, nullptr);
    ASSERT_NE(t_sl, nullptr);
    ASSERT_NE(t_ss, nullptr);

    // Each operates independently
    InnerMapLongLong ll_data{{1, 100}};
    InnerMapLongString ls_data{{1, "hello"}};
    InnerMapStringLong sl_data{{"k", 200}};
    InnerMap ss_data{{"uk", "uv"}};

    t_ll->put(0, 1, std::move(ll_data));
    t_ls->put(0, 1, std::move(ls_data));
    t_sl->put(0, 1, std::move(sl_data));
    t_ss->put(0, 1, std::move(ss_data));

    ASSERT_EQ((*t_ll->get(0, 1))[1], 100);
    ASSERT_EQ((*t_ls->get(0, 1))[1], "hello");
    ASSERT_EQ((*t_sl->get(0, 1))["k"], 200);
    ASSERT_EQ((*t_ss->get(0, 1))["uk"], "uv");
}

// ============================================================================
//  COW Snapshot with TypedInnerMap
// ============================================================================

TEST(TypedInnerMapTest, COWSnapshotLongLong) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map{{1, 100}, {2, 200}};
    st.put(0, 10, std::move(map));

    st.prepare_snapshot();

    // Modify after snapshot
    InnerMapLongLong new_map{{1, 999}};
    st.put(0, 10, std::move(new_map));

    // Snapshot should see old map
    std::map<int64_t, InnerMapLongLong> snapshot_state;
    st.for_each_snapshot_in_key_group(0, [&snapshot_state](const int64_t& k, const InnerMapLongLong& v) {
        snapshot_state[k] = v;
    });

    ASSERT_EQ(snapshot_state[10].size(), 2u);
    ASSERT_EQ(snapshot_state[10][1], 100);
    ASSERT_EQ(snapshot_state[10][2], 200);

    st.release_snapshot();
}

// ============================================================================
//  Large InnerMap
// ============================================================================

TEST(TypedInnerMapTest, LargeInnerMapLongLong) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong map;
    for (int i = 0; i < 1000; ++i) {
        map[i] = i * 10;
    }
    st.put(0, 1, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_EQ(m->size(), 1000u);
    ASSERT_EQ((*m)[0], 0);
    ASSERT_EQ((*m)[500], 5000);
    ASSERT_EQ((*m)[999], 9990);
}

TEST(TypedInnerMapTest, LargeInnerMapStringString) {
    StateTable<int64_t, InnerMap> st(0, 1, true);

    InnerMap map;
    for (int i = 0; i < 1000; ++i) {
        map["key_" + std::to_string(i)] = "val_" + std::to_string(i);
    }
    st.put(0, 1, std::move(map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_EQ(m->size(), 1000u);
    ASSERT_EQ((*m)["key_0"], "val_0");
    ASSERT_EQ((*m)["key_999"], "val_999");
}

// ============================================================================
//  Empty InnerMap
// ============================================================================

TEST(TypedInnerMapTest, EmptyInnerMap) {
    StateTable<int64_t, InnerMapLongLong> st(0, 1, true);

    InnerMapLongLong empty_map;
    st.put(0, 1, std::move(empty_map));

    auto* m = st.get(0, static_cast<int64_t>(1));
    ASSERT_NE(m, nullptr);
    ASSERT_TRUE(m->empty());
}

TEST(TypedInnerMapTest, DenseRangePreservesGeneralMapSemantics) {
    InnerMapLongLong m;
    ASSERT_FALSE(m.is_dense());
    ASSERT_EQ(m.add_sequence(62, 8, 64, 1), 8);
    ASSERT_TRUE(m.is_dense());
    ASSERT_EQ(m.size(), 8u);
    ASSERT_EQ(m.sum_sequence(-2, 8, 64), 8);
    m.erase(0);
    ASSERT_EQ(m.count(0), 0u);
    ASSERT_EQ(m[0], 0);
    m[0] = std::numeric_limits<int64_t>::min();
    ASSERT_EQ(m.find(0)->second, std::numeric_limits<int64_t>::min());
    auto copy = m;
    m[-9] = 100;
    ASSERT_FALSE(m.is_dense());
    ASSERT_EQ(m[-9], 100);
    ASSERT_EQ(m[62], 1);
    ASSERT_EQ(copy.count(-9), 0u);
    ASSERT_TRUE(copy.is_dense());
    size_t seen=0; for(const auto& kv:copy) { (void)kv; ++seen; }
    ASSERT_EQ(seen, copy.size());
    copy.clear(); ASSERT_TRUE(copy.empty());
    ASSERT_EQ(copy.add_sequence(0, 4, 64, 1), 4);
}
TEST(TypedInnerMapTest, DenseRangeMatchesHashOracleWithWrapAndOverflow) {
    InnerMapLongLong m;
    std::unordered_map<int64_t,int64_t> expected;
    for(int round=0;round<200;++round) {
        int64_t start=round-100;
        int64_t delta=round%3-1;
        int64_t sum=0;
        for(int i=0;i<97;++i) {
            int64_t k=(start+i)%64; if(k<0)k+=64;
            expected[k]+=delta;sum+=expected[k];
        }
        ASSERT_EQ(m.add_sequence(start,97,64,delta),sum);
        for(const auto& kv:expected)ASSERT_EQ(m.find(kv.first)->second,kv.second);
    }
    m[0]=std::numeric_limits<int64_t>::max();
    ASSERT_EQ(m.add_sequence(0,1,64,1),std::numeric_limits<int64_t>::min());
    m[1000000000L]=9;
    ASSERT_FALSE(m.is_dense());
    ASSERT_FALSE(m.enable_dense(64));
    ASSERT_EQ(m[1000000000L],9);
}
TEST(TypedInnerMapTest, DenseRangeCopyOnWriteKeepsOldVector) {
    StateTable<int64_t,InnerMapLongLong> st(0,1,true);
    InnerMapLongLong m; m.add_sequence(0,64,64,1);st.put(0,1,std::move(m));
    st.prepare_snapshot();
    ASSERT_TRUE(st.modify_in_place(0,1,[](InnerMapLongLong& v){v.add_sequence(0,64,64,1);}));
    int seen=0;
    st.for_each_snapshot_in_key_group(0,[&](const int64_t&,const InnerMapLongLong& v){
        ASSERT_EQ(v.sum_sequence(0,64,64),64);++seen;
    });
    ASSERT_EQ(seen,1);ASSERT_EQ(st.get(0,1)->sum_sequence(0,64,64),128);
    st.release_snapshot();
}
