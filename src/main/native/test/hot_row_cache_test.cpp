#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "hot_cache.h"
#include "long_inner_map.h"
#include <cstdlib>
using namespace forl0;

TEST(HotRowCache, ExactKeyGroupAndEpoch) {
    void* memory=nullptr;ASSERT_EQ(posix_memalign(&memory,64,4*sizeof(HotRowSet)),0);
    {HotRowCache cache(memory,4);int64_t first[64]{},second[64]{};first[0]=91;second[0]=82;
    cache.insert(7,1,11,first,64);cache.insert(7,2,11,second,64);
    int64_t* backing=nullptr;size_t span=0;
    auto* row=cache.get(7,1,11,&backing,&span);ASSERT_TRUE(row!=nullptr);ASSERT_EQ(row[0],91);ASSERT_EQ(backing,first);
    ASSERT_EQ(span,64u);row=cache.get(7,2,11,&backing,&span);ASSERT_TRUE(row!=nullptr);ASSERT_EQ(row[0],82);
    ASSERT_TRUE(cache.get(7,1,12,&backing,&span)==nullptr);
    ASSERT_TRUE(cache.get(8,1,11,&backing,&span)==nullptr);
    ASSERT_EQ(cache.lookups(),4u);ASSERT_EQ(cache.hits(),2u);}std::free(memory);
}

TEST(HotRowCache, EvictionAndPayloadCopy) {
    void* memory=nullptr;ASSERT_EQ(posix_memalign(&memory,64,sizeof(HotRowSet)),0);
    {HotRowCache cache(memory,1);int64_t payload[64]{};
    for(int key=0;key<5;key++){payload[0]=100+key;cache.insert(key,0,0,payload,64);}
    int64_t* backing=nullptr;size_t span=0;
    ASSERT_TRUE(cache.get(0,0,0,&backing,&span)==nullptr);
    auto* row=cache.get(4,0,0,&backing,&span);ASSERT_TRUE(row!=nullptr);ASSERT_EQ(row[0],104);
    payload[0]=0;ASSERT_EQ(row[0],104);ASSERT_EQ(cache.evictions(),1u);ASSERT_EQ(cache.fill_bytes(),5u*512u);}std::free(memory);
}

TEST(HotRowCache, ScalarAndRowsShareOnePoolAndRecycle) {
    const char* previous=std::getenv("FORL0_L0_MEMORY_MODE");std::string saved=previous?previous:"";bool had=previous;
    setenv("FORL0_L0_MEMORY_MODE","dram",1);
    {
        HotCacheManager manager(1048576);ASSERT_TRUE(manager.is_active());
        auto scalar=manager.acquire_ll(1024);auto rows=manager.acquire_rows(262144);
        ASSERT_TRUE(scalar!=nullptr);ASSERT_TRUE(rows!=nullptr);ASSERT_TRUE(manager.used_bytes()<=manager.capacity_bytes());
        ASSERT_EQ(manager.cache_count(),2u);uint32_t total=manager.total_sets();
        auto* s=scalar.release();auto* r=rows.release();manager.release_ll(s);manager.release_rows(r);
        ASSERT_EQ(manager.free_sets(),total);ASSERT_EQ(manager.used_bytes(),0u);
        auto next=manager.acquire_rows(524288);ASSERT_TRUE(next!=nullptr);manager.release_rows(next.release());
        ASSERT_EQ(manager.free_sets(),total);
    }
    if(had)setenv("FORL0_L0_MEMORY_MODE",saved.c_str(),1);else unsetenv("FORL0_L0_MEMORY_MODE");
}

TEST(HotRowCache, StateIdentityAndIndependentViewInvalidation) {
    void* memory=nullptr;ASSERT_EQ(posix_memalign(&memory,64,sizeof(HotRowSet)),0);
    {HotRowCache cache(memory,1);int64_t a[64]{},b[64]{};a[0]=101;b[0]=202;
    std::atomic<uint64_t> view_a{0},view_b{0};
    cache.insert(1,0,0,a,64,10,&view_a);cache.insert(1,0,0,b,64,20,&view_b);
    int64_t* backing=nullptr;size_t span=0;
    ASSERT_EQ(cache.get(1,0,0,&backing,&span,10)[0],101);
    ASSERT_EQ(cache.get(1,0,0,&backing,&span,20)[0],202);
    cache.insert(2,0,0,a,64,10,&view_a);cache.insert(3,0,0,a,64,10,&view_a);
    cache.insert(4,0,0,b,64,20,&view_b);
    ASSERT_EQ(view_a.load(),1u);ASSERT_EQ(view_b.load(),0u);
    // Evicting a pointer increments its view epoch, not the content version
    // of unrelated entries that remain resident in this shared pool.
    ASSERT_EQ(cache.get(2,0,0,&backing,&span,10)[0],101);
    }std::free(memory);
}

TEST(HotRowCache, AuthoritativeDenseRowCopyMoveAndPromotion) {
    LongInnerMap row;for(int64_t k=0;k<64;++k)row.put_value(k,1000000007+k);
    size_t span=0;int64_t cached[64];std::memcpy(cached,row.full_dense_data(span),512);
    row.attach_cache_data(cached);row.put_value(7,999);
    ASSERT_EQ(*row.find_value(7),999);ASSERT_EQ(row.find(7)->second,999);
    LongInnerMap copy(row);ASSERT_EQ(*copy.find_value(7),999);
    cached[7]=777;ASSERT_EQ(*copy.find_value(7),999);
    LongInnerMap moved(std::move(row));ASSERT_EQ(*moved.find_value(7),777);
    ASSERT_TRUE(moved.detach_cache_data(cached));cached[7]=0;ASSERT_EQ(*moved.find_value(7),777);
    std::memcpy(cached,moved.full_dense_data(span),512);moved.attach_cache_data(cached);
    cached[8]=555;moved.put_value(-1,123); // Promotion must read the authoritative cache.
    ASSERT_EQ(*moved.find_value(8),555);ASSERT_EQ(*moved.find_value(-1),123);
    ASSERT_FALSE(moved.detach_cache_data(cached));
}
