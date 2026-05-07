// hot_cache_manager_test.cpp — exercise HotCacheManager with an injected
// L0BindingsLoader. We do NOT depend on the real /dev/hisi_l0; the loader
// here returns a heap-backed fake so the manager logic is testable on any host.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif

#include <cstdlib>
#include <cstring>
#include <string>

#include "hot_cache.h"

using namespace forl0;

// ----------------------------------------------------------------------
//  Fake L0 backend (heap-backed). NEVER used in production code paths.
// ----------------------------------------------------------------------
namespace fake_backend {

struct FakeTuner {
    size_t cap;
};

static int s_init_calls    = 0;
static int s_destroy_calls = 0;
static int s_alloc_calls   = 0;
static int s_free_calls    = 0;
static bool s_force_init_fail = false;
static bool s_force_alloc_fail = false;

static void reset() {
    s_init_calls = s_destroy_calls = s_alloc_calls = s_free_calls = 0;
    s_force_init_fail = false;
    s_force_alloc_fail = false;
}

static int  fake_init(void** tuner_out, size_t cap) {
    ++s_init_calls;
    if (s_force_init_fail) return -1;
    auto* t = new FakeTuner{cap};
    *tuner_out = t;
    return 0;
}
static int  fake_destroy(void* tuner) {
    ++s_destroy_calls;
    delete static_cast<FakeTuner*>(tuner);
    return 0;
}
static void* fake_alloc(void* /*tuner*/, size_t bytes) {
    ++s_alloc_calls;
    if (s_force_alloc_fail) return nullptr;
    void* p = nullptr;
    if (posix_memalign(&p, 64, bytes) != 0) return nullptr;
    std::memset(p, 0, bytes);
    return p;
}
static int  fake_free(void* /*tuner*/, void* p) {
    ++s_free_calls;
    free(p);
    return 0;
}

static bool loader(L0LibBindings* out, std::string* /*reason*/) {
    out->lib_handle          = nullptr;
    out->cache_tuner_init    = reinterpret_cast<L0LibBindings::CacheTunerInitFn>(&fake_init);
    out->cache_tuner_destroy = reinterpret_cast<L0LibBindings::CacheTunerDestroyFn>(&fake_destroy);
    out->l0_mem_alloc        = reinterpret_cast<L0LibBindings::L0MemAllocFn>(&fake_alloc);
    out->l0_mem_free         = reinterpret_cast<L0LibBindings::L0MemFreeFn>(&fake_free);
    out->owns_lib_handle     = false;  // no dlclose
    return true;
}

static bool failing_loader(L0LibBindings* /*out*/, std::string* reason) {
    if (reason) *reason = "loader-failure-injected";
    return false;
}

}  // namespace fake_backend

// ----------------------------------------------------------------------
//  Tests
// ----------------------------------------------------------------------

TEST(HotCacheManager, LoaderFailureLeavesManagerInactive) {
    fake_backend::reset();
    HotCacheManager mgr(8 * 1024 * 1024, &fake_backend::failing_loader);
    ASSERT_FALSE(mgr.is_active());
    ASSERT_EQ(mgr.capacity_bytes(), 0u);
    ASSERT_EQ(mgr.failure_reason(), "loader-failure-injected");
    // acquire on inactive manager must return null.
    ASSERT_TRUE(mgr.acquire_ll(8) == nullptr);
}

TEST(HotCacheManager, TunerInitFailureLeavesManagerInactive) {
    fake_backend::reset();
    fake_backend::s_force_init_fail = true;
    HotCacheManager mgr(8 * 1024 * 1024, &fake_backend::loader);
    ASSERT_FALSE(mgr.is_active());
    ASSERT_EQ(fake_backend::s_init_calls, 1);
    // tuner was never created → no destroy.
    ASSERT_EQ(fake_backend::s_destroy_calls, 0);
}

TEST(HotCacheManager, AllocFailureLeavesManagerInactive) {
    fake_backend::reset();
    fake_backend::s_force_alloc_fail = true;
    HotCacheManager mgr(8 * 1024 * 1024, &fake_backend::loader);
    ASSERT_FALSE(mgr.is_active());
    ASSERT_GE(fake_backend::s_alloc_calls, 1);
    // tuner was created, then destroyed during failure cleanup.
    ASSERT_EQ(fake_backend::s_init_calls, 1);
    ASSERT_EQ(fake_backend::s_destroy_calls, 1);
}

TEST(HotCacheManager, AcquireAndReleaseAccountsForFreeSets) {
    fake_backend::reset();
    HotCacheManager mgr(64 * 192, &fake_backend::loader);  // 64 HotSets exactly
    ASSERT_TRUE(mgr.is_active());
    uint32_t total = mgr.total_sets();
    ASSERT_GE(total, 32u);  // we asked for 64; alignment may shave one
    uint32_t free_before = mgr.free_sets();
    ASSERT_EQ(free_before, total);

    auto a = mgr.acquire_ll(8);
    ASSERT_TRUE(a != nullptr);
    ASSERT_EQ(a->num_sets(), 8u);
    ASSERT_EQ(mgr.free_sets(), total - 8u);

    auto b = mgr.acquire_ll(16);
    ASSERT_TRUE(b != nullptr);
    ASSERT_EQ(b->num_sets(), 16u);
    ASSERT_EQ(mgr.free_sets(), total - 8u - 16u);

    HotCacheLL* a_raw = a.release();
    mgr.release_ll(a_raw);
    // After release, the run is back in the free pool. Adjacent or not
    // depends on layout; what we can assert is: total free went up by 8.
    ASSERT_EQ(mgr.free_sets(), total - 16u);

    HotCacheLL* b_raw = b.release();
    mgr.release_ll(b_raw);
    ASSERT_EQ(mgr.free_sets(), total);
}

TEST(HotCacheManager, NonPow2RequestRoundsDown) {
    fake_backend::reset();
    HotCacheManager mgr(64 * 192, &fake_backend::loader);
    ASSERT_TRUE(mgr.is_active());

    auto c = mgr.acquire_ll(13);  // → 8
    ASSERT_TRUE(c != nullptr);
    ASSERT_EQ(c->num_sets(), 8u);

    auto c2 = mgr.acquire_ll(31); // → 16
    ASSERT_TRUE(c2 != nullptr);
    ASSERT_EQ(c2->num_sets(), 16u);

    HotCacheLL* a = c.release();
    HotCacheLL* b = c2.release();
    mgr.release_ll(a);
    mgr.release_ll(b);
}

TEST(HotCacheManager, MultipleCachesAreIndependent) {
    fake_backend::reset();
    HotCacheManager mgr(64 * 192, &fake_backend::loader);
    ASSERT_TRUE(mgr.is_active());

    auto a = mgr.acquire_ll(8);
    auto b = mgr.acquire_ll(8);
    ASSERT_TRUE(a && b);
    ASSERT_NE(a->sets(), b->sets());
    // Distinct backing → writes to one must not appear in the other.
    a->put(7, 70);
    b->put(7, 700);
    int64_t va = 0, vb = 0;
    ASSERT_TRUE(a->get(7, &va));
    ASSERT_TRUE(b->get(7, &vb));
    ASSERT_EQ(va, 70);
    ASSERT_EQ(vb, 700);

    mgr.release_ll(a.release());
    mgr.release_ll(b.release());
}

TEST(HotCacheManager, PowerOfTwoFloor) {
    ASSERT_EQ(HotCacheManager::pow2_floor(0), 0u);
    ASSERT_EQ(HotCacheManager::pow2_floor(1), 1u);
    ASSERT_EQ(HotCacheManager::pow2_floor(2), 2u);
    ASSERT_EQ(HotCacheManager::pow2_floor(3), 2u);
    ASSERT_EQ(HotCacheManager::pow2_floor(7), 4u);
    ASSERT_EQ(HotCacheManager::pow2_floor(64), 64u);
    ASSERT_EQ(HotCacheManager::pow2_floor(65), 64u);
    ASSERT_EQ(HotCacheManager::pow2_floor(1023), 512u);
}
