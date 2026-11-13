// Linked ONLY into the opt-in forl0_engine_test_l0 target, never production.
#include "hot_cache.h"
#include <cstdlib>
#include <atomic>
#include <jni.h>

static int init(void** tuner, size_t) { *tuner = std::malloc(1); return *tuner ? 0 : -1; }
static int destroy(void* tuner) { std::free(tuner); return 0; }
static void* alloc(void*, size_t bytes) {
    void* p = nullptr;
    return posix_memalign(&p, 64, bytes) == 0 ? p : nullptr;
}
static int release(void*, void* p) { std::free(p); return 0; }
bool test_l0_loader(forl0::L0LibBindings* out, std::string*) {
    out->cache_tuner_init = init;
    out->cache_tuner_destroy = destroy;
    out->l0_mem_alloc = alloc;
    out->l0_mem_free = release;
    out->owns_lib_handle = false;
    return true;
}

static std::atomic<uint64_t> attached{0}, lookups{0}, hits{0}, writes{0};
void test_l0_record(forl0::HotCacheLL* cache) {
    attached.fetch_add(1); lookups.fetch_add(cache->lookups());
    hits.fetch_add(cache->hits()); writes.fetch_add(cache->writes());
}
extern "C" JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NexmarkHotCacheSqlITCase_testCacheStats(JNIEnv* env, jclass, jboolean reset) {
    jlong stats[4] = {jlong(attached.load()), jlong(lookups.load()), jlong(hits.load()), jlong(writes.load())};
    if (reset) { attached = 0; lookups = 0; hits = 0; writes = 0; }
    auto out = env->NewLongArray(4);
    env->SetLongArrayRegion(out, 0, 4, stats);
    return out;
}
