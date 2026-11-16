// Linked only into the opt-in forl0_engine_test target.
#include "hot_cache.h"
#include <atomic>
#include <jni.h>

static std::atomic<uint64_t> attached{0}, lookups{0}, hits{0}, writes{0};
void test_cache_record(forl0::HotCacheLL* cache) {
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
