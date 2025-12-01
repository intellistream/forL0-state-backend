/*
 * 本地测试程序：验证 native 内存分配逻辑
 * 
 * 编译运行：
 *   Mac:   gcc -o test_native test_native.c -DSTANDALONE_TEST && ./test_native
 *   Linux: gcc -o test_native test_native.c -DSTANDALONE_TEST -ldl && ./test_native
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#ifdef __APPLE__
#define L0_NOT_SUPPORTED 1
#endif

/* ==================== 从 forl0_native.c 复制的核心逻辑 ==================== */

#ifndef L0_NOT_SUPPORTED
#include <dlfcn.h>
#include <unistd.h>

typedef struct cache_tuner cache_tuner;
typedef int (*cache_tuner_init_fn)(cache_tuner **tuner, size_t max_capacity);
typedef int (*cache_tuner_destroy_fn)(cache_tuner *tuner);
typedef void* (*l0_mem_alloc_fn)(cache_tuner *tuner, size_t size);
typedef int (*l0_mem_free_fn)(cache_tuner *tuner, void *p);

static cache_tuner_init_fn p_cache_tuner_init = NULL;
static cache_tuner_destroy_fn p_cache_tuner_destroy = NULL;
static l0_mem_alloc_fn p_l0_mem_alloc = NULL;
static l0_mem_free_fn p_l0_mem_free = NULL;
static void *l0_lib_handle = NULL;
static cache_tuner *global_tuner = NULL;
#define RET_SUCCESS 0
#endif

static volatile int g_mode = 0;

static int check_l0_device(void) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    return access("/dev/hisi_l0", F_OK) == 0;
#endif
}

static int load_l0_library(void) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    l0_lib_handle = dlopen("libl0mempool.so", RTLD_NOW);
    if (!l0_lib_handle) {
        printf("[TEST] Cannot load libl0mempool.so: %s\n", dlerror());
        return 0;
    }
    p_cache_tuner_init = (cache_tuner_init_fn)dlsym(l0_lib_handle, "cache_tuner_init");
    p_cache_tuner_destroy = (cache_tuner_destroy_fn)dlsym(l0_lib_handle, "cache_tuner_destroy");
    p_l0_mem_alloc = (l0_mem_alloc_fn)dlsym(l0_lib_handle, "l0_mem_alloc");
    p_l0_mem_free = (l0_mem_free_fn)dlsym(l0_lib_handle, "l0_mem_free");
    if (!p_cache_tuner_init || !p_cache_tuner_destroy || !p_l0_mem_alloc || !p_l0_mem_free) {
        printf("[TEST] Failed to resolve L0 library functions\n");
        dlclose(l0_lib_handle);
        l0_lib_handle = NULL;
        return 0;
    }
    return 1;
#endif
}

static int init_l0_mode(size_t max_capacity) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    if (!p_cache_tuner_init) return 0;
    int ret = p_cache_tuner_init(&global_tuner, max_capacity);
    if (ret != RET_SUCCESS) {
        printf("[TEST] Failed to initialize cache tuner: %d\n", ret);
        return 0;
    }
    return 1;
#endif
}

static void init_mode(void) {
    if (g_mode != 0) return;
    size_t default_capacity = 1024UL * 1024UL * 1024UL;
    
    if (check_l0_device()) {
        printf("[TEST] L0 device detected (/dev/hisi_l0)\n");
        if (load_l0_library()) {
            printf("[TEST] L0 library loaded successfully\n");
            if (init_l0_mode(default_capacity)) {
                g_mode = 2;
                printf("[TEST] Running in L0 MODE\n");
                return;
            }
        }
    }
    g_mode = 1;
    printf("[TEST] Running in SIMULATION MODE (malloc/free)\n");
}

static void* do_malloc(size_t size) {
    init_mode();
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_alloc) {
        void *ptr = p_l0_mem_alloc(global_tuner, size);
        if (ptr) return ptr;
        printf("[TEST] L0 alloc failed, falling back to malloc\n");
    }
#endif
    return malloc(size);
}

static void do_free(void *ptr) {
    if (!ptr) return;
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_free) {
        int ret = p_l0_mem_free(global_tuner, ptr);
        if (ret == RET_SUCCESS) return;
    }
#endif
    free(ptr);
}

static void* do_malloc_aligned(size_t size, size_t alignment) {
    init_mode();
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_alloc) {
        void *ptr = p_l0_mem_alloc(global_tuner, size);
        if (ptr) return ptr;
    }
#endif
    void *ptr = NULL;
    if (alignment < sizeof(void *)) alignment = sizeof(void *);
    int result = posix_memalign(&ptr, alignment, size);
    return (result == 0) ? ptr : NULL;
}

/* ==================== 测试用例 ==================== */

void test_mode_detection() {
    printf("\n=== Test: Mode Detection ===\n");
    init_mode();
    
    assert(g_mode == 1 || g_mode == 2);
    
#ifdef __APPLE__
    assert(g_mode == 1);  // Mac 只能是模拟模式
    printf("PASS: Mac correctly uses simulation mode\n");
#else
    printf("Mode: %s\n", g_mode == 2 ? "L0" : "Simulation");
    printf("PASS: Mode detection completed\n");
#endif
}

void test_malloc_free() {
    printf("\n=== Test: Malloc/Free ===\n");
    
    // 测试各种大小的分配
    size_t sizes[] = {64, 1024, 4096, 1024*1024};
    for (int i = 0; i < 4; i++) {
        void *ptr = do_malloc(sizes[i]);
        assert(ptr != NULL);
        
        // 写入数据验证内存可用
        memset(ptr, 0xAB, sizes[i]);
        assert(((unsigned char*)ptr)[0] == 0xAB);
        assert(((unsigned char*)ptr)[sizes[i]-1] == 0xAB);
        
        do_free(ptr);
        printf("PASS: malloc/free %zu bytes\n", sizes[i]);
    }
}

void test_aligned_allocation() {
    printf("\n=== Test: Aligned Allocation ===\n");
    
    size_t alignments[] = {8, 16, 32, 64, 128, 256};
    for (int i = 0; i < 6; i++) {
        void *ptr = do_malloc_aligned(4096, alignments[i]);
        assert(ptr != NULL);
        
        // 验证对齐
        uintptr_t addr = (uintptr_t)ptr;
        assert(addr % alignments[i] == 0);
        
        printf("PASS: %zu-byte aligned allocation at %p\n", alignments[i], ptr);
        do_free(ptr);
    }
}

void test_zero_and_invalid() {
    printf("\n=== Test: Edge Cases ===\n");
    
    // free(NULL) 应该安全
    do_free(NULL);
    printf("PASS: free(NULL) is safe\n");
    
    // 分配 0 字节
    void *ptr = do_malloc(0);
    // malloc(0) 的行为是实现定义的，可能返回 NULL 或有效指针
    printf("INFO: malloc(0) returned %p\n", ptr);
    if (ptr) do_free(ptr);
}

int main() {
    printf("========================================\n");
    printf("  ForL0 Native Memory Test\n");
    printf("========================================\n");
    
#ifdef __APPLE__
    printf("Platform: macOS (L0 not supported)\n");
#else
    printf("Platform: Linux\n");
#endif
    
    test_mode_detection();
    test_malloc_free();
    test_aligned_allocation();
    test_zero_and_invalid();
    
    printf("\n========================================\n");
    printf("  All tests passed!\n");
    printf("========================================\n");
    
    return 0;
}
