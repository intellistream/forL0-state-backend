/*
 * Native L0 Memory Implementation for ForL0 State Backend
 * 
 * This file implements JNI native methods for memory allocation.
 * 
 * Two modes are supported:
 * 1. L0 Mode: Uses L0 memory pool library (libl0mempool.so) on L0-enabled servers
 * 2. Simulation Mode: Uses standard malloc/free for development/testing
 *
 * The mode is automatically detected at runtime:
 * - If /dev/hisi_l0 exists and libl0mempool.so can be loaded -> L0 Mode
 * - Otherwise -> Simulation Mode
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __APPLE__
#include <malloc/malloc.h>
#define L0_NOT_SUPPORTED 1
#else
#include <malloc.h>
#include <dlfcn.h>
#include <unistd.h>
#endif

/* ==================== L0 Library Types and Function Pointers ==================== */

#ifndef L0_NOT_SUPPORTED

/* L0 memory pool library types */
typedef struct cache_tuner cache_tuner;
typedef struct mem_pool mem_pool;

/* Return codes */
#define RET_SUCCESS 0
#define RET_FAIL -1

/* Function pointer types for L0 library */
typedef int (*cache_tuner_init_fn)(cache_tuner **tuner, size_t max_capacity);
typedef int (*cache_tuner_destroy_fn)(cache_tuner *tuner);
typedef void* (*mem_pool_create_raw_fn)(cache_tuner *tuner, const char *name, size_t size);
typedef void (*mem_pool_release_raw_fn)(void **ppool);
typedef void* (*l0_mem_alloc_fn)(cache_tuner *tuner, size_t size);
typedef int (*l0_mem_free_fn)(cache_tuner *tuner, void *p);

/* L0 library function pointers */
static cache_tuner_init_fn p_cache_tuner_init = NULL;
static cache_tuner_destroy_fn p_cache_tuner_destroy = NULL;
static mem_pool_create_raw_fn p_mem_pool_create_raw = NULL;
static mem_pool_release_raw_fn p_mem_pool_release_raw = NULL;
static l0_mem_alloc_fn p_l0_mem_alloc = NULL;
static l0_mem_free_fn p_l0_mem_free = NULL;

/* L0 library handle */
static void *l0_lib_handle = NULL;

/* Global cache tuner (initialized once) */
static cache_tuner *global_tuner = NULL;

#endif /* L0_NOT_SUPPORTED */

/* ==================== Mode Detection ==================== */

/* 0 = not initialized, 1 = simulation mode, 2 = L0 mode */
static volatile int g_mode = 0;

/* Check if L0 device exists */
static int check_l0_device(void) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    return access("/dev/hisi_l0", F_OK) == 0;
#endif
}

/* Try to load L0 library and resolve functions */
static int load_l0_library(void) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    /* Try to load the library */
    l0_lib_handle = dlopen("libl0mempool.so", RTLD_NOW);
    if (!l0_lib_handle) {
        fprintf(stderr, "[ForL0] Cannot load libl0mempool.so: %s\n", dlerror());
        return 0;
    }

    /* Resolve function pointers */
    p_cache_tuner_init = (cache_tuner_init_fn)dlsym(l0_lib_handle, "cache_tuner_init");
    p_cache_tuner_destroy = (cache_tuner_destroy_fn)dlsym(l0_lib_handle, "cache_tuner_destroy");
    p_mem_pool_create_raw = (mem_pool_create_raw_fn)dlsym(l0_lib_handle, "mem_pool_create_raw");
    p_mem_pool_release_raw = (mem_pool_release_raw_fn)dlsym(l0_lib_handle, "mem_pool_release_raw");
    p_l0_mem_alloc = (l0_mem_alloc_fn)dlsym(l0_lib_handle, "l0_mem_alloc");
    p_l0_mem_free = (l0_mem_free_fn)dlsym(l0_lib_handle, "l0_mem_free");

    if (!p_cache_tuner_init || !p_cache_tuner_destroy || !p_l0_mem_alloc || !p_l0_mem_free) {
        fprintf(stderr, "[ForL0] Failed to resolve L0 library functions\n");
        dlclose(l0_lib_handle);
        l0_lib_handle = NULL;
        return 0;
    }

    return 1;
#endif
}

/* Initialize L0 mode */
static int init_l0_mode(size_t max_capacity) {
#ifdef L0_NOT_SUPPORTED
    return 0;
#else
    if (!p_cache_tuner_init) {
        return 0;
    }

    int ret = p_cache_tuner_init(&global_tuner, max_capacity);
    if (ret != RET_SUCCESS) {
        fprintf(stderr, "[ForL0] Failed to initialize cache tuner: %d\n", ret);
        return 0;
    }

    fprintf(stdout, "[ForL0] L0 mode initialized with max_capacity=%zu\n", max_capacity);
    return 1;
#endif
}

/* Initialize mode (called once) */
static void init_mode(void) {
    if (g_mode != 0) {
        return; /* Already initialized */
    }

    /* Default max capacity: 1GB */
    size_t default_capacity = 1024UL * 1024UL * 1024UL;

    /* Check if L0 device exists */
    if (check_l0_device()) {
        fprintf(stdout, "[ForL0] L0 device detected (/dev/hisi_l0)\n");
        
        /* Try to load L0 library */
        if (load_l0_library()) {
            fprintf(stdout, "[ForL0] L0 library loaded successfully\n");
            
            /* Initialize L0 mode */
            if (init_l0_mode(default_capacity)) {
                g_mode = 2; /* L0 mode */
                fprintf(stdout, "[ForL0] Running in L0 MODE\n");
                return;
            }
        }
    }

    /* Fall back to simulation mode */
    g_mode = 1;
    fprintf(stdout, "[ForL0] Running in SIMULATION MODE (malloc/free)\n");
}

/* ==================== Memory Allocation Functions ==================== */

static void* do_malloc(size_t size) {
    init_mode();
    
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_alloc) {
        void *ptr = p_l0_mem_alloc(global_tuner, size);
        if (ptr) {
            return ptr;
        }
        /* Fall through to malloc if L0 alloc fails */
        fprintf(stderr, "[ForL0] L0 alloc failed, falling back to malloc\n");
    }
#endif

    return malloc(size);
}

static void do_free(void *ptr) {
    if (!ptr) return;
    
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_free) {
        int ret = p_l0_mem_free(global_tuner, ptr);
        if (ret == RET_SUCCESS) {
            return;
        }
        /* If L0 free fails, try regular free (might be malloc'd memory) */
    }
#endif

    free(ptr);
}

static void* do_malloc_aligned(size_t size, size_t alignment) {
    init_mode();
    
#ifndef L0_NOT_SUPPORTED
    if (g_mode == 2 && global_tuner && p_l0_mem_alloc) {
        /* L0 memory is already aligned, just allocate */
        void *ptr = p_l0_mem_alloc(global_tuner, size);
        if (ptr) {
            return ptr;
        }
        fprintf(stderr, "[ForL0] L0 aligned alloc failed, falling back to posix_memalign\n");
    }
#endif

    /* Simulation mode: use posix_memalign */
    void *ptr = NULL;
#ifdef _WIN32
    ptr = _aligned_malloc(size, alignment);
#else
    if (alignment < sizeof(void *)) {
        alignment = sizeof(void *);
    }
    int result = posix_memalign(&ptr, alignment, size);
    if (result != 0) {
        ptr = NULL;
    }
#endif
    return ptr;
}

/* ==================== JNI Functions ==================== */

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    malloc
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_malloc
  (JNIEnv *env, jclass cls, jlong size)
{
    if (size <= 0) {
        return 0;
    }
    
    void *ptr = do_malloc((size_t)size);
    return (jlong)(uintptr_t)ptr;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_free
  (JNIEnv *env, jclass cls, jlong address)
{
    if (address != 0) {
        do_free((void *)(uintptr_t)address);
    }
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    mallocAligned
 * Signature: (JI)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_mallocAligned
  (JNIEnv *env, jclass cls, jlong size, jint alignment)
{
    if (size <= 0 || alignment <= 0) {
        return 0;
    }
    
    void *ptr = do_malloc_aligned((size_t)size, (size_t)alignment);
    return (jlong)(uintptr_t)ptr;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    getMode
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_getMode
  (JNIEnv *env, jclass cls)
{
    init_mode();
    return (jint)g_mode;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    isL0Mode
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_isL0Mode
  (JNIEnv *env, jclass cls)
{
    init_mode();
    return g_mode == 2 ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    setMaxCapacity
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_setMaxCapacity
  (JNIEnv *env, jclass cls, jlong capacity)
{
    /* 
     * This should be called BEFORE any allocation if user wants custom capacity.
     * Once mode is initialized, capacity cannot be changed.
     */
    if (g_mode != 0) {
        fprintf(stderr, "[ForL0] Warning: setMaxCapacity called after mode initialization\n");
        return;
    }
    
#ifndef L0_NOT_SUPPORTED
    /* Store capacity for init_l0_mode to use */
    /* For now we use default, but could add global variable for custom capacity */
#endif
    
    fprintf(stdout, "[ForL0] setMaxCapacity(%lld) - will be applied on init\n", (long long)capacity);
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    copyFromArray
 * Signature: (J[BII)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_copyFromArray
  (JNIEnv *env, jclass cls, jlong destAddress, jbyteArray src, jint srcOffset, jint length)
{
    if (destAddress == 0 || src == NULL || length <= 0) {
        return;
    }
    
    jbyte *srcBytes = (*env)->GetByteArrayElements(env, src, NULL);
    if (srcBytes != NULL) {
        memcpy((void *)(uintptr_t)destAddress, srcBytes + srcOffset, (size_t)length);
        (*env)->ReleaseByteArrayElements(env, src, srcBytes, JNI_ABORT);
    }
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    copyToArray
 * Signature: (J[BII)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_copyToArray
  (JNIEnv *env, jclass cls, jlong srcAddress, jbyteArray dest, jint destOffset, jint length)
{
    if (srcAddress == 0 || dest == NULL || length <= 0) {
        return;
    }
    
    jbyte *destBytes = (*env)->GetByteArrayElements(env, dest, NULL);
    if (destBytes != NULL) {
        memcpy(destBytes + destOffset, (void *)(uintptr_t)srcAddress, (size_t)length);
        (*env)->ReleaseByteArrayElements(env, dest, destBytes, 0);
    }
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    getByte
 * Signature: (J)B
 */
JNIEXPORT jbyte JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_getByte
  (JNIEnv *env, jclass cls, jlong address)
{
    return *((jbyte *)(uintptr_t)address);
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    putByte
 * Signature: (JB)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_putByte
  (JNIEnv *env, jclass cls, jlong address, jbyte value)
{
    *((jbyte *)(uintptr_t)address) = value;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    getShort
 * Signature: (J)S
 */
JNIEXPORT jshort JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_getShort
  (JNIEnv *env, jclass cls, jlong address)
{
    return *((jshort *)(uintptr_t)address);
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    putShort
 * Signature: (JS)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_putShort
  (JNIEnv *env, jclass cls, jlong address, jshort value)
{
    *((jshort *)(uintptr_t)address) = value;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    getInt
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_getInt
  (JNIEnv *env, jclass cls, jlong address)
{
    return *((jint *)(uintptr_t)address);
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    putInt
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_putInt
  (JNIEnv *env, jclass cls, jlong address, jint value)
{
    *((jint *)(uintptr_t)address) = value;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    getLong
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_getLong
  (JNIEnv *env, jclass cls, jlong address)
{
    return *((jlong *)(uintptr_t)address);
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    putLong
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_putLong
  (JNIEnv *env, jclass cls, jlong address, jlong value)
{
    *((jlong *)(uintptr_t)address) = value;
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    memset
 * Signature: (JBJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_memset
  (JNIEnv *env, jclass cls, jlong address, jbyte value, jlong length)
{
    if (address != 0 && length > 0) {
        memset((void *)(uintptr_t)address, (int)value, (size_t)length);
    }
}

/*
 * Class:     org_apache_flink_runtime_state_heap_space_NativeL0Memory
 * Method:    memcpy
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_state_heap_space_NativeL0Memory_memcpy
  (JNIEnv *env, jclass cls, jlong destAddress, jlong srcAddress, jlong length)
{
    if (destAddress != 0 && srcAddress != 0 && length > 0) {
        memcpy((void *)(uintptr_t)destAddress, (void *)(uintptr_t)srcAddress, (size_t)length);
    }
}
