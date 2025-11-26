/*
 * Native L0 Memory Implementation for ForL0 State Backend
 * 
 * This file implements JNI native methods for memory allocation
 * using standard C malloc/free functions.
 *
 * Future versions may replace with:
 * - CXL memory allocation via libcxl
 * - PMEM allocation via libpmem
 * - Custom memory pool allocators
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

/* JNI class: org.apache.flink.runtime.state.heap.space.NativeL0Memory */

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
    
    void *ptr = malloc((size_t)size);
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
        free((void *)(uintptr_t)address);
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
    
    void *ptr = NULL;
    
#ifdef _WIN32
    ptr = _aligned_malloc((size_t)size, (size_t)alignment);
#else
    /* posix_memalign requires alignment to be a power of 2 and multiple of sizeof(void*) */
    size_t align = (size_t)alignment;
    if (align < sizeof(void *)) {
        align = sizeof(void *);
    }
    
    int result = posix_memalign(&ptr, align, (size_t)size);
    if (result != 0) {
        ptr = NULL;
    }
#endif
    
    return (jlong)(uintptr_t)ptr;
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
