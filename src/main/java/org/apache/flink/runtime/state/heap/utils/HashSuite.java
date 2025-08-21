// filepath: /Users/jinyunyang/IdeaProjects/forL0-state-backend/src/main/java/org/apache/flink/runtime/state/heap/utils/HashSuite.java
package org.apache.flink.runtime.state.heap.utils;

/**
 * HashSuite 统一提供 key/namespace 组合哈希与 tag 计算，
 * 便于在 ForL0StateMap/L0Table/MainTable 之间保持一致策略。
 */
public final class HashSuite {
    private HashSuite() {}

    public static int combinedHash(byte[] keyBuf, int keyLen, byte[] nsBuf, int nsLen) {
        int keyHash = HashFunctions.murmurHash3(keyBuf, 0, keyLen);
        int nsHash = HashFunctions.murmurHash3(nsBuf, 0, nsLen);
        return keyHash ^ nsHash;
    }

    public static short tagOf(int hash) {
        return (short) (hash & 0xFFFF);
    }
}

