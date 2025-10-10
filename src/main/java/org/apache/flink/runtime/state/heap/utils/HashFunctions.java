package org.apache.flink.runtime.state.heap.utils;

/**
 * Hash functions optimized for ForL0 State Backend.
 * Provides consistent, high-quality hash functions for different purposes.
 */
public final class HashFunctions {

    private HashFunctions() {
        // Utility class
    }

    /**
     * MurmurHash3-inspired hash function for general key hashing.
     * Provides good distribution and avalanche properties.
     *
     * @param data Input data bytes
     * @return 32-bit hash value
     */
    public static int murmurHash3(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }
        return murmurHash3(data, 0, data.length, 0x9747b28c);
    }

    /**
     * MurmurHash3 for a byte[] slice [offset, offset+length).
     */
    public static int murmurHash3(byte[] data, int offset, int len, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // 4字节对齐

        // 一次处理4字节
        for (int i = offset; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // 处理尾部
        int k1 = 0;
        int tail = len & 0x03;
        if (tail == 3) {
            k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
        }
        if (tail >= 2) {
            k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
        }
        if (tail >= 1) {
            k1 ^= (data[roundedEnd] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }

        // final mix
        h1 ^= len;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }

    // ================= Jenkins Hash & tag helpers =================

    /**
     * Combined Jenkins hash for key + namespace (顺序叠加避免额外拷贝)。
     * 优化版本：减少重复终末处理，提升性能。
     */
    public static int compositeHash(byte[] keyBuf, int keyLen, byte[] nsBuf, int nsLen) {
        int hash = 0;
        hash = murmurHash3(keyBuf, 0, keyLen, hash);
        return murmurHash3(nsBuf, 0, nsLen, hash);
    }

    public static int compositeHash(byte[] key, byte[] namespace) {
        return compositeHash(key, key.length, namespace, namespace.length);
    }

    /**
     * Jenkins hash function - kept for potential future use.
     * Currently unused but may be needed for alternative hashing strategies.
     */
    @SuppressWarnings("unused")
    private static int jenkinsHash(byte[] buf, int len, int hash) {
        if (buf != null && len > 0) {
            for (int i = 0; i < len; i++) {
                hash += (buf[i] & 0xFF);
                hash += (hash << 10);
                hash ^= (hash >>> 6);
            }
        }
        return hash;
    }


    // Methods for levelhash compatibility (legacy support)

    /**
     * Mix64 function for 32-bit input (legacy support).
     *
     * @param value 32-bit value to mix
     * @return Mixed hash value
     */
    public static long mix64(int value) {
        return mix64((long) value);
    }

    /**
     * Mix64 function for 64-bit input (legacy support).
     *
     * @param value 64-bit value to mix
     * @return Mixed hash value
     */
    public static long mix64(long value) {
        long x = value;
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

    /**
     * Extract tag from 64-bit hash (legacy support).
     *
     * @param hash 64-bit hash value
     * @return 8-bit tag
     */
    public static byte tag(long hash) {
        // Use high 8 bits for tag
        return (byte) (hash >>> 56);
    }

    /**
     * Rotate left operation - kept for potential future use.
     * Currently unused but may be needed for alternative hashing strategies.
     */
    @SuppressWarnings("unused")
    private static int rotateLeft(int value, int shift) {
        return (value << shift) | (value >>> (32 - shift));
    }
}
