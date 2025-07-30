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

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        final int r1 = 15;
        final int r2 = 13;
        final int m = 5;
        final int n = 0xe6546b64;

        int hash = 0x9747b28c; // Seed

        int len = data.length;
        int roundedEnd = len & 0xfffffffc; // Round down to 4 byte block

        for (int i = 0; i < roundedEnd; i += 4) {
            int k = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) |
                    ((data[i + 2] & 0xff) << 16) | ((data[i + 3] & 0xff) << 24);

            k *= c1;
            k = rotateLeft(k, r1);
            k *= c2;

            hash ^= k;
            hash = rotateLeft(hash, r2);
            hash = hash * m + n;
        }

        // Handle remaining bytes
        int k = 0;
        switch (len & 3) {
            case 3:
                k ^= (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k ^= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k ^= (data[roundedEnd] & 0xff);
                k *= c1;
                k = rotateLeft(k, r1);
                k *= c2;
                hash ^= k;
        }

        // Finalization
        hash ^= len;
        hash ^= (hash >>> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >>> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >>> 16);

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
        long x = value;
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
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

    private static int rotateLeft(int value, int shift) {
        return (value << shift) | (value >>> (32 - shift));
    }
}
