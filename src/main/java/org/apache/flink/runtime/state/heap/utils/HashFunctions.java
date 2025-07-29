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
        switch (len & 0x03) {
            case 3:
                k ^= (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k ^= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k ^= data[roundedEnd] & 0xff;
                k *= c1;
                k = rotateLeft(k, r1);
                k *= c2;
                hash ^= k;
        }

        // Finalization
        hash ^= len;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;

        return hash;
    }

    /**
     * Combines key and namespace into a single hash for main table lookup.
     *
     * @param keyBytes Key bytes
     * @param namespaceBytes Namespace bytes
     * @return Combined hash value
     */
    public static int combineKeyNamespaceHash(byte[] keyBytes, byte[] namespaceBytes) {
        // Combine key and namespace with different seeds
        int keyHash = murmurHash3(keyBytes);
        int namespaceHash = murmurHash3WithSeed(namespaceBytes, 0x85ebca77);

        // Mix the two hashes
        return mixHashes(keyHash, namespaceHash);
    }

    /**
     * Generates a 16-bit tag from hash value for fast comparison.
     * Uses the upper 16 bits for better distribution.
     *
     * @param hash 32-bit hash value
     * @return 16-bit tag
     */
    public static short extractTag(int hash) {
        // Use upper 16 bits and apply additional mixing to avoid clustering
        int tag = hash >>> 16;
        tag ^= hash & 0xFFFF; // XOR with lower 16 bits
        return (short) (tag & 0xFFFF);
    }

    /**
     * Computes bucket index from hash for a table with given capacity.
     *
     * @param hash Hash value
     * @param bucketCount Number of buckets (must be power of 2)
     * @return Bucket index
     */
    public static int getBucketIndex(int hash, int bucketCount) {
        // Ensure bucket count is power of 2
        assert (bucketCount & (bucketCount - 1)) == 0 : "Bucket count must be power of 2";

        // Use bit masking for power-of-2 bucket counts
        return hash & (bucketCount - 1);
    }

    /**
     * Alternative hash function using xxHash-inspired algorithm.
     * Used for rehashing during table expansion.
     *
     * @param data Input data bytes
     * @return 32-bit hash value
     */
    public static int xxHash32(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }

        final int prime1 = 0x9E3779B1;
        final int prime2 = 0x85EBCA77;
        final int prime3 = 0xC2B2AE3D;
        final int prime4 = 0x27D4EB2F;
        final int prime5 = 0x165667B1;

        int seed = 0;
        int hash;
        int len = data.length;
        int offset = 0;

        if (len >= 16) {
            int v1 = seed + prime1 + prime2;
            int v2 = seed + prime2;
            int v3 = seed;
            int v4 = seed - prime1;

            while (offset <= len - 16) {
                v1 += getInt(data, offset) * prime2;
                v1 = rotateLeft(v1, 13) * prime1;

                v2 += getInt(data, offset + 4) * prime2;
                v2 = rotateLeft(v2, 13) * prime1;

                v3 += getInt(data, offset + 8) * prime2;
                v3 = rotateLeft(v3, 13) * prime1;

                v4 += getInt(data, offset + 12) * prime2;
                v4 = rotateLeft(v4, 13) * prime1;

                offset += 16;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
        } else {
            hash = seed + prime5;
        }

        hash += len;

        // Process remaining bytes
        while (offset <= len - 4) {
            hash += getInt(data, offset) * prime3;
            hash = rotateLeft(hash, 17) * prime4;
            offset += 4;
        }

        while (offset < len) {
            hash += (data[offset] & 0xFF) * prime5;
            hash = rotateLeft(hash, 11) * prime1;
            offset++;
        }

        // Final avalanche
        hash ^= hash >>> 15;
        hash *= prime2;
        hash ^= hash >>> 13;
        hash *= prime3;
        hash ^= hash >>> 16;

        return hash;
    }

    /**
     * Simple hash function for testing purposes.
     *
     * @param value Input integer value
     * @return Hash value
     */
    public static int simpleIntHash(int value) {
        // Wang's integer hash function
        value ^= value >>> 16;
        value *= 0x85ebca6b;
        value ^= value >>> 13;
        value *= 0xc2b2ae35;
        value ^= value >>> 16;
        return value;
    }

    /**
     * MurmurHash3 with custom seed.
     *
     * @param data Input data
     * @param seed Hash seed
     * @return Hash value
     */
    public static int murmurHash3WithSeed(byte[] data, int seed) {
        if (data == null || data.length == 0) {
            return seed;
        }

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        final int r1 = 15;
        final int r2 = 13;
        final int m = 5;
        final int n = 0xe6546b64;

        int hash = seed;

        int len = data.length;
        int roundedEnd = len & 0xfffffffc;

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
        switch (len & 0x03) {
            case 3:
                k ^= (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k ^= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k ^= data[roundedEnd] & 0xff;
                k *= c1;
                k = rotateLeft(k, r1);
                k *= c2;
                hash ^= k;
        }

        // Finalization
        hash ^= len;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;

        return hash;
    }

    /**
     * Mixes two hash values to create a combined hash.
     *
     * @param hash1 First hash
     * @param hash2 Second hash
     * @return Mixed hash
     */
    private static int mixHashes(int hash1, int hash2) {
        // Thomas Wang's 32-bit mix function
        hash1 ^= hash2;
        hash1 ^= hash1 >>> 16;
        hash1 *= 0x85ebca6b;
        hash1 ^= hash1 >>> 13;
        hash1 *= 0xc2b2ae35;
        hash1 ^= hash1 >>> 16;
        return hash1;
    }

    /**
     * Left rotation for 32-bit integers.
     */
    private static int rotateLeft(int value, int distance) {
        return (value << distance) | (value >>> (32 - distance));
    }

    /**
     * Reads a 32-bit integer from byte array at given offset (little-endian).
     */
    private static int getInt(byte[] data, int offset) {
        return (data[offset] & 0xFF) |
               ((data[offset + 1] & 0xFF) << 8) |
               ((data[offset + 2] & 0xFF) << 16) |
               ((data[offset + 3] & 0xFF) << 24);
    }

    /**
     * 64-bit mixing function for levelhash compatibility.
     *
     * @param value Input value
     * @return Mixed 64-bit value
     */
    public static long mix64(long value) {
        // Wang's 64-bit integer hash
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return value;
    }

    /**
     * 64-bit mixing function for integer input.
     *
     * @param value Input integer value
     * @return Mixed 64-bit value
     */
    public static long mix64(int value) {
        return mix64((long) value);
    }

    /**
     * Extracts tag from 64-bit hash for levelhash compatibility.
     *
     * @param hash 64-bit hash value
     * @return 16-bit tag
     */
    public static short tag(long hash) {
        return (short) ((hash >>> 48) & 0xFFFF);
    }
}
