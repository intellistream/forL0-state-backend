package org.apache.flink.runtime.state.heap.utils;

public final class HashFunctions {
    private HashFunctions() {}

    /** Simple 64‑bit mix (splitmix64). */
    public static long mix64(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** 32‑bit tag extracted from the higher half of the first hash. */
    public static int tag(long h1) {
        return (int) (h1 >>> 32);
    }
}
