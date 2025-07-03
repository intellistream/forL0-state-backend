package org.apache.flink.runtime.state.heap.hotspot.sketch.WavingSketch;

public class Murmur {
    public static int hash(int key, int seed) {
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ 4;
        int k = key;
        k *= m; k ^= k >>> r; k *= m;
        h *= m; h ^= k; h ^= h >>> 13; h *= m; h ^= h >>> 15;
        return h;
    }
}

