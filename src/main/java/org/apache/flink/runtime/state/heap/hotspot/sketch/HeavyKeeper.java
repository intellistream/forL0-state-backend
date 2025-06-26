package org.apache.flink.runtime.state.heap.hotspot.sketch;

import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class HeavyKeeper implements Sketch {
    private final int D;
    private final double B;
    private final int M2;
    private final int K;
    private final int FP_LEN;

    private final int[] bucketSize;
    private final int[][] C;
    private final int[][] FP;
    private final Hash hash;
    private final Random rand;

    // New: count map for Top-K
    private final Map<String, Integer> counts;

    public HeavyKeeper() {
        this.D      = Config.INSTANCE.hkDepth;
        this.B      = Config.INSTANCE.hkDecay;
        this.K      = Config.INSTANCE.topK;
        this.FP_LEN = Config.INSTANCE.keyLength4;

        long bytes      = (long) Config.INSTANCE.hkMemoryKb * 1024;
        long totalSlots = bytes / 8;
        this.M2 = (int)(totalSlots / D + D - 2);

        this.hash = new Hash(0);
        this.rand = new Random(1005);

        this.bucketSize = new int[D];
        this.C          = new int[D][];
        this.FP         = new int[D][];
        for (int d = 0; d < D; d++) {
            int mod = M2 - 2 * D + 2 * d + 3;
            bucketSize[d] = mod;
            C[d]  = new int[mod];
            FP[d] = new int[mod];
        }

        this.counts = new HashMap<>();
        clear();
    }

    @Override
    public void clear() {
        for (int d = 0; d < D; d++) {
            Arrays.fill(C[d], 0);
            Arrays.fill(FP[d], 0);
        }
        counts.clear();
    }

    @Override
    public void insert(byte[] key, int f) {
        int h32   = hash.run(key, FP_LEN);
        long uh   = Integer.toUnsignedLong(h32);
        int fpVal = (int)(uh >>> 16);

        for (int d = 0; d < D; d++) {
            long raw = uh + d;
            int mod = bucketSize[d];
            long r = raw % mod;
            if (r < 0) r += mod;
            int idx = (int) r;

            int c = C[d][idx];
            if (FP[d][idx] == fpVal) {
                C[d][idx] = c + f;
            } else if (rand.nextDouble() < 1.0 / Math.pow(B, c)) {
                C[d][idx] = c - 1;
                if (C[d][idx] < 0) {
                    FP[d][idx] = fpVal;
                    C[d][idx]  = f;
                }
            }
        }

        String x = new String(key, 0, FP_LEN, StandardCharsets.ISO_8859_1);
        counts.merge(x, f, Integer::sum);
    }

    @Override
    public int query(byte[] key) {
        int h32   = hash.run(key, FP_LEN);
        long uh   = Integer.toUnsignedLong(h32);
        int fpVal = (int)(uh >>> 16);
        int res   = Integer.MAX_VALUE;

        for (int d = 0; d < D; d++) {
            long raw = uh + d;
            int mod = bucketSize[d];
            long r = raw % mod;
            if (r < 0) r += mod;
            int idx = (int) r;

            if (FP[d][idx] == fpVal) {
                res = Math.min(res, C[d][idx]);
            } else {
                return 0;
            }
        }
        return res;
    }

    @Override
    public void getHeavyHitters(int K, List<Map.Entry<String,Integer>> results) {
        PriorityQueue<Map.Entry<String,Integer>> pq = new PriorityQueue<>(
                Comparator.comparingInt(Map.Entry::getValue)
        );
        for (Map.Entry<String,Integer> e : counts.entrySet()) {
            pq.offer(e);
            if (pq.size() > K) pq.poll();
        }
        List<Map.Entry<String,Integer>> list = new ArrayList<>();
        while (!pq.isEmpty()) list.add(pq.poll());
        Collections.reverse(list);
        results.clear();
        results.addAll(list);
    }
}
