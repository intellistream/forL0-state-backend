package org.apache.flink.runtime.state.heap.hotspot.sketch;

import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.nio.charset.StandardCharsets;

/**
 * HeavyKeeper implementation, using 4-byte fingerprint length from Config
 * and allowing direct memory budget (in KB) via hkMemoryKb.
 */
public class HeavyKeeper implements Sketch {
    private final int D;           // depth
    private final double B;        // decay
    private final int M2;          // computed slot count
    private final int K;           // top-K
    private final int FP_LEN;      // fingerprint length

    private final SSummary ss;
    private final int[] bucketSize;
    private final int[][] C;
    private final int[][] FP;
    private final Hash hash;
    private final Random rand;

    public HeavyKeeper() {
        // read params
        this.D      = Config.INSTANCE.hkDepth;
        this.B      = Config.INSTANCE.hkDecay;
        this.K      = Config.INSTANCE.topK;
        this.FP_LEN = Config.INSTANCE.keyLength4;

        // compute M2 from memory budget (in KB)
        long bytes      = (long) Config.INSTANCE.hkMemoryKb * 1024;
        long totalSlots = bytes / 8;                // each slot = 8 bytes (4B count + 4B fp)
        // invert totalSlots = D * (M2 - D + 2)
        this.M2 = (int) (totalSlots / D + D - 2);

        this.ss    = new SSummary(K);
        this.bucketSize = new int[D];
        this.C    = new int[D][];
        this.FP   = new int[D][];
        this.hash = new Hash(0);
        this.rand = new Random(1005);

        // initialize buckets per layer
        for (int d = 0; d < D; d++) {
            int mod = M2 - 2 * D + 2 * d + 3;
            bucketSize[d] = mod;
            C[d]  = new int[mod];
            FP[d] = new int[mod];
        }
        clear();
    }

    @Override
    public void clear() {
        ss.clear();
        for (int d = 0; d < D; d++) {
            Arrays.fill(C[d],  0);
            Arrays.fill(FP[d], 0);
        }
    }

    @Override
    public void insert(byte[] key, int f) {
        String x = new String(key, 0, FP_LEN, StandardCharsets.ISO_8859_1);
        boolean inSS = ss.find(x) != 0;

        int h32    = hash.run(key, key.length);
        long uh    = Integer.toUnsignedLong(h32);
        int fpVal  = (int)(uh >>> 16);
        int maxv   = 0;

        for (int d = 0; d < D; d++) {
            int mod = bucketSize[d];
            long r = uh % mod;
            if (r < 0) r += mod;
            int idx = (int) r;

            int c = C[d][idx];
            if (FP[d][idx] == fpVal) {
                if (inSS || c <= ss.getmin()) {
                    c += f;
                    C[d][idx] = c;
                }
                maxv = Math.max(maxv, c);
            } else {
                double p = 1.0 / Math.pow(B, c);
                if (rand.nextDouble() < p) {
                    c--;
                    C[d][idx] = c;
                    if (c <= 0) {
                        FP[d][idx] = fpVal;
                        C[d][idx]  = f;
                        maxv       = Math.max(maxv, f);
                    }
                }
            }

            if (!inSS) {
                if (maxv - ss.getmin() == 1 || ss.tot < K) {
                    int id = ss.getid();
                    ss.add2(ss.location(x), id);
                    ss.str[id] = x;
                    ss.sum[id] = maxv;
                    ss.link(id, 0);
                    while (ss.tot > K) {
                        int lvl = ss.Right[0];
                        int v   = ss.head[lvl];
                        ss.cut(v);
                        ss.recycling(v);
                    }
                    inSS = true;
                }
            } else {
                int p = ss.find(x);
                if (maxv > ss.sum[p]) {
                    int tmp = ss.Left[ss.sum[p]];
                    ss.cut(p);
                    if (ss.head[ss.sum[p]] != 0) tmp = ss.sum[p];
                    ss.sum[p] = maxv;
                    ss.link(p, tmp);
                }
            }
        }
    }

    @Override
    public int query(byte[] key) {
        String x = new String(key, 0, FP_LEN, StandardCharsets.ISO_8859_1);
        int p = ss.find(x);
        return (p == 0) ? 0 : ss.sum[p];
    }

    @Override
    public void getHeavyHitters(int K, List<Map.Entry<String, Integer>> results) {
        results.clear();
        List<Map.Entry<String,Integer>> topk = ss.getTopK();
        for (int i = 0; i < Math.min(K, topk.size()); i++) {
            results.add(topk.get(i));
        }
    }
}
