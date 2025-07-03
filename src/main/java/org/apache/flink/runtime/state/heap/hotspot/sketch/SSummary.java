package org.apache.flink.runtime.state.heap.hotspot.sketch;

import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.AbstractMap;

public class SSummary {
    private final int M    = Config.INSTANCE.summaryMaxEntries;   // total slots for summary
    private final int N    = Config.INSTANCE.summaryChainDepth;   // max possible frequency bucket
    private final int LEN2 = Config.INSTANCE.summaryHashSlots;    // hash‐by‐key slots

    public int tot;                // current number of entries in summary
    public final int[] sum;        // frequency counts
    public final int K;            // Top-K capacity
    public final int[] last, Next, ID;
    public final int[] head, Left, Right;
    public int num;                // free-list pointer
    public final String[] str;     // keys
    public final int[] head2, Next2;

    private final Hash bobhash;

    public SSummary(int K) {
        this.K = K;
        this.bobhash = new Hash(1000);

        sum   = new int[M + 10];
        last  = new int[M + 10];
        Next  = new int[M + 10];
        ID    = new int[M + 10];
        head  = new int[N + 10];
        Left  = new int[N + 10];
        Right = new int[N + 10];
        str   = new String[M + 10];
        head2 = new int[LEN2 + 10];
        Next2 = new int[M + 10];

        clear();
    }

    public void clear() {
        Arrays.fill(sum,    0);
        Arrays.fill(last,   0);
        Arrays.fill(Next,   0);
        Arrays.fill(Next2,  0);
        Arrays.fill(head,   0);
        Arrays.fill(Left,   0);
        Arrays.fill(Right,  0);
        Arrays.fill(head2,  0);

        tot = 0;
        num = M + 2;
        for (int i = 1; i <= M + 2; i++) {
            ID[i] = i;
        }
        Right[0] = N;
        Left[N]  = 0;
    }

    public int getid() {
        int i = ID[num--];
        last[i]  = 0;
        Next[i]  = 0;
        sum[i]   = 0;
        Next2[i] = 0;
        str[i]   = null;
        return i;
    }

    public int location(String s) {
        int h = bobhash.run(s.getBytes(), s.length());
        return Math.floorMod(h, LEN2);
    }

    public void add2(int bucket, int id) {
        Next2[id]    = head2[bucket];
        head2[bucket] = id;
    }

    public int find(String s) {
        int w = location(s);
        for (int i = head2[w]; i != 0; i = Next2[i]) {
            if (s.equals(str[i])) {
                return i;
            }
        }
        return 0;
    }

    public int getmin() {
        if (tot < K) return 0;
        int lowestBucket = Right[0];
        return lowestBucket == N ? 1 : lowestBucket;
    }

    public void link(int id, int prevFreq) {
        int freq = sum[id];
        if (freq < 0) freq = 0;
        if (freq > N) freq = N;
        int p = prevFreq;
        if (p < 0) p = 0;
        if (p > N) p = N;

        tot++;
        boolean wasEmpty = (head[freq] == 0);
        Next[id] = head[freq];
        if (Next[id] != 0) {
            last[Next[id]] = id;
        }
        last[id]  = 0;
        head[freq] = id;

        if (wasEmpty) {
            // insert bucket freq into bucket‐list after p
            int insertAfter = p;
            for (int j = freq - 1; j > 0; j--) {
                if (head[j] != 0) {
                    insertAfter = j;
                    break;
                }
            }
            // link layer node
            Left[freq]        = insertAfter;
            Right[freq]       = Right[insertAfter];
            Right[insertAfter] = freq;
            Left[Right[freq]]  = freq;
        }
    }

    public void cut(int id) {
        int freq = sum[id];
        if (freq < 0) freq = 0;
        if (freq > N) freq = N;

        tot--;
        if (head[freq] == id) {
            head[freq] = Next[id];
        }
        if (head[freq] == 0) {
            // remove freq layer from bucket‐list
            int l = Left[freq], r = Right[freq];
            Right[l] = r;
            Left[r]  = l;
        }
        int l = last[id], n = Next[id];
        if (l != 0) Next[l] = n;
        if (n != 0) last[n]  = l;
    }

    public void recycling(int id) {
        if (str[id] == null) return;
        int w = location(str[id]);
        if (head2[w] == id) {
            head2[w] = Next2[id];
        } else {
            for (int j = head2[w]; j != 0; j = Next2[j]) {
                if (Next2[j] == id) {
                    Next2[j] = Next2[id];
                    break;
                }
            }
        }
        ID[++num] = id;
    }

    /**
     * 返回 Top-K heavy hitters，按频次从高到低。
     */
    public List<Entry<String, Integer>> getTopK() {
        List<Entry<String, Integer>> list = new ArrayList<>(K);
        int count = 0;

        // 从最高频层向下遍历
        for (int lvl = Left[0]; lvl != 0 && count < K; lvl = Left[lvl]) {
            for (int i = head[lvl]; i != 0 && count < K; i = Next[i]) {
                list.add(new AbstractMap.SimpleEntry<>(str[i], sum[i]));
                count++;
            }
        }
        return list;
    }
}