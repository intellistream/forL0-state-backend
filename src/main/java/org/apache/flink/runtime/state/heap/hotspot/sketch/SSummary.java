package org.apache.flink.runtime.state.heap.hotspot.sketch;

import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.AbstractMap;

public class SSummary {
    private final int M    = Config.INSTANCE.summaryMaxEntries;
    private final int N    = Config.INSTANCE.summaryChainDepth;
    private final int LEN2 = Config.INSTANCE.summaryHashSlots;

    public int tot;
    public final int[] sum;
    public final int K;
    public final int[] last, Next, ID;
    public final int[] head, Left, Right;
    public int num;
    public final String[] str;
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
        for (int i = 1; i <= M + 2; i++) ID[i] = i;
        Right[0] = N;
        Left[N]  = 0;
    }

    public int getid() {
        int i = ID[num--];
        last[i] = Next[i] = sum[i] = Next2[i] = 0;
        str[i] = null;
        return i;
    }

    public int location(String s) {
        int h = bobhash.run(s.getBytes(), s.length());
        return Math.floorMod(h, LEN2);
    }

    public void add2(int x, int y) {
        Next2[y] = head2[x];
        head2[x] = y;
    }

    public int find(String s) {
        int w = location(s);
        for (int i = head2[w]; i != 0; i = Next2[i]) {
            if (s.equals(str[i])) return i;
        }
        return 0;
    }

    public int getmin() {
        if (tot < K) return 0;
        int first = Right[0];
        return first == N ? 1 : first;
    }

    /**
     * 插入或更新：将节点 i 放入频次链表，ww 是旧频次参照。
     * 所有 sum[i] 使用前均 clamp 到 [0,N].
     */
    public void link(int i, int ww) {
        int freq = sum[i];
        if (freq < 0) freq = 0;
        if (freq > N) freq = N;
        // clamp ww as well
        int prev = ww;
        if (prev < 0) prev = 0;
        if (prev > N) prev = N;

        tot++;
        boolean first = (head[freq] == 0);
        Next[i] = head[freq];
        if (Next[i] != 0) last[Next[i]] = i;
        last[i] = 0;
        head[freq] = i;

        if (first) {
            int insertAfter = prev;
            for (int j = freq - 1; j > 0 && j > freq - 10; j--) {
                if (head[j] != 0) {
                    insertAfter = j;
                    break;
                }
            }
            linkhead(freq, insertAfter);
        }
    }

    public void cut(int i) {
        int freq = sum[i];
        if (freq < 0) freq = 0;
        if (freq > N) freq = N;

        tot--;
        if (head[freq] == i) head[freq] = Next[i];
        if (head[freq] == 0) cuthead(freq);
        int l = last[i], n = Next[i];
        if (l != 0) Next[l] = n;
        if (n != 0) last[n] = l;
    }

    public void recycling(int i) {
        if (str[i] == null) return;
        int w = location(str[i]);
        if (head2[w] == i) {
            head2[w] = Next2[i];
        } else {
            for (int j = head2[w]; j != 0; j = Next2[j]) {
                if (Next2[j] == i) {
                    Next2[j] = Next2[i];
                    break;
                }
            }
        }
        ID[++num] = i;
    }

    private void linkhead(int i, int j) {
        Left[i]     = j;
        Right[i]    = Right[j];
        Right[j]    = i;
        Left[Right[i]] = i;
    }

    private void cuthead(int i) {
        int l = Left[i], r = Right[i];
        Right[l] = r;
        Left[r]  = l;
    }

    /**
     * 返回当前 Top-K heavy hitters，按频次降序。
     */
    public List<Entry<String, Integer>> getTopK() {
        List<Entry<String, Integer>> list = new ArrayList<>(K);
        int count = 0;
        for (int lvl = Right[0]; lvl != 0 && count < K; lvl = Right[lvl]) {
            for (int i = head[lvl]; i != 0 && count < K; i = Next[i]) {
                list.add(new AbstractMap.SimpleEntry<>(str[i], sum[i]));
                count++;
            }
        }
        return list;
    }
}