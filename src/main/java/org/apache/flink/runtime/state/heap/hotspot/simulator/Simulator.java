package org.apache.flink.runtime.state.heap.hotspot.simulator;

import org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch.ElasticSketch;
import org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch.ElasticSketch.Pair;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Simulator {
    private static final int KEY_LEN       = 13;
    private static final int FP_LEN        = 4;
    private static final int START_FILE_NO = 1;
    private static final int END_FILE_NO   = 11;

    @SuppressWarnings("unchecked")
    private static final List<byte[]>[] traces = new ArrayList[END_FILE_NO - START_FILE_NO + 1];

    public static void main(String[] args) throws IOException {
        Config cfg = Config.INSTANCE;
        int K = cfg.topK;  // Top-K 数量

        // 1) 读取所有 trace
        String traceDir = "./src/main/data";  // 根据工作目录调整
        readInTraces(traceDir);

        // 2) 初始化 ElasticSketch
        ElasticSketch sketch = new ElasticSketch();

        double sum = 0;
        // 3) 对每个 trace 运行 Top-K 评测
        for (int fileIdx = START_FILE_NO; fileIdx <= END_FILE_NO; fileIdx++) {
            List<byte[]> flowList = traces[fileIdx - START_FILE_NO];

            // 3.1) 构建真实频次 B
            Map<String,Integer> B = new HashMap<>();
            for (byte[] buf : flowList) {
                String fp = new String(buf, 0, FP_LEN, StandardCharsets.ISO_8859_1);
                B.merge(fp, 1, Integer::sum);
            }

            // 3.2) 构建真实 Top-K 列表 sorted
            List<Map.Entry<String,Integer>> sortedTrue = new ArrayList<>(B.entrySet());
            sortedTrue.sort((a,b) -> b.getValue() - a.getValue());
            Set<String> C = new HashSet<>();
            for (int i = 0; i < Math.min(K, sortedTrue.size()); i++) {
                C.add(sortedTrue.get(i).getKey());
            }

            // 3.3) 插入并打印进度
            int m = flowList.size();
            sketch.clear();
            for (int i = 1; i <= m; i++) {
                if (i % (m/10) == 0) {
                    System.out.printf("Insert %d%n", i);
                }
                sketch.insert(flowList.get(i - 1));
            }
            System.out.println();

            // 3.4) 预测 Top-K
            System.out.println("Calculating");
            List<Pair<String,Integer>> pred = new ArrayList<>();
            sketch.getHeavyHitters(K, pred);

            /*
            // —— 调试输出开始 ——
            System.out.println("=== Debug: True Top-K ===");
            for (int i = 0; i < Math.min(K, sortedTrue.size()); i++) {
                Map.Entry<String,Integer> e = sortedTrue.get(i);
                System.out.printf("  [%d] fp=%s  real=%d%n", i, e.getKey(), e.getValue());
            }
            System.out.println("=== Debug: Predicted Top-K ===");
            for (int i = 0; i < pred.size(); i++) {
                Pair<String,Integer> p = pred.get(i);
                System.out.printf("  [%d] fp=%s  est=%d%n", i, p.first, p.second);
            }
            System.out.println("=== End Debug ===\n");
            // —— 调试输出结束 ——
             */

            // 3.5) 计算指标
            int accepted = 0;
            for (Pair<String,Integer> p : pred) {
                String fp = p.first;
                int est   = p.second;
                int real  = B.getOrDefault(fp, 0);
                if (C.contains(fp)) {
                    accepted++;
                }
            }
            double accRate = accepted / (double) K;
            sum += accRate;

            // 3.6) 输出结果
            System.out.println("ElasticSketch:");
            System.out.printf("Accepted: %d/%d  %.10f%n%n", accepted, K, accRate);
        }
        System.out.printf("Accepted: %.10f%n%n", sum / 11);
    }

    /**
     * 原有 readInTraces，加载 traceDir/0.dat…10.dat
     */
    private static void readInTraces(String traceDir) throws IOException {
        String sep = File.separator;
        for (int idx = START_FILE_NO; idx <= END_FILE_NO; idx++) {
            String filename = traceDir + sep + (idx - 1) + ".dat";
            List<byte[]> list = new ArrayList<>();
            try (FileInputStream fis = new FileInputStream(new File(filename))) {
                byte[] buf = new byte[KEY_LEN];
                int r;
                while ((r = fis.read(buf)) == KEY_LEN) {
                    list.add(Arrays.copyOf(buf, KEY_LEN));
                }
            }
            traces[idx - START_FILE_NO] = list;
            System.out.printf("Loaded %s: %d packets%n", filename, list.size());
        }
        System.out.println();
    }
}
