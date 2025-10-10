package org.apache.flink.runtime.state.heap.hotspot.simulator;

import org.apache.flink.runtime.state.heap.hotspot.sketch.Sketch;
import org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch.ElasticSketch;
import org.apache.flink.runtime.state.heap.hotspot.sketch.HeavyKeeper;
import org.apache.flink.runtime.state.heap.hotspot.sketch.WavingSketch.WavingSketch;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Simulator {
    private static final int KEY_LEN       = 13;
    private static final int FP_LEN        = Config.INSTANCE.keyLength4;
    private static final int START_FILE_NO = 1;
    private static final int END_FILE_NO   = 1;

    @SuppressWarnings("unchecked")
    private static final List<byte[]>[] traces = new ArrayList[END_FILE_NO - START_FILE_NO + 1];

    public static void main(String[] args) throws IOException {
        readInTraces(Config.INSTANCE.traceDir);
        runAllTraces(Config.INSTANCE.topK);
    }

    private static void runAllTraces(int K) {
        double sumAcc = 0.0;
        Sketch sketch = createSketchByConfig();

        for (int fileIdx = START_FILE_NO; fileIdx <= END_FILE_NO; fileIdx++) {
            List<byte[]> flowList = traces[fileIdx - START_FILE_NO];

            // build and sort true frequencies
            Map<String,Integer> trueFreq = buildTrueFrequencies(flowList);
            List<Map.Entry<String,Integer>> sorted = new ArrayList<>(trueFreq.entrySet());
            sorted.sort((a,b) -> Integer.compare(b.getValue(), a.getValue()));

            // ----- DEBUG: real Top-K -----
            List<Map.Entry<String,Integer>> trueTopKList = new ArrayList<>();
            for (int i = 0; i < Math.min(K, sorted.size()); i++) {
                trueTopKList.add(sorted.get(i));
            }
            /*System.out.println("[DEBUG] True Top-" + K + " for trace " + fileIdx + ":");
            for (int i = 0; i < trueTopKList.size(); i++) {
                Map.Entry<String,Integer> e = trueTopKList.get(i);
                System.out.printf("  #%d: key=%s, real=%d%n",
                        i+1, e.getKey(), e.getValue());
            }*/
            // --------------------------------

            // insert all into sketch
            sketch.clear();
            sketch.insertAll(flowList);

            // predict Top-K
            List<Map.Entry<String,Integer>> preds = new ArrayList<>();
            sketch.getHeavyHitters(K, preds);

            // ----- DEBUG: predicted Top-K -----
            /*System.out.println("[DEBUG] Predicted Top-" + K + " for trace " + fileIdx + ":");
            for (int i = 0; i < preds.size(); i++) {
                Map.Entry<String,Integer> e = preds.get(i);
                System.out.printf("  #%d: key=%s, est=%d%n",
                        i+1, e.getKey(), e.getValue());
            }*/
            // ------------------------------------

            // compute accepted rate
            Set<String> trueTopKSet = new HashSet<>();
            for (Map.Entry<String,Integer> e : trueTopKList) {
                trueTopKSet.add(e.getKey());
            }
            int ok = 0;
            for (Map.Entry<String,Integer> e : preds) {
                if (trueTopKSet.contains(e.getKey())) ok++;
            }
            double acc = ok / (double)K;
            System.out.printf("[%s] Trace %d Accepted Rate: %.6f%n",
                    Config.INSTANCE.algorithm, fileIdx, acc);
            sumAcc += acc;
        }

        double avg = sumAcc / (END_FILE_NO - START_FILE_NO + 1);
        System.out.println("\nAverage Accepted Rate: " +
                String.format("%.6f", avg));
    }

    private static Sketch createSketchByConfig() {
        String algo = Config.INSTANCE.algorithm.toLowerCase();
        switch (algo) {
            case "elastic":
                return new ElasticSketch();
            case "heavykeeper":
                return new HeavyKeeper();
            case "wavingsketch":
                return new WavingSketch();
            default:
                throw new IllegalArgumentException("Unsupported algorithm: " + algo);
        }
    }

    private static Map<String,Integer> buildTrueFrequencies(List<byte[]> flowList) {
        Map<String,Integer> freq = new HashMap<>();
        for (byte[] buf : flowList) {
            String key = new String(buf, 0, FP_LEN, StandardCharsets.ISO_8859_1);
            freq.merge(key, 1, Integer::sum);
        }
        return freq;
    }

    private static void readInTraces(String traceDir) throws IOException {
        String sep = File.separator;
        for (int idx = START_FILE_NO; idx <= END_FILE_NO; idx++) {
            String filename = traceDir + sep + (idx - 1) + ".dat";
            List<byte[]> list = new ArrayList<>();
            try (FileInputStream fis = new FileInputStream(filename)) {
                byte[] buf = new byte[KEY_LEN];
                @SuppressWarnings("unused")
                int r;  // Result of read operation - kept for potential error checking
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
