/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark.benchset.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.SplittableRandom;

/**
 * High-performance event source for benchmark.
 *
 * <p>Design goals:
 * <ul>
 *   <li>Minimize source overhead - should NOT be a bottleneck</li>
 *   <li>Use primitive long directly - no object allocation</li>
 *   <li>Support uniform and Zipf distribution</li>
 *   <li>Lock-free emission using SplittableRandom</li>
 * </ul>
 *
 * <p>Output: long values representing event keys (e.g., userId, sessionId)
 */
public class LongEventSource extends RichParallelSourceFunction<Long> {

    private static final long serialVersionUID = 1L;

    private final int numKeys;
    private final long numRecords;
    private final double skewFactor;

    private volatile boolean running = true;

    /**
     * Creates a high-performance long event source.
     *
     * @param numKeys     number of unique keys
     * @param numRecords  total number of records to generate
     * @param skewFactor  Zipf s parameter (0 = uniform, &gt;0 = Zipf)
     */
    public LongEventSource(int numKeys, long numRecords, double skewFactor) {
        this.numKeys = numKeys;
        this.numRecords = numRecords;
        this.skewFactor = skewFactor;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

        // Calculate this subtask's portion
        long recordsPerSubtask = numRecords / parallelism;
        long myRecords = (subtaskIndex == parallelism - 1)
                ? numRecords - recordsPerSubtask * (parallelism - 1)
                : recordsPerSubtask;

        // Use SplittableRandom for better performance than java.util.Random
        SplittableRandom random = new SplittableRandom(subtaskIndex * 31L + 17);

        // Pre-compute Zipf distribution if needed
        final boolean useZipf = skewFactor > 0;
        final double[] cumulativeProb;
        if (useZipf) {
            cumulativeProb = computeZipfCDF(numKeys, skewFactor);
        } else {
            cumulativeProb = null;
        }

        long count = 0;
        final Object lock = ctx.getCheckpointLock();

        // Main generation loop - optimized for throughput
        while (running && count < myRecords) {
            // Generate key
            long key;
            if (useZipf) {
                key = sampleZipf(random, cumulativeProb);
            } else {
                key = random.nextLong(numKeys);
            }

            // Emit without synchronization when possible
            synchronized (lock) {
                ctx.collect(key);
            }
            count++;

            // Progress logging every 50M records
            if (count % 50_000_000 == 0) {
                System.out.printf("[Source %d] Generated %,d / %,d records (%.1f%%)%n",
                        subtaskIndex, count, myRecords, 100.0 * count / myRecords);
            }
        }

        System.out.printf("[Source %d] Finished generating %,d records%n", subtaskIndex, count);
    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * Pre-compute Zipf cumulative distribution function for fast sampling.
     */
    private static double[] computeZipfCDF(int n, double s) {
        double[] cdf = new double[n];
        double sum = 0;
        for (int i = 1; i <= n; i++) {
            sum += 1.0 / Math.pow(i, s);
        }
        double cumulative = 0;
        for (int i = 0; i < n; i++) {
            cumulative += (1.0 / Math.pow(i + 1, s)) / sum;
            cdf[i] = cumulative;
        }
        return cdf;
    }

    /**
     * Sample from pre-computed Zipf CDF using binary search.
     */
    private static long sampleZipf(SplittableRandom random, double[] cdf) {
        double u = random.nextDouble();
        int low = 0, high = cdf.length - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (cdf[mid] < u) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }
}
