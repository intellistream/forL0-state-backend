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

package org.apache.flink.benchmark.wordcount.source;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * A source that generates words with Zipf distribution (skewed data).
 * 
 * <p>The Zipf distribution is commonly used to model real-world data skew,
 * where a small number of keys account for a large portion of the data.
 * 
 * <p>With skewFactor (s parameter):
 * <ul>
 *   <li>s = 1.0: approximately 20% skew</li>
 *   <li>s = 1.1: approximately 25% skew</li>
 *   <li>s = 1.2: approximately 30% skew</li>
 * </ul>
 */
public class SkewedWordSource extends RichParallelSourceFunction<Tuple2<String, Long>> {
    
    private static final long serialVersionUID = 1L;
    
    private final int numKeys;
    private final long numRecords;
    private final double skewFactor;
    /** Records per second (0 = unlimited) */
    private final int arrivalRate;
    
    private volatile boolean running = true;
    
    /**
     * Creates a skewed word source with unlimited arrival rate.
     * 
     * @param numKeys number of unique keys (words)
     * @param numRecords total number of records to generate
     * @param skewFactor Zipf distribution s parameter (higher = more skew)
     */
    public SkewedWordSource(int numKeys, long numRecords, double skewFactor) {
        this(numKeys, numRecords, skewFactor, 0);
    }
    
    /**
     * Creates a skewed word source with controlled arrival rate.
     * 
     * @param numKeys number of unique keys (words)
     * @param numRecords total number of records to generate
     * @param skewFactor Zipf distribution s parameter (higher = more skew)
     * @param arrivalRate target records per second (0 = unlimited)
     */
    public SkewedWordSource(int numKeys, long numRecords, double skewFactor, int arrivalRate) {
        this.numKeys = numKeys;
        this.numRecords = numRecords;
        this.skewFactor = skewFactor;
        this.arrivalRate = arrivalRate;
    }
    
    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        
        // Each subtask generates its portion of records
        long recordsPerSubtask = numRecords / parallelism;
        long startRecord = subtaskIndex * recordsPerSubtask;
        long endRecord = (subtaskIndex == parallelism - 1) 
            ? numRecords  // Last subtask handles remaining records
            : startRecord + recordsPerSubtask;
        
        // Create distribution for key selection
        // When skewFactor <= 0, use uniform distribution; otherwise use Zipf distribution
        final boolean useUniform = skewFactor <= 0;
        ZipfDistribution zipf = useUniform ? null : new ZipfDistribution(numKeys, skewFactor);
        Random random = new Random(subtaskIndex);  // Seed for reproducibility
        
        // Rate limiting: calculate per-subtask rate
        int ratePerSubtask = arrivalRate > 0 ? arrivalRate / parallelism : 0;
        long batchSize = ratePerSubtask > 0 ? Math.max(1, ratePerSubtask / 100) : 10000;  // ~10ms batches
        long batchIntervalNanos = ratePerSubtask > 0 ? (batchSize * 1_000_000_000L) / ratePerSubtask : 0;
        
        long count = 0;
        long batchCount = 0;
        long lastBatchTime = System.nanoTime();
        
        while (running && count < (endRecord - startRecord)) {
            // Generate key using uniform or Zipf distribution
            long keyId = useUniform ? random.nextInt(numKeys) : zipf.sample();
            String key = "word_" + keyId;
            
            // Emit record: (key, 1L)
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(key, 1L));
            }
            
            count++;
            batchCount++;
            
            // Rate limiting
            if (ratePerSubtask > 0 && batchCount >= batchSize) {
                long elapsed = System.nanoTime() - lastBatchTime;
                long sleepNanos = batchIntervalNanos - elapsed;
                if (sleepNanos > 1_000_000) {  // Only sleep if > 1ms
                    try {
                        Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                lastBatchTime = System.nanoTime();
                batchCount = 0;
            } else if (ratePerSubtask == 0 && batchCount >= 10000) {
                batchCount = 0;
            }
            
            // Progress logging
            if (count % 10_000_000 == 0) {
                System.out.printf("[Subtask %d] Generated %d / %d records (%.1f%%)%n",
                    subtaskIndex, count, (endRecord - startRecord),
                    100.0 * count / (endRecord - startRecord));
            }
        }
        
        System.out.printf("[Subtask %d] Finished generating %d records%n", subtaskIndex, count);
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
