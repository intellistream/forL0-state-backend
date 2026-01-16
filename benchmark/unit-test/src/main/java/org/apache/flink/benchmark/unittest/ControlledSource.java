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

package org.apache.flink.benchmark.unittest;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

/**
 * A fully controllable source that generates key indices with configurable distribution.
 * 
 * <p>Distribution is controlled by zipfExponent:
 * <ul>
 *   <li>zipfExponent = 0: Uniform distribution</li>
 *   <li>zipfExponent > 0: Zipf distribution (higher = more skewed)</li>
 * </ul>
 */
public class ControlledSource extends RichSourceFunction<Integer> {

    private static final long serialVersionUID = 1L;

    private final int numKeys;
    private final long numOperations;
    private final double zipfExponent;
    private final int arrivalRate;

    private volatile boolean running = true;

    /**
     * Creates a controlled source.
     *
     * @param numKeys       number of unique keys (0 to numKeys-1)
     * @param numOperations total number of operations to generate
     * @param zipfExponent  Zipf s parameter (0 = uniform, > 0 = skewed)
     * @param arrivalRate   target operations per second (0 = unlimited)
     */
    public ControlledSource(int numKeys, long numOperations, double zipfExponent, int arrivalRate) {
        this.numKeys = numKeys;
        this.numOperations = numOperations;
        this.zipfExponent = zipfExponent;
        this.arrivalRate = arrivalRate;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        long startEmit = System.currentTimeMillis();
        System.out.println("[Source] Starting emission (real-time key generation)...");
        
        // Initialize key generator (no pre-generation needed!)
        final KeyGenerator keyGen;
        if (zipfExponent > 0) {
            // Zipf distribution
            ZipfDistribution zipf = new ZipfDistribution(numKeys, zipfExponent);
            keyGen = () -> zipf.sample() - 1;  // ZipfDistribution is 1-based
        } else {
            // Uniform distribution
            Random random = new Random(42);
            keyGen = () -> random.nextInt(numKeys);
        }
        
        // Batch emission to reduce synchronization overhead
        final int BATCH_SIZE = 100000;  // Much larger batch
        long emitted = 0;
        long lastReport = System.currentTimeMillis();
        
        while (emitted < numOperations && running) {
            int batchCount = (int) Math.min(BATCH_SIZE, numOperations - emitted);
            
            // Emit batch with single lock
            synchronized (ctx.getCheckpointLock()) {
                for (int i = 0; i < batchCount; i++) {
                    ctx.collect(keyGen.nextKey());
                }
            }
            
            emitted += batchCount;
            
            // Progress report every 10s
            long now = System.currentTimeMillis();
            if (now - lastReport > 10000) {
                double progress = 100.0 * emitted / numOperations;
                System.out.printf("[Source] Progress: %.1f%% (%d / %d)%n", progress, emitted, numOperations);
                lastReport = now;
            }
            
            // Rate limiting (batch-level, not per-record)
            if (arrivalRate > 0) {
                long sleepMs = (batchCount * 1000L) / arrivalRate;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        }
        
        long emitTime = System.currentTimeMillis() - startEmit;
        System.out.println("[Source] Emission completed: " + emitted + " records in " + emitTime + " ms");
        System.out.printf("[Source] Emission throughput: %.0f records/sec%n", emitted * 1000.0 / emitTime);
    }

    @Override
    public void cancel() {
        running = false;
    }

    @FunctionalInterface
    private interface KeyGenerator {
        int nextKey();
    }
}
