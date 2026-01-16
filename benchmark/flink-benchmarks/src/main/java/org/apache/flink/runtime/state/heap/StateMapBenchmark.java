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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;

/**
 * Micro-benchmark for StateMap implementations (ForL0StateMap vs CopyOnWriteStateMap).
 * 
 * <p>Uses batch operations with @OperationsPerInvocation to avoid the overhead of
 * @Setup(Level.Invocation) which would dominate the measurement for fast operations.
 * 
 * <p>Supports two namespace types to analyze equals() overhead impact:
 * <ul>
 *   <li>STRING: String namespace with expensive equals() (~70ns per call)</li>
 *   <li>TIMEWINDOW: TimeWindow namespace with cheap equals() (~2ns per call)</li>
 * </ul>
 * 
 * <p>Key insight: ForL0StateMap (Swiss Table) uses H2 filtering to reduce equals() calls,
 * making it faster for expensive equals() types like String.
 */
@State(Scope.Benchmark)
public class StateMapBenchmark extends BenchmarkBase {

    private static final int KEY_RANGE = 1_000_000;       // 与 WordCount numKeys 一致
    private static final int NAMESPACE_COUNT = 10;        // 模拟滑动窗口数量 (windowSize/slideSize)
    private static final int PREFILL_COUNT = 10_000_000;  // 预填充操作数
    private static final int BATCH_SIZE = 10_000;         // 每次 benchmark 批量大小
    private static final long WINDOW_SIZE = 5000;         // 窗口大小 (毫秒), 与 WordCount 一致
    private static final long SLIDE_SIZE = 200;           // 滑动步长 (毫秒), 与 WordCount 一致
    
    @Param({"FORL0", "COPYONWRITE"})
    private String mapType;
    
    @Param({"STRING", "TIMEWINDOW"})
    private String nsType;
    
    @SuppressWarnings("rawtypes")
    private StateMap stateMap;
    
    // 预生成 String keys
    private String[] keys;
    
    // 两种 namespace 类型
    private String[] stringNamespaces;
    private TimeWindow[] timeWindowNamespaces;
    private Object[] namespaces;  // 实际使用的 namespace 数组
    
    private Long[] values;
    
    // 预生成随机访问序列（与 JUnit 保持一致）
    private int[] randomKeyIndices;
    private int[] randomNsIndices;
    
    private static final StateTransformationFunction<Long, Long> TRANSFORM_FUNC = 
        (oldState, value) -> oldState == null ? value : oldState + value;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + StateMapBenchmark.class.getCanonicalName() + ".*")
                        .build();
        new Runner(opt).run();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random(42);  // 固定种子
        
        // 预生成所有 String keys
        keys = new String[KEY_RANGE];
        for (int i = 0; i < KEY_RANGE; i++) {
            keys[i] = "word_" + i;  // 与 WordCount SkewedWordSource 一致
        }
        
        // 预生成两种类型的 namespace
        stringNamespaces = new String[NAMESPACE_COUNT];
        timeWindowNamespaces = new TimeWindow[NAMESPACE_COUNT];
        
        long baseTime = 1000000000000L;  // 固定基准时间
        for (int i = 0; i < NAMESPACE_COUNT; i++) {
            stringNamespaces[i] = "window_" + i;
            // 使用分散的时间戳
            long start = baseTime + i * 3600000L;
            long end = start + WINDOW_SIZE;
            timeWindowNamespaces[i] = new TimeWindow(start, end);
        }
        
        // 根据参数选择 namespace 类型
        if ("STRING".equals(nsType)) {
            namespaces = stringNamespaces;
        } else {
            namespaces = timeWindowNamespaces;
        }
        
        // 预生成随机访问序列和 values
        values = new Long[BATCH_SIZE];
        randomKeyIndices = new int[BATCH_SIZE];
        randomNsIndices = new int[BATCH_SIZE];
        
        for (int i = 0; i < BATCH_SIZE; i++) {
            values[i] = random.nextLong();
            randomKeyIndices[i] = random.nextInt(KEY_RANGE);
            randomNsIndices[i] = random.nextInt(NAMESPACE_COUNT);
        }
        
        // 创建 StateMap
        if ("FORL0".equals(mapType)) {
            stateMap = new ForL0StateMap<>();
        } else {
            stateMap = new CopyOnWriteStateMap<>(LongSerializer.INSTANCE);
        }
        
        // 预填充数据
        Random prefillRandom = new Random(42);
        for (int i = 0; i < PREFILL_COUNT; i++) {
            int keyIdx = prefillRandom.nextInt(KEY_RANGE);
            int nsIdx = prefillRandom.nextInt(NAMESPACE_COUNT);
            stateMap.put(keys[keyIdx], namespaces[nsIdx], (long) i);
        }
        
        System.out.println("[Setup] " + mapType + " (ns=" + nsType + ") stateMap size: " + stateMap.size());
    }
    
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (stateMap instanceof ForL0StateMap) {
            ((ForL0StateMap<?, ?, ?>) stateMap).close();
        }
        stateMap = null;
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void mapPut() {
        for (int i = 0; i < BATCH_SIZE; i++) {
            stateMap.put(keys[randomKeyIndices[i]], namespaces[randomNsIndices[i]], values[i]);
        }
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long mapGet() {
        long sum = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            String key = keys[randomKeyIndices[i]];
            Object ns = namespaces[randomNsIndices[i]];
            Long value = (Long) stateMap.get(key, ns);
            if (value != null) {
                sum += value;
            }
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void mapUpdate() {
        for (int i = 0; i < BATCH_SIZE; i++) {
            String key = keys[randomKeyIndices[i]];
            Object ns = namespaces[randomNsIndices[i]];
            stateMap.put(key, ns, values[i]);
        }
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void mapTransform() throws Exception {
        for (int i = 0; i < BATCH_SIZE; i++) {
            String key = keys[randomKeyIndices[i]];
            Object ns = namespaces[randomNsIndices[i]];
            stateMap.transform(key, ns, values[i], TRANSFORM_FUNC);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long mapPutAndGetOld() {
        long sum = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            String key = keys[randomKeyIndices[i]];
            Object ns = namespaces[randomNsIndices[i]];
            Long old = (Long) stateMap.putAndGetOld(key, ns, values[i]);
            if (old != null) {
                sum += old;
            }
        }
        return sum;
    }
    
    @SuppressWarnings("unchecked")
    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public int mapContainsKey() {
        int count = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            String key = keys[randomKeyIndices[i]];
            Object ns = namespaces[randomNsIndices[i]];
            if (stateMap.containsKey(key, ns)) {
                count++;
            }
        }
        return count;
    }
}
