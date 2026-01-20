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
import org.apache.flink.runtime.state.VoidNamespace;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micro-benchmark for StateMap implementations (ForL0StateMap vs CopyOnWriteStateMap).
 * 
 * <p>Uses the same testing methodology as Flink State Benchmark:
 * <ul>
 *   <li>KeyValue inner class with @Setup(Level.Invocation)</li>
 *   <li>Single operation per benchmark (no batching)</li>
 *   <li>Pre-generated keys shuffled for random access</li>
 * </ul>
 * 
 * <p>Uses Long key + VoidNamespace to match WordCount benchmark and enable
 * SwissTableLongVoid specialization for ForL0StateMap.
 * 
 * <p>This configuration uses SwissTableLongVoid specialization which:
 * <ul>
 *   <li>Stores keys as primitive long[] (no boxing overhead)</li>
 *   <li>Uses identity comparison for VoidNamespace (no equals() overhead)</li>
 *   <li>Achieves optimal memory layout and cache efficiency</li>
 * </ul>
 */
@State(Scope.Benchmark)
public class StateMapBenchmark extends BenchmarkBase {

    // Key counts - aligned with Flink State Benchmark
    public static final int setupKeyCount = 500_000;       // 已存在的 keys
    public static final int newKeyCount = 500_000;         // 新插入的 keys
    public static final int randomValueCount = 1_000_000;  // 随机 values
    
    // Pre-generated keys (shuffled for random access)
    public static final ArrayList<Long> setupKeys = new ArrayList<>(setupKeyCount);
    public static final ArrayList<Long> newKeys = new ArrayList<>(newKeyCount);
    public static final ArrayList<Long> randomValues = new ArrayList<>(randomValueCount);
    
    static {
        for (long i = 0; i < setupKeyCount; i++) {
            setupKeys.add(i);
        }
        Collections.shuffle(setupKeys);
    }
    
    static {
        for (long i = 0; i < newKeyCount; i++) {
            newKeys.add(i + setupKeyCount);  // 确保 newKeys 与 setupKeys 不重叠
        }
        Collections.shuffle(newKeys);
    }
    
    static {
        for (long i = 0; i < randomValueCount; i++) {
            randomValues.add(i);
        }
        Collections.shuffle(randomValues);
    }
    
    // Thread-safe key index for KeyValue
    protected static AtomicInteger keyIndex;
    
    @Param({"FORL0", "COPYONWRITE"})
    private String mapType;
    
    @SuppressWarnings("rawtypes")
    private StateMap stateMap;
    
    // VoidNamespace (单一 namespace)
    private static final VoidNamespace NAMESPACE = VoidNamespace.INSTANCE;
    
    private static final StateTransformationFunction<Long, Long> TRANSFORM_FUNC = 
        (oldState, value) -> oldState == null ? value : oldState + value;
    
    private static int getCurrentIndex() {
        int currentIndex = keyIndex.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            keyIndex.set(0);
        }
        return currentIndex;
    }

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
        // 创建 StateMap - 使用带类型的构造函数以启用特化版本
        if ("FORL0".equals(mapType)) {
            // 使用 Long.class + VoidNamespace.class 构造函数
            // 这会创建 SwissTableLongVoid 特化版本
            stateMap = new ForL0StateMap<>(Long.class, VoidNamespace.class);
        } else {
            stateMap = new CopyOnWriteStateMap<>(LongSerializer.INSTANCE);
        }
        
        // 预填充数据 - 与 Flink State Benchmark 一致
        for (int i = 0; i < setupKeyCount; i++) {
            stateMap.put(setupKeys.get(i), NAMESPACE, randomValues.get(i % randomValueCount));
        }
        
        // 初始化 key index
        keyIndex = new AtomicInteger();
        
        // 打印验证信息
        if (stateMap instanceof ForL0StateMap) {
            System.out.println("[Setup] ForL0StateMap created - size: " + stateMap.size());
        } else {
            System.out.println("[Setup] CopyOnWriteStateMap created - size: " + stateMap.size());
        }
    }
    
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (stateMap instanceof ForL0StateMap) {
            ((ForL0StateMap<?, ?, ?>) stateMap).close();
        }
        stateMap = null;
    }
    
    /**
     * KeyValue holder for per-invocation key generation.
     * Same pattern as Flink State Benchmark.
     */
    @State(Scope.Thread)
    public static class KeyValue {
        public long setUpKey;  // 已存在的 key
        public long newKey;    // 新 key
        public long value;     // 随机 value
        
        @Setup(Level.Invocation)
        public void kvSetup() {
            int currentIndex = getCurrentIndex();
            setUpKey = setupKeys.get(currentIndex % setupKeyCount);
            newKey = newKeys.get(currentIndex % newKeyCount);
            value = randomValues.get(currentIndex % randomValueCount);
        }
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    public void mapPut(KeyValue keyValue) {
        stateMap.put(keyValue.setUpKey, NAMESPACE, keyValue.value);
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    public Long mapGet(KeyValue keyValue) {
        return (Long) stateMap.get(keyValue.setUpKey, NAMESPACE);
    }

    @SuppressWarnings("unchecked")
    @Benchmark
    public void mapTransform(KeyValue keyValue) throws Exception {
        stateMap.transform(keyValue.setUpKey, NAMESPACE, keyValue.value, TRANSFORM_FUNC);
    }
    
    @SuppressWarnings("unchecked")
    @Benchmark
    public Long mapPutAndGetOld(KeyValue keyValue) {
        return (Long) stateMap.putAndGetOld(keyValue.setUpKey, NAMESPACE, keyValue.value);
    }
    
    @SuppressWarnings("unchecked")
    @Benchmark
    public boolean mapContainsKey(KeyValue keyValue) {
        return stateMap.containsKey(keyValue.setUpKey, NAMESPACE);
    }
}
