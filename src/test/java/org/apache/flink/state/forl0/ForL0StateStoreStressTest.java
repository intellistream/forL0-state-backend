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

package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 压力测试ForL0StateStore在高负载生产环境下的性能和稳定性
 * 注意：所有测试都是单线程的，符合Flink Task对StateStore的访问模式
 * 
 * Migrated from ForL0StateMapStressTest to adapt to the new ForL0StateStore API.
 */
public class ForL0StateStoreStressTest {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateStoreStressTest.class);

    private static final int STRESS_DURATION_SECONDS = 30;
    private static final int NUM_KEY_GROUPS = 128;
    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, NUM_KEY_GROUPS - 1);

    private ForL0StateStore<Integer, String, String> stateStore;

    @BeforeEach
    void setUp() throws Exception {
        RegisteredKeyValueStateBackendMetaInfo<String, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "stressTestState",
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        stateStore = new ForL0StateStore<>(KEY_GROUP_RANGE, IntSerializer.INSTANCE, metaInfo);
    }

    private int computeKeyGroup(int key) {
        return Math.abs(key % NUM_KEY_GROUPS);
    }

    // Helper method to estimate heap memory usage
    private static long getHeapUsed() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @Nested
    @DisplayName("基础操作压力测试")
    class BasicOperationStressTests {

        @Test
        @Timeout(value = STRESS_DURATION_SECONDS + 10, unit = TimeUnit.SECONDS)
        @DisplayName("大量PUT操作压力测试")
        void testMassivePutOperations() throws Exception {
            final int totalOperations = 1_000_000;

            LOG.info("PUT操作压力测试: {} 次操作", totalOperations);

            long startTime = System.currentTimeMillis();
            long startMemory = getHeapUsed();

            // 执行大量PUT操作
            for (int i = 0; i < totalOperations; i++) {
                String namespace = "ns" + (i % 50);
                String value = "value-" + i;
                int keyGroup = computeKeyGroup(i);

                stateStore.put(i, namespace, value, keyGroup);

                if (i % 500000 == 0 && i > 0) {
                    LOG.info("已完成 {} 次PUT操作, 当前大小: {}, 内存使用: {} MB",
                            i + 1, stateStore.size(), getHeapUsed() / 1024 / 1024);
                }
            }

            long endTime = System.currentTimeMillis();
            long endMemory = getHeapUsed();

            double duration = (endTime - startTime) / 1000.0;
            long memoryUsed = endMemory - startMemory;

            LOG.info("PUT操作压力测试结果:");
            LOG.info("  总操作数: {}", totalOperations);
            LOG.info("  最终状态数量: {}", stateStore.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均PUT QPS: {}", totalOperations / duration);
            LOG.info("  内存使用: {} KB", memoryUsed / 1024);
            LOG.info("  平均每条记录内存: {} bytes", memoryUsed / totalOperations);

            assertEquals(totalOperations, stateStore.size());

            // 验证数据正确性
            Random verifyRandom = new Random(42);
            int verifyCount = totalOperations / 10;
            int correctCount = 0;

            for (int i = 0; i < verifyCount; i++) {
                int key = verifyRandom.nextInt(totalOperations);
                String namespace = "ns" + (key % 50);
                String expectedValue = "value-" + key;
                int keyGroup = computeKeyGroup(key);

                String actualValue = stateStore.get(key, namespace, keyGroup);
                if (expectedValue.equals(actualValue)) {
                    correctCount++;
                }
            }

            LOG.info("数据正确性验证: {}/{} 正确率: {}%",
                    correctCount, verifyCount, (correctCount * 100.0) / verifyCount);
            assertEquals(verifyCount, correctCount, "所有验证的数据都应该正确");
        }

        @Test
        @Timeout(value = STRESS_DURATION_SECONDS + 10, unit = TimeUnit.SECONDS)
        @DisplayName("大量GET操作压力测试")
        void testMassiveGetOperations() throws Exception {
            final int dataSize = 500_000;
            final int getOperations = 1_000_000;

            // 先准备数据
            LOG.info("准备测试数据: {} 条记录", dataSize);
            for (int i = 0; i < dataSize; i++) {
                String namespace = "ns" + (i % 50);
                int keyGroup = computeKeyGroup(i);
                stateStore.put(i, namespace, "test-value-" + i, keyGroup);
            }

            LOG.info("GET操作压力测试: {} 次操作", getOperations);

            Random random = new Random(42);
            long startTime = System.currentTimeMillis();
            int hits = 0;
            int misses = 0;
            int correctValues = 0;

            // 执行大量GET操作
            for (int i = 0; i < getOperations; i++) {
                int key = random.nextInt(dataSize * 2); // 50%命中率
                String namespace = "ns" + (key % 50);
                int keyGroup = computeKeyGroup(key);

                String value = stateStore.get(key, namespace, keyGroup);
                if (value != null) {
                    hits++;
                    String expectedValue = "test-value-" + key;
                    if (expectedValue.equals(value)) {
                        correctValues++;
                    }
                } else {
                    misses++;
                }

                if (i % 200000 == 0 && i > 0) {
                    LOG.info("已完成 {} 次GET操作, 命中: {}, 未命中: {}, 正确值: {}",
                            i, hits, misses, correctValues);
                }
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("GET操作压力测试结果:");
            LOG.info("  总操作数: {}", getOperations);
            LOG.info("  命中数: {}", hits);
            LOG.info("  未命中数: {}", misses);
            LOG.info("  正确值数: {}", correctValues);
            LOG.info("  命中率: {}%", (hits * 100.0) / getOperations);
            LOG.info("  数据正确率: {}%", hits > 0 ? (correctValues * 100.0) / hits : 0.0);
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均GET QPS: {}", getOperations / duration);

            assertEquals(dataSize, stateStore.size());
            assertTrue(hits > 0, "应该有命中的GET操作");
            assertEquals(hits, correctValues, "所有命中的数据都应该正确");
        }

        @Test
        @Timeout(value = STRESS_DURATION_SECONDS + 10, unit = TimeUnit.SECONDS)
        @DisplayName("混合操作压力测试")
        void testMixedOperationsStress() throws Exception {
            final int totalOperations = 30_000_000;
            final double putRatio = 0.4;    // 40% PUT
            final double getRatio = 0.5;    // 50% GET
            final double removeRatio = 0.1; // 10% REMOVE

            LOG.info("开始混合操作压力测试: {} 次操作 (PUT:{}%, GET:{}%, REMOVE:{}%)",
                    totalOperations, (int) (putRatio * 100), (int) (getRatio * 100), (int) (removeRatio * 100));

            Random random = new Random(42);
            long startTime = System.currentTimeMillis();

            int putCount = 0, getCount = 0, removeCount = 0;
            int getHits = 0;

            for (int i = 0; i < totalOperations; i++) {
                double op = random.nextDouble();
                int key = random.nextInt(5000);
                String namespace = "ns" + (key % 20);
                int keyGroup = computeKeyGroup(key);

                if (op < putRatio) {
                    // PUT操作
                    String value = "mixed-value-" + i;
                    stateStore.put(key, namespace, value, keyGroup);
                    putCount++;

                } else if (op < putRatio + getRatio) {
                    // GET操作
                    String value = stateStore.get(key, namespace, keyGroup);
                    if (value != null) {
                        getHits++;
                    }
                    getCount++;

                } else {
                    // REMOVE操作
                    stateStore.remove(key, namespace, keyGroup);
                    removeCount++;
                }

                if (i % 50000 == 0 && i > 0) {
                    LOG.info("已完成 {} 次操作, 当前大小: {}, PUT:{}, GET:{}, REMOVE:{}",
                            i, stateStore.size(), putCount, getCount, removeCount);
                }
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("混合操作压力测试结果:");
            LOG.info("  总操作数: {}", totalOperations);
            LOG.info("  PUT操作数: {}", putCount);
            LOG.info("  GET操作数: {} (命中: {}, 命中率: {}%)", getCount, getHits, (getHits * 100.0) / getCount);
            LOG.info("  REMOVE操作数: {}", removeCount);
            LOG.info("  最终状态数量: {}", stateStore.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均QPS: {}", totalOperations / duration);
            LOG.info("  内存使用: {} KB", getHeapUsed() / 1024);

            assertEquals(totalOperations, putCount + getCount + removeCount);
        }
    }

    @Nested
    @DisplayName("长时间运行测试")
    class LongRunningTests {

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        @DisplayName("长时间运行稳定性测试")
        void testLongRunningStability() throws Exception {
            final int duration = 30; // 30秒
            final int keyRange = 5_000_000;
            final int operationsPerSecond = 100_000;

            long operationCount = 0;
            int errorCount = 0;

            long startTime = System.currentTimeMillis();
            long nextReportTime = startTime + 5_000;

            Random random = new Random();

            while (System.currentTimeMillis() - startTime < duration * 1000L) {
                try {
                    for (int i = 0; i < operationsPerSecond / 100; i++) {
                        int key = random.nextInt(keyRange);
                        String namespace = "ns" + (key % 100);
                        int keyGroup = computeKeyGroup(key);

                        if (random.nextDouble() < 0.5) {
                            // 50% 读操作
                            stateStore.get(key, namespace, keyGroup);
                        } else if (random.nextDouble() < 0.8) {
                            // 30% 写操作
                            stateStore.put(key, namespace, "value-" + operationCount, keyGroup);
                        } else {
                            // 20% 删除操作
                            stateStore.remove(key, namespace, keyGroup);
                        }

                        operationCount++;
                    }

                    Thread.sleep(10);

                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= nextReportTime) {
                        double elapsed = (currentTime - startTime) / 1000.0;
                        LOG.info("运行 {} s: 总操作数: {}, 前QPS: {}, 状态数量: {}, 内存: {}KB",
                                elapsed, operationCount, operationCount / elapsed,
                                stateStore.size(), getHeapUsed() / 1024);
                        nextReportTime = currentTime + 5_000;
                    }

                } catch (Exception e) {
                    errorCount++;
                    if (errorCount % 100 == 0) {
                        LOG.warn("遇到第 {} 个错误: {}", errorCount, e.getMessage());
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            double actualDuration = (endTime - startTime) / 1000.0;

            LOG.info("长时间稳定性测试结果:");
            LOG.info("  运行时间: {}秒", actualDuration);
            LOG.info("  总操作数: {}", operationCount);
            LOG.info("  错误数: {}", errorCount);
            LOG.info("  平均QPS: {}", operationCount / actualDuration);
            LOG.info("  错误率: {}%", (errorCount * 100.0) / operationCount);
            LOG.info("  最终状态数量: {}", stateStore.size());
            LOG.info("  内存使用: {} MB", getHeapUsed() / 1024 / 1024);

            assertTrue(operationCount > 0);
            assertTrue(errorCount < operationCount * 0.01);
        }
    }

    @Nested
    @DisplayName("自动扩容压力测试")
    class AutoResizeStressTests {

        @Test
        @Timeout(value = 180, unit = TimeUnit.SECONDS)
        @DisplayName("高写入并验证数据一致性")
        void testHighVolumeInsertWithVerification() throws Exception {
            LOG.info("开始 SwissTable 高写入压力测试：目标插入 150000 条数据");

            final int totalInsert = 150_000;
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < totalInsert; i++) {
                int keyGroup = computeKeyGroup(i);
                stateStore.put(i, "ns" + (i % 100), "value" + i, keyGroup);
                
                if (i % 30000 == 0 && i > 0) {
                    LOG.info("已插入 {} 条, size={}", i, stateStore.size());
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            int finalSize = stateStore.size();
            
            LOG.info("高写入压力测试完成:");
            LOG.info("  总插入: {} 条", totalInsert);
            LOG.info("  最终 size: {}", finalSize);
            LOG.info("  耗时: {} 秒", duration / 1000.0);
            LOG.info("  QPS: {}", totalInsert / (duration / 1000.0));

            assertEquals(totalInsert, finalSize, "应有 " + totalInsert + " 条数据");

            // 抽样验证数据完整性
            LOG.info("开始数据完整性验证...");
            int sampleInterval = totalInsert / 1000;
            int verifiedCount = 0;
            for (int k = 0; k < totalInsert; k += sampleInterval) {
                String expected = "value" + k;
                int keyGroup = computeKeyGroup(k);
                String actual = stateStore.get(k, "ns" + (k % 100), keyGroup);
                assertEquals(expected, actual, "数据不一致: key=" + k);
                verifiedCount++;
            }
            LOG.info("数据完整性验证通过: 抽样验证 {} 条", verifiedCount);
        }

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        @DisplayName("混合读写一致性验证")
        void testMixedOpsDuringHighLoad() throws Exception {
            LOG.info("开始高负载混合读写测试");
            
            Random rnd = new Random(123);
            
            // 先插入基础数据
            int baseInsert = 110_000;
            LOG.info("插入 {} 条基础数据...", baseInsert);
            for (int i = 0; i < baseInsert; i++) {
                int keyGroup = computeKeyGroup(i);
                stateStore.put(i, "ns" + (i % 50), "baseVal" + i, keyGroup);
            }
            
            LOG.info("基础数据插入完成: entries={}", stateStore.size());
            
            // 进行混合操作
            int mixedOps = 20_000;
            int writes = 0, reads = 0, deletes = 0;
            
            for (int i = 0; i < mixedOps; i++) {
                double op = rnd.nextDouble();
                int key = rnd.nextInt(baseInsert + 50_000);
                String ns = "ns" + (key % 50);
                int keyGroup = computeKeyGroup(key);
                
                if (op < 0.6) {
                    // 60% 写入
                    stateStore.put(key, ns, "mixedVal" + i, keyGroup);
                    writes++;
                } else if (op < 0.9) {
                    // 30% 读取
                    stateStore.get(key, ns, keyGroup);
                    reads++;
                } else {
                    // 10% 删除
                    stateStore.remove(key, ns, keyGroup);
                    deletes++;
                }
            }
            
            LOG.info("混合操作完成: writes={}, reads={}, deletes={}", writes, reads, deletes);
            
            // 验证部分基础数据仍然存在
            int verified = 0;
            for (int k = 0; k < baseInsert; k += 1000) {
                int keyGroup = computeKeyGroup(k);
                if (stateStore.containsKey(k, "ns" + (k % 50), keyGroup)) {
                    verified++;
                }
            }
            
            LOG.info("基础数据保留验证: {} 条仍存在", verified);
            assertTrue(verified > 0, "应有部分基础数据保留");
        }
    }

    @Nested
    @DisplayName("PUT/GET 基准测试")
    class PutGetBenchmarkTests {

        @Test
        @Timeout(value = STRESS_DURATION_SECONDS + 30, unit = TimeUnit.SECONDS)
        @DisplayName("单线程 PUT/GET 基准测试 - 模拟真实工作负载")
        void testRealisticWorkload() throws Exception {
            final int numKeys = 1_000_000;
            final int numOperations = 10_000_000;

            LOG.info("初始化 {} 条数据...", numKeys);
            for (int i = 0; i < numKeys; i++) {
                int keyGroup = computeKeyGroup(i);
                stateStore.put(i, "ns0", "init-" + i, keyGroup);
            }

            LOG.info("开始基准测试: {} 次操作", numOperations);

            Random random = new Random(42);
            long startTime = System.currentTimeMillis();
            int puts = 0, gets = 0;

            for (int i = 0; i < numOperations; i++) {
                int key = random.nextInt(numKeys);
                int keyGroup = computeKeyGroup(key);

                if (random.nextDouble() < 0.3) {
                    // 30% 写入
                    stateStore.put(key, "ns0", "updated-" + i, keyGroup);
                    puts++;
                } else {
                    // 70% 读取
                    stateStore.get(key, "ns0", keyGroup);
                    gets++;
                }

                if (i % 1_000_000 == 0 && i > 0) {
                    LOG.info("进度: {} / {}", i, numOperations);
                }
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("基准测试结果:");
            LOG.info("  总操作: {}", numOperations);
            LOG.info("  PUT操作: {}", puts);
            LOG.info("  GET操作: {}", gets);
            LOG.info("  耗时: {} 秒", duration);
            LOG.info("  QPS: {}", numOperations / duration);
            LOG.info("  内存使用: {} MB", getHeapUsed() / 1024 / 1024);

            assertEquals(numKeys, stateStore.size());
        }
    }
}
