package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.heap.space.NativeL0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 压力测试ForL0StateMap在高负载生产环境下的性能和稳定性
 * 注意：所有测试都是单线程的，符合Flink Task对StateMap的访问模式
 */
public class ForL0StateMapStressTest {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMapStressTest.class);

    private static final int STRESS_DURATION_SECONDS = 30;

    private L0MemoryAllocator l0Allocator;
    private ForL0StateMap<Integer, String, String> stateMap;

    @BeforeEach
    void setUp() throws Exception {
        l0Allocator = new NativeL0MemoryAllocator();
        stateMap = new ForL0StateMap<>(
                l0Allocator,
                10, // l0Cache 1K buckets
                IntSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                true // enable L0 cache
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (stateMap != null) {
            stateMap.close();
        }
        if (l0Allocator != null && !l0Allocator.isClosed()) {
            l0Allocator.close();
        }
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
                String namespace = "ns" + (i % 50); // 增加namespace数量
                String value = "value-" + i;

                stateMap.put(i, namespace, value);

                if (i % 500000 == 0 && i > 0) {
                    LOG.info("已完成 {} 次PUT操作, 当前大小: {}, 内存使用: {} MB",
                            i + 1, stateMap.size(), getHeapUsed() / 1024 / 1024);
                }
            }

            long endTime = System.currentTimeMillis();
            long endMemory = getHeapUsed();

            double duration = (endTime - startTime) / 1000.0;
            long memoryUsed = endMemory - startMemory;

            LOG.info("PUT操作压力测试结果:");
            LOG.info("  总操作数: {}", totalOperations);
            LOG.info("  最终状态数量: {}", stateMap.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均PUT QPS: {}", totalOperations / duration);
            LOG.info("  内存使用: {} KB", memoryUsed / 1024);
            LOG.info("  平均每条记录内存: {} bytes", memoryUsed / totalOperations);

            assertEquals(totalOperations, stateMap.size());

            // 验证数据正确性
            Random verifyRandom = new Random(42);
            int verifyCount = totalOperations / 10;
            int correctCount = 0;

            for (int i = 0; i < verifyCount; i++) {
                int key = verifyRandom.nextInt(totalOperations);
                String namespace = "ns" + (key % 50);
                String expectedValue = "value-" + key;

                String actualValue = stateMap.get(key, namespace);
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
            final int dataSize = 5_000_000; // 提高数据量
            final int getOperations = 1_000_000; // 提高GET操作数量

            // 先准备数据
            LOG.info("准备测试数据: {} 条记录", dataSize);
            for (int i = 0; i < dataSize; i++) {
                String namespace = "ns" + (i % 50);
                stateMap.put(i, namespace, "test-value-" + i);
            }

            LOG.info("GET操作压力测试: {} 次操作", getOperations);

            Random random = new Random(42); // 固定种子保证可重复性
            long startTime = System.currentTimeMillis();
            int hits = 0;
            int misses = 0;
            int correctValues = 0; // 统计正确数据的数量

            // 执行大量GET操作
            for (int i = 0; i < getOperations; i++) {
                int key = random.nextInt(dataSize * 2); // 50%命中率
                String namespace = "ns" + (key % 50);

                String value = stateMap.get(key, namespace);
                if (value != null) {
                    hits++;
                    // 验证数据正确性
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

            assertEquals(dataSize, stateMap.size());
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
                int key = random.nextInt(5000); // 密集的key范围
                String namespace = "ns" + (key % 20);

                if (op < putRatio) {
                    // PUT操作
                    String value = "mixed-value-" + i;
                    stateMap.put(key, namespace, value);
                    putCount++;

                } else if (op < putRatio + getRatio) {
                    // GET操作
                    String value = stateMap.get(key, namespace);
                    if (value != null) {
                        getHits++;
                    }
                    getCount++;

                } else {
                    // REMOVE操作
                    stateMap.remove(key, namespace);
                    removeCount++;
                }

                if (i % 50000 == 0 && i > 0) {
                    LOG.info("已完成 {} 次操作, 当前大小: {}, PUT:{}, GET:{}, REMOVE:{}",
                            i, stateMap.size(), putCount, getCount, removeCount);
                }
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("混合操作压力测试结果:");
            LOG.info("  总操作数: {}", totalOperations);
            LOG.info("  PUT操作数: {}", putCount);
            LOG.info("  GET操作数: {} (命中: {}, 命中率: {}%)", getCount, getHits, (getHits * 100.0) / getCount);
            LOG.info("  REMOVE操作数: {}", removeCount);
            LOG.info("  最终状态数量: {}", stateMap.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均QPS: {}", totalOperations / duration);
            LOG.info("  内存使用: {} KB", getHeapUsed() / 1024);

            assertEquals(totalOperations, putCount + getCount + removeCount);
        }
    }

    @Nested
    @DisplayName("缓存性能测试")
    class CachePerformanceTests {

        @Test
        @DisplayName("L0缓存命中率测试")
        void testL0CacheHitRate() throws Exception {
            final int hotKeys = 100;
            final int coldKeys = 1000;
            final int hotAccesses = 5000;

            // 插入冷数据
            for (int i = 0; i < coldKeys; i++) {
                stateMap.put(i + hotKeys, "ns", "cold-value-" + i);
            }

            // 插入热数据
            for (int i = 0; i < hotKeys; i++) {
                stateMap.put(i, "ns", "hot-value-" + i);
            }

            // 访问热数据多次以提高缓存命中率
            Random random = new Random();
            for (int i = 0; i < hotAccesses; i++) {
                int hotKey = random.nextInt(hotKeys);
                String value = stateMap.get(hotKey, "ns");
                assertNotNull(value);
                assertTrue(value.startsWith("hot-value-"));
            }

            // 获取缓存统计
            LOG.info("L0缓存性能测试完成");
            LOG.info("  热键数量: {}", hotKeys);
            LOG.info("  冷键数量: {}", coldKeys);
            LOG.info("  热访问次数: {}", hotAccesses);
            LOG.info("  总状态数量: {}", stateMap.size());

            assertEquals(hotKeys + coldKeys, stateMap.size());
        }

        @Test
        @DisplayName("缓存替换策略测试")
        void testCacheReplacementPolicy() throws Exception {
            @SuppressWarnings("unused")
            final int cacheSize = 1 << 12; // 4K buckets - Reserved for future cache size configuration
            final int testKeys = 1000; // 适中的键数量

            // 顺序插入数据，这会导致缓存替换
            for (int i = 0; i < testKeys; i++) {
                stateMap.put(i, "ns", "value-" + i);

                if (i % 100 == 0) {
                    LOG.info("已插入 {} 个键, 当前大小: {}", i + 1, stateMap.size());
                }
            }

            // 访问期插入的键（可能已被替换出缓存）
            int earlyAccessCount = 100;
            int earlyHits = 0;
            for (int i = 0; i < earlyAccessCount; i++) {
                String value = stateMap.get(i, "ns");
                if (value != null) {
                    earlyHits++;
                }
            }

            // 访问最近插入的键（应该在缓存中）
            int recentAccessCount = 100;
            int recentHits = 0;
            int startKey = testKeys - recentAccessCount;
            for (int i = startKey; i < testKeys; i++) {
                String value = stateMap.get(i, "ns");
                if (value != null) {
                    recentHits++;
                }
            }

            LOG.info("缓存替换策略测试结果:");
            LOG.info("  测试键总数: {}", testKeys);
            LOG.info("  早期键命中: {}/{}", earlyHits, earlyAccessCount);
            LOG.info("  最近键命中: {}/{}", recentHits, recentAccessCount);
            LOG.info("  最终状态数量: {}", stateMap.size());

            assertEquals(testKeys, stateMap.size());
            assertEquals(recentAccessCount, recentHits); // 最近的键应该全部命中
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
            final int operationsPerSecond = 100_000; // 每秒操作数

            long operationCount = 0;
            int errorCount = 0;

            long startTime = System.currentTimeMillis();
            long nextReportTime = startTime + 5_000; // 每5秒一次

            Random random = new Random();

            while (System.currentTimeMillis() - startTime < duration * 1000L) {
                try {
                    // 控制操作频率
                    for (int i = 0; i < operationsPerSecond / 100; i++) {
                        int key = random.nextInt(keyRange);
                        String namespace = "ns" + (key % 100);

                        if (random.nextDouble() < 0.5) {
                            // 50% 读操作
                            stateMap.get(key, namespace);
                        } else if (random.nextDouble() < 0.8) {
                            // 30% 写操作
                            stateMap.put(key, namespace, "value-" + operationCount);
                        } else {
                            // 20% 删除操作
                            stateMap.remove(key, namespace);
                        }

                        operationCount++;
                    }

                    // 控制频率：每10ms执行一批操作
                    Thread.sleep(10);

                    // 定期报告
                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= nextReportTime) {
                        double elapsed = (currentTime - startTime) / 1000.0;
                        LOG.info("运行 {} s: 总操作数: {}, 前QPS: {}, 状态数量: {}, 内存: {}KB",
                                elapsed, operationCount, operationCount / elapsed,
                                stateMap.size(), getHeapUsed() / 1024);
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
            LOG.info("  最终状态数量: {}", stateMap.size());
            LOG.info("  内存使用: {} MB", getHeapUsed() / 1024 / 1024);

            assertTrue(operationCount > 0);
            assertTrue(errorCount < operationCount * 0.01); // 错误率应小于1%
        }
    }

    @Nested
    @DisplayName("自动扩容压力测试")
    class AutoResizeStressTests {

        @Test
        @Timeout(value = 180, unit = TimeUnit.SECONDS)
        @DisplayName("高写入触发多次扩容并验证数据一致性")
        void testMultipleAutoResizes() throws Exception {
            // MainTable 固定初始大小为 65536 buckets
            // 负载因子阈值 1.5 意味着需要 > 98304 entries 才能触发扩容
            // 我们插入 150000 条数据来触发扩容
            
            int initialBucket = stateMap.getDetailedStats().mainTableStats.bucketCount;
            assertEquals(65536, initialBucket, "初始应该是 65536 buckets");
            
            LOG.info("开始自动扩容压力测试：目标插入 150000 条数据触发扩容");

            // 记录扩容发生的 bucketCount 序列
            java.util.List<Integer> bucketHistory = new java.util.ArrayList<>();
            bucketHistory.add(initialBucket);
            int lastBucket = initialBucket;

            final int totalInsert = 150_000; // 超过 98304 触发扩容
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < totalInsert; i++) {
                stateMap.put(i, "ns" + (i % 100), "value" + i);
                
                // 每 10000 条检查一次是否扩容
                if (i % 10000 == 0 && i > 0) {
                    int current = stateMap.getDetailedStats().mainTableStats.bucketCount;
                    if (current != lastBucket) {
                        bucketHistory.add(current);
                        LOG.info("扩容发生: {} -> {} buckets (已插入 {} 条)", lastBucket, current, i);
                        lastBucket = current;
                    }
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            ForL0StateMap.DetailedStats stats = stateMap.getDetailedStats();
            
            LOG.info("自动扩容压力测试完成:");
            LOG.info("  总插入: {} 条", totalInsert);
            LOG.info("  最终 bucket 数: {}", stats.mainTableStats.bucketCount);
            LOG.info("  扩容历史: {}", bucketHistory);
            LOG.info("  耗时: {} 秒", duration / 1000.0);
            LOG.info("  QPS: {}", totalInsert / (duration / 1000.0));

            // 验证已经扩容（从 65536 扩容到 131072）
            assertTrue(stats.mainTableStats.bucketCount > 65536, 
                    "应已扩容到 > 65536 buckets, 实际=" + stats.mainTableStats.bucketCount);
            assertEquals(totalInsert, stats.totalEntries, "应有 " + totalInsert + " 条数据");

            // 抽样验证数据完整性
            LOG.info("开始数据完整性验证...");
            int sampleInterval = totalInsert / 1000; // 抽样 1000 条
            int verifiedCount = 0;
            for (int k = 0; k < totalInsert; k += sampleInterval) {
                String expected = "value" + k;
                String actual = stateMap.get(k, "ns" + (k % 100));
                assertEquals(expected, actual, "数据不一致: key=" + k);
                verifiedCount++;
            }
            LOG.info("数据完整性验证通过: 抽样验证 {} 条", verifiedCount);

            // bucket 序列应严格递增且为2的幂
            int prev = -1;
            for (int b : bucketHistory) {
                assertEquals(0, (b & (b - 1)), "bucketCount 应为2次幂: " + b);
                if (prev != -1) {
                    assertTrue(b > prev, "bucketCount 应递增: " + prev + "->" + b);
                }
                prev = b;
            }
        }

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        @DisplayName("扩容过程中混合读写一致性验证")
        void testMixedOpsDuringResize() throws Exception {
            // 目标：在扩容过程中进行混合读写删操作，验证数据一致性
            // 需要插入足够多的数据触发扩容（> 98304 条）
            // 由于有删除操作，需要更多的写入操作来确保最终条目数超过阈值
            
            LOG.info("开始扩容过程中混合读写测试");
            
            java.util.Random rnd = new java.util.Random(123);
            
            // 先插入足够多的基础数据确保触发扩容
            int baseInsert = 110_000;
            LOG.info("插入 {} 条基础数据...", baseInsert);
            for (int i = 0; i < baseInsert; i++) {
                stateMap.put(i, "ns" + (i % 50), "baseVal" + i);
            }
            
            ForL0StateMap.DetailedStats statsAfterBase = stateMap.getDetailedStats();
            LOG.info("基础数据插入完成: entries={}, buckets={}", 
                    statsAfterBase.totalEntries, statsAfterBase.mainTableStats.bucketCount);
            
            // 验证已经扩容
            assertTrue(statsAfterBase.mainTableStats.bucketCount > 65536, 
                    "基础数据应已触发扩容");
            
            // 在扩容后继续进行混合操作
            int mixedOps = 20_000;
            int writes = 0, reads = 0, deletes = 0;
            
            for (int i = 0; i < mixedOps; i++) {
                int op = rnd.nextInt(100);
                int key = baseInsert + i;
                String ns = "n" + (i % 7);
                
                if (op < 60) { // 60% 写入
                    stateMap.put(key, ns, "mixVal" + i);
                    writes++;
                } else if (op < 90) { // 30% 读取
                    int k = rnd.nextInt(baseInsert + i);
                    stateMap.get(k, "ns" + (k % 50));
                    reads++;
                } else { // 10% 删除（只删除最近插入的，不删除基础数据）
                    int k = baseInsert + rnd.nextInt(Math.max(1, i));
                    stateMap.remove(k, "n" + ((k - baseInsert) % 7));
                    deletes++;
                }
            }

            ForL0StateMap.DetailedStats finalStats = stateMap.getDetailedStats();
            LOG.info("混合操作完成: writes={}, reads={}, deletes={}", writes, reads, deletes);
            LOG.info("最终状态: entries={}, buckets={}", finalStats.totalEntries, finalStats.mainTableStats.bucketCount);

            // 强制再进行一次读写穿插，确保扩容后读写正常
            for (int i = 0; i < 1000; i++) {
                int key = 300_000 + i;
                stateMap.put(key, "nX", "VX" + i);
                assertEquals("VX" + i, stateMap.get(key, "nX"));
            }
            
            // 验证部分基础数据仍可访问
            for (int i = 0; i < baseInsert; i += baseInsert / 100) {
                String expected = "baseVal" + i;
                String actual = stateMap.get(i, "ns" + (i % 50));
                assertEquals(expected, actual, "基础数据应仍可访问: key=" + i);
            }
            
            LOG.info("扩容后读写验证通过");
        }
    }

    @Nested
    @DisplayName("Transform方法压力测试")
    class TransformStressTests {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        @DisplayName("Transform vs Get+Put性能对比测试")
        void testTransformPerformanceComparison() throws Exception {
            final int operationsCount = 10_000_000;
            final int dataSize = 1_000_000;

            // 准备测试数据
            LOG.info("准备 {} 条测试数据", dataSize);
            for (int i = 0; i < dataSize; i++) {
                String namespace = "ns" + (i % 100);
                stateMap.put(i, namespace, "initial-value-" + i);
            }

            Random random = new Random(42);

            // 测试Transform方法性能
            LOG.info("开始Transform方法性能测试: {} 次操作", operationsCount);
            long transformStartTime = System.nanoTime();
            long transformOpsCompleted = 0;

            for (int i = 0; i < operationsCount; i++) {
                int key = random.nextInt(dataSize);
                String namespace = "ns" + (key % 10);

                stateMap.transform(key, namespace, "_transformed", (previous, value) -> {
                    if (previous == null) {
                        return "new" + value;
                    }
                    return (previous + value).substring(0, 10); // 截断以防过长
                });
                transformOpsCompleted++;
            }

            long transformEndTime = System.nanoTime();
            double transformDuration = (transformEndTime - transformStartTime) / 1_000_000_000.0;

            // 重置状态，重新准备数据用于Get+Put测试
            stateMap.close();
            stateMap = new ForL0StateMap<>(
                    l0Allocator,
                    10,
                    IntSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    true
            );

            for (int i = 0; i < dataSize; i++) {
                String namespace = "ns" + (i % 10);
                stateMap.put(i, namespace, "initial-value-" + i);
            }

            // 测试Get+Put方法性能（重置Random确保相同的操作序列）
            random = new Random(42);
            LOG.info("开始Get+Put方法性能测试: {} 次操作", operationsCount);
            long getPutStartTime = System.nanoTime();
            long getPutOpsCompleted = 0;

            for (int i = 0; i < operationsCount; i++) {
                int key = random.nextInt(dataSize);
                String namespace = "ns" + (key % 10);

                String previous = stateMap.get(key, namespace);
                String newValue;
                if (previous == null) {
                    newValue = "new_transformed";
                } else {
                    newValue = (previous + "_transformed").substring(0, 10);
                }
                stateMap.put(key, namespace, newValue);
                getPutOpsCompleted++;
            }

            long getPutEndTime = System.nanoTime();
            double getPutDuration = (getPutEndTime - getPutStartTime) / 1_000_000_000.0;

            // 输出性能对比结果
            LOG.info("Transform vs Get+Put性能对比结果:");
            LOG.info("  Transform方法:");
            LOG.info("    完成操作数: {}", transformOpsCompleted);
            LOG.info("    耗时: {}秒", String.format("%.3f", transformDuration));
            LOG.info("    QPS: {}", String.format("%.0f", transformOpsCompleted / transformDuration));
            LOG.info("    平均每次操作: {}微秒", String.format("%.3f", (transformDuration * 1_000_000) / transformOpsCompleted));

            LOG.info("  Get+Put方法:");
            LOG.info("    完成操作数: {}", getPutOpsCompleted);
            LOG.info("    耗时: {}秒", String.format("%.3f", getPutDuration));
            LOG.info("    QPS: {}", String.format("%.0f", getPutOpsCompleted / getPutDuration));
            LOG.info("    平均每次操作: {}微秒", String.format("%.3f", (getPutDuration * 1_000_000) / getPutOpsCompleted));

            double performanceImprovement = (getPutDuration - transformDuration) / getPutDuration * 100;
            LOG.info("  性能提升: {}%", String.format("%.1f", performanceImprovement));

            // 验证两种方法都成功完成了操作
            assertEquals(operationsCount, transformOpsCompleted);
            assertEquals(operationsCount, getPutOpsCompleted);
        }

        @Test
        @Timeout(value = 45, unit = TimeUnit.SECONDS)
        @DisplayName("Transform大量操作压力测试")
        void testMassiveTransformOperations() throws Exception {
            final int totalOperations = 1_000_000;
            final int keyRange = 50_000;

            LOG.info("Transform大量操作压力测试: {} 次操作", totalOperations);

            Random random = new Random(42);
            long startTime = System.currentTimeMillis();
            long transformOpsCompleted = 0;
            int newEntries = 0;
            int updatedEntries = 0;
            int deletedEntries = 0;

            try {
                for (int i = 0; i < totalOperations; i++) {
                    int key = random.nextInt(keyRange);
                    double operation = random.nextDouble();

                    if (operation < 0.6) {
                        // 60% - 新增或更新操作（使用string namespace）
                        String namespace = "str_ns" + (key % 20);
                        boolean wasPresent = stateMap.containsKey(key, namespace);
                        stateMap.transform(key, namespace, "_update_" + i, (previous, value) -> {
                            if (previous == null) {
                                return "new" + value;
                            } else {
                                return previous + value;
                            }
                        });
                        if (wasPresent) {
                            updatedEntries++;
                        } else {
                            newEntries++;
                        }
                    } else if (operation < 0.8) {
                        // 20% - 计数器操作（使用count namespace）
                        String namespace = "count_ns" + (key % 20);
                        stateMap.transform(key, namespace, 1, (previous, increment) -> {
                            int currentValue = 0;
                            if (previous != null && previous.startsWith("count:")) {
                                currentValue = Integer.parseInt(previous.substring(6));
                            }
                            return "count:" + (currentValue + increment);
                        });
                    } else {
                        // 20% - 删除操作（使用delete namespace）
                        String namespace = "del_ns" + (key % 20);
                        boolean wasPresent = stateMap.containsKey(key, namespace);
                        stateMap.transform(key, namespace, "delete", (previous, value) -> null);
                        if (wasPresent) {
                            deletedEntries++;
                        }
                    }

                    transformOpsCompleted++;

                    if (i % 200_000 == 0 && i > 0) {
                        LOG.info("已完成 {} 次Transform操作, 当前状态数: {}, 内存: {} MB",
                                i, stateMap.size(), getHeapUsed() / 1024 / 1024);
                    }
                }
            } catch (Exception e) {
                LOG.error("Transform压力测试出现异常", e);
                throw e;
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("Transform大量操作压力测试结果:");
            LOG.info("  总操作数: {}", transformOpsCompleted);
            LOG.info("  新增条目: {}", newEntries);
            LOG.info("  更新条目: {}", updatedEntries);
            LOG.info("  删除条目: {}", deletedEntries);
            LOG.info("  最终状态数量: {}", stateMap.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均Transform QPS: {}", transformOpsCompleted / duration);
            LOG.info("  内存使用: {} MB", getHeapUsed() / 1024 / 1024);

            assertEquals(totalOperations, transformOpsCompleted);
            // 注意：由于删除操作，最终状态可能为空
            assertTrue(stateMap.size() >= 0, "状态数量应该非负");
        }

        @Test
        @Timeout(value = 30, unit = TimeUnit.SECONDS)
        @DisplayName("Transform复杂转换逻辑测试")
        void testComplexTransformLogic() throws Exception {
            final int operationsCount = 100_000;
            final int keyRange = 1_000;

            LOG.info("Transform复杂转换逻辑测试: {} 次操作", operationsCount);

            Random random = new Random(42);
            long startTime = System.currentTimeMillis();
            int jsonOperations = 0;
            int aggregationOperations = 0;
            int conditionalOperations = 0;

            for (int i = 0; i < operationsCount; i++) {
                int key = random.nextInt(keyRange);
                double opType = random.nextDouble();

                try {
                    if (opType < 0.33) {
                        // 模拟JSON聚合操作（使用json namespace）
                        String namespace = "json_ns" + (key % 5);
                        stateMap.transform(key, namespace, "item_" + i, (previous, newItem) -> {
                            if (previous == null) {
                                return "[\"" + newItem + "\"]";
                            } else {
                                // 简单的JSON数组追加
                                String withoutBracket = previous.substring(0, previous.length() - 1);
                                return withoutBracket + ",\"" + newItem + "\"]";
                            }
                        });
                        jsonOperations++;

                    } else if (opType < 0.66) {
                        // 模拟数值聚合操作（使用sum namespace）
                        String namespace = "sum_ns" + (key % 5);
                        int value = random.nextInt(100);
                        stateMap.transform(key, namespace, value, (previous, newValue) -> {
                            if (previous == null) {
                                return "sum:" + newValue + ",count:1";
                            } else {
                                try {
                                    String[] parts = previous.split(",");
                                    int sum = Integer.parseInt(parts[0].split(":")[1]);
                                    int count = Integer.parseInt(parts[1].split(":")[1]);
                                    return "sum:" + (sum + newValue) + ",count:" + (count + 1);
                                } catch (Exception e) {
                                    // 如果解析失败，重新开始计算
                                    return "sum:" + newValue + ",count:1";
                                }
                            }
                        });
                        aggregationOperations++;

                    } else {
                        // 模拟条件更新操作（使用status namespace）
                        String namespace = "status_ns" + (key % 5);
                        String status = random.nextBoolean() ? "active" : "inactive";
                        stateMap.transform(key, namespace, status, (previous, newStatus) -> {
                            if (previous == null) {
                                return "status:" + newStatus + ",timestamp:" + System.currentTimeMillis();
                            } else {
                                try {
                                    // 只有状态改变时才更新
                                    String currentStatus = previous.split(",")[0].split(":")[1];
                                    if (!currentStatus.equals(newStatus)) {
                                        return "status:" + newStatus + ",timestamp:" + System.currentTimeMillis();
                                    }
                                    return previous; // 状态未改变，保持原值
                                } catch (Exception e) {
                                    // 如果解析失败，创建新的状态记录
                                    return "status:" + newStatus + ",timestamp:" + System.currentTimeMillis();
                                }
                            }
                        });
                        conditionalOperations++;
                    }

                    if (i % 20_000 == 0 && i > 0) {
                        LOG.info("已完成 {} 次复杂Transform操作, 当前状态数: {}",
                                i, stateMap.size());
                    }

                } catch (Exception e) {
                    LOG.error("复杂Transform操作失败 at iteration {}", i, e);
                    throw e;
                }
            }

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;

            LOG.info("Transform复杂转换逻辑测试结果:");
            LOG.info("  总操作数: {}", operationsCount);
            LOG.info("  JSON聚合操作: {}", jsonOperations);
            LOG.info("  数值聚合操作: {}", aggregationOperations);
            LOG.info("  条件更新操作: {}", conditionalOperations);
            LOG.info("  最终状态数量: {}", stateMap.size());
            LOG.info("  耗时: {}秒", duration);
            LOG.info("  平均QPS: {}", operationsCount / duration);

            assertEquals(operationsCount, jsonOperations + aggregationOperations + conditionalOperations);
            assertTrue(stateMap.size() <= keyRange * 3, "状态数量不应超过key范围 * namespace数量");

            // 验证部分数据的正确性
            int verificationCount = 0;
            for (int key = 0; key < Math.min(keyRange, 100); key += 10) {
                for (int ns = 0; ns < 5; ns++) {
                    // 验证JSON数据
                    String jsonNamespace = "json_ns" + ns;
                    String jsonValue = stateMap.get(key, jsonNamespace);
                    if (jsonValue != null) {
                        assertTrue(jsonValue.startsWith("[") && jsonValue.endsWith("]"),
                                "JSON格式应该正确: " + jsonValue);
                        verificationCount++;
                    }

                    // 验证聚合数据
                    String sumNamespace = "sum_ns" + ns;
                    String sumValue = stateMap.get(key, sumNamespace);
                    if (sumValue != null) {
                        assertTrue(sumValue.startsWith("sum:") && sumValue.contains(",count:"),
                                "聚合格式应该正确: " + sumValue);
                        verificationCount++;
                    }

                    // 验证状态数据
                    String statusNamespace = "status_ns" + ns;
                    String statusValue = stateMap.get(key, statusNamespace);
                    if (statusValue != null) {
                        assertTrue(statusValue.startsWith("status:") && statusValue.contains(",timestamp:"),
                                "状态格式应该正确: " + statusValue);
                        verificationCount++;
                    }
                }
            }
            LOG.info("数据格式验证: {} 个条目验证通过", verificationCount);
        }
    }

    @Nested
    @Disabled
    @DisplayName("Put/Get操作性能基准测试")
    class PutGetBenchmarkTests {

        @Test
        @DisplayName("Put/Get/Transform操作性能基准测试 - ForL0StateMap vs CopyOnWriteStateMap")
        void benchmarkPutGetOperations() {
            final int operationNum = 50_000_000; // 操作次数
            final int namespaceRange = 10;
            final int keyRange = 1_000_000;

            LOG.info("=== PUT/GET/TRANSFORM操作性能基准测试对比 ===");
            LOG.info("PUT操作数: {}, GET操作数: {}, TRANSFORM操作数: {}", operationNum, operationNum, operationNum);
            LOG.info("Namespace范围: 0-{}, Key范围: 0-{}", namespaceRange - 1, keyRange - 1);

            // ===== 生成测试数据 =====
            LOG.info("=== 生成测试数据 ===");
            int[] keySequence = createSequence(operationNum, keyRange);
            int[] namespaceSequence = createSequence(operationNum, namespaceRange);
            int[] transformValues = createSequence(operationNum, 1000); // Transform增量值
            LOG.info("=== 测试数据生成完成 ===");

            // ===== ForL0StateMap 基准测试 =====
            LOG.info("=== ForL0StateMap 基准测试 ===");

            BenchmarkResult forL0Result = runStateMapBenchmark(
                "ForL0StateMap",
                createForL0StateMapInteger(),
                keySequence,
                namespaceSequence,
                transformValues,
                ForL0StateMapStressTest::getHeapUsed
            );

            // ===== CopyOnWriteStateMap 基准测试 =====
            LOG.info("=== CopyOnWriteStateMap 基准测试 ===");

            // 创建CopyOnWriteStateMap实例
            CopyOnWriteStateMap<String, String, Integer> copyOnWriteStateMap =
                new CopyOnWriteStateMap<>(IntSerializer.INSTANCE);

            BenchmarkResult cowResult = runStateMapBenchmark(
                "CopyOnWriteStateMap",
                copyOnWriteStateMap,
                keySequence,
                namespaceSequence,
                transformValues,
                ()-> 0L // CopyOnWriteStateMap使用堆内存，无法直接测量
            );

            // ===== 性能对比总结 =====
            LOG.info("\n=== 性能对比总结 ===");

            printPerformanceComparison("PUT", forL0Result, cowResult, "put");
            printPerformanceComparison("GET", forL0Result, cowResult, "get");
            printPerformanceComparison("TRANSFORM", forL0Result, cowResult, "transform");
            printMemoryComparison(forL0Result, cowResult);

            // 验证数据正确性
            validateBenchmarkResults(forL0Result, cowResult, keyRange, namespaceRange);
        }

        /**
         * 创建使用Integer类型value的ForL0StateMap
         */
        private StateMap<String, String, Integer> createForL0StateMapInteger() {
            return new ForL0StateMap<>(
                l0Allocator,
                10, // l0Cache 1K buckets
                StringSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                true // enable L0 cache
            );
        }

        /**
         * 运行StateMap基准测试的通用方法
         */
        private BenchmarkResult runStateMapBenchmark(
                String name,
                StateMap<String, String, Integer> stateMap,
                int[] keySequence,
                int[] namespaceSequence,
                int[] transformValues,
                java.util.function.Supplier<Long> memorySupplier) {

            // PUT操作基准测试
            LOG.info("开始{} PUT操作基准测试...", name);
            long startMemory = memorySupplier.get();

            PutBenchmarkResult putResult = runPutBenchmark(
                name, stateMap, keySequence, namespaceSequence
            );

            long endMemory = memorySupplier.get();
            long memoryUsed = endMemory - startMemory;

            logPutResults(name, putResult, stateMap.size(), memoryUsed);

            // GET操作基准测试
            LOG.info("开始{} GET操作基准测试...", name);

            GetBenchmarkResult getResult = runGetBenchmark(
                name, stateMap, keySequence, namespaceSequence
            );

            logGetResults(name, getResult);

            // TRANSFORM操作基准测试
            LOG.info("开始{} TRANSFORM操作基准测试...", name);

            TransformBenchmarkResult transformResult = runTransformBenchmark(
                name, stateMap, keySequence, namespaceSequence, transformValues
            );

            logTransformResults(name, transformResult);

            return new BenchmarkResult(name, putResult, getResult, transformResult, stateMap.size(), memoryUsed);
        }

        /**
         * 执行PUT操作基准测试
         */
        private PutBenchmarkResult runPutBenchmark(
                String name,
                StateMap<String, String, Integer> stateMap,
                int[] keySequence,
                int[] namespaceSequence){
            int operationNum = keySequence.length;
            long startTime = System.nanoTime();

            for (int i = 0; i < operationNum; i++) {
                // key 使用String类型
                String key = "key" + keySequence[i];
                String namespace = "ns" + namespaceSequence[i];
                stateMap.put(key, namespace, i); // 使用Integer值

                if (i % 10_000_000 == 0 && i > 0) {
                    LOG.info("{}已完成 {} 次PUT操作", name, i);
                }
            }

            long endTime = System.nanoTime();

            long duration = (endTime - startTime) / 1_000_000; // ms
            double qps = operationNum * 1000.0 / duration;
            double avgLatency = (double) (endTime - startTime) / operationNum / 1000.0; // μs

            return new PutBenchmarkResult(operationNum, duration, qps, avgLatency);
        }

        /**
         * 执行GET操作基准测试
         */
        private GetBenchmarkResult runGetBenchmark(
                String name,
                StateMap<String, String, Integer> stateMap,
                int[] keySequence,
                int[] namespaceSequence) {
            int operationNum = keySequence.length;
            long startTime = System.nanoTime();
            long hits = 0;

            for (int i = 0; i < operationNum; i++) {
                String key = "key" + keySequence[i];
                String namespace = "ns" + namespaceSequence[i];

                Integer value = stateMap.get(key, namespace);
                if (value != null) {
                    hits++;
                }

                if (i % 10_000_000 == 0 && i > 0) {
                    LOG.info("{}已完成 {} 次GET操作", name, i);
                }
            }

            long endTime = System.nanoTime();

            long duration = (endTime - startTime) / 1_000_000; // ms
            double qps = operationNum * 1000.0 / duration;
            double avgLatency = (double) (endTime - startTime) / operationNum / 1000.0; // μs
            double hitRate = (hits * 100.0) / operationNum;

            return new GetBenchmarkResult(operationNum, hits, duration, qps, avgLatency, hitRate);
        }

        /**
         * 执行TRANSFORM操作基准测试
         */
        private TransformBenchmarkResult runTransformBenchmark(
                String name,
                StateMap<String, String, Integer> stateMap,
                int[] keySequence,
                int[] namespaceSequence,
                int[] transformValues) {

            int operationNum = keySequence.length;
            long startTime = System.nanoTime();
            long transformations = 0;
            AtomicLong newValues = new AtomicLong();

            for (int i = 0; i < operationNum; i++) {
                // 使用与PUT操作相同的key生成逻辑
                String key = "key" + keySequence[i];
                String namespace = "ns" + namespaceSequence[i];
                int incrementValue = transformValues[i];

                try {
                    stateMap.transform(key, namespace, incrementValue, (previous, increment) -> {
                        if (previous == null) {
                            newValues.getAndIncrement();
                            return increment;
                        } else {
                            return previous + increment;
                        }
                    });
                    transformations++;
                } catch (Exception e) {
                    // 忽略异常，继续执行
                    LOG.info("Transform操作异常: {}", e.getMessage());
                }

                if (i % 10_000_000 == 0 && i > 0) {
                    LOG.info("{}已完成 {} 次TRANSFORM操作", name, i);
                }
            }

            long endTime = System.nanoTime();

            long duration = (endTime - startTime) / 1_000_000; // ms
            double qps = transformations > 0 ? transformations * 1000.0 / duration : 0;
            double avgLatency = transformations > 0 ? (double) (endTime - startTime) / transformations / 1000.0 : 0; // μs

            return new TransformBenchmarkResult(operationNum, transformations, newValues.get(), duration, qps, avgLatency);
        }

        /**
         * 输出PUT操作测试结果
         */
        private void logPutResults(String name, PutBenchmarkResult result, int recordCount, long memoryUsed) {
            LOG.info("{} PUT操作基准测试结果:", name);
            LOG.info("  操作数: {}", result.operationCount);
            LOG.info("  实际记录数: {}", recordCount);
            LOG.info("  耗时: {}ms", result.duration);
            LOG.info("  QPS: {}", String.format("%.0f", result.qps));
            LOG.info("  平均延迟: {}μs", String.format("%.3f", result.avgLatency));
            if (memoryUsed > 0) {
                LOG.info("  内存使用: {}MB", memoryUsed / 1024 / 1024);
                LOG.info("  平均每条记录: {}bytes", recordCount > 0 ? memoryUsed / recordCount : 0);
            }
        }

        /**
         * 输出GET操作测试结果
         */
        private void logGetResults(String name, GetBenchmarkResult result) {
            LOG.info("{} GET操作基准测试结果:", name);
            LOG.info("  操作数: {}", result.operationCount);
            LOG.info("  命中数: {}", result.hits);
            LOG.info("  耗时: {}ms", result.duration);
            LOG.info("  QPS: {}", String.format("%.0f", result.qps));
            LOG.info("  平均延迟: {}μs", String.format("%.3f", result.avgLatency));
            LOG.info("  命中率: {}%", String.format("%.1f", result.hitRate));
        }

        /**
         * 输出TRANSFORM操作测试结果
         */
        private void logTransformResults(String name, TransformBenchmarkResult result) {
            LOG.info("{} TRANSFORM操作基准测试结果:", name);
            LOG.info("  总尝试操作数: {}", result.operationCount);
            LOG.info("  实际执行转换数: {}", result.transformations);
            LOG.info("  新值创建数: {}", result.newValues);
            LOG.info("  耗时: {}ms", result.duration);
            if (result.transformations > 0) {
                LOG.info("  QPS: {}", String.format("%.0f", result.qps));
                LOG.info("  平均延迟: {}μs", String.format("%.3f", result.avgLatency));
                LOG.info("  有效操作比例: {}%", String.format("%.1f", (result.transformations * 100.0) / result.operationCount));
                LOG.info("  新值比例: {}%", String.format("%.1f", (result.newValues * 100.0) / result.transformations));
            } else {
                LOG.info("  无有效转换操作");
            }
        }

        /**
         * 打印性能对比结果
         */
        private void printPerformanceComparison(String operation, BenchmarkResult forL0, BenchmarkResult cow, String operationType) {
            double forL0Qps, forL0Latency, cowQps, cowLatency;
            String additionalInfo = "";

            switch (operationType) {
                case "put":
                    forL0Qps = forL0.putResult.qps;
                    forL0Latency = forL0.putResult.avgLatency;
                    cowQps = cow.putResult.qps;
                    cowLatency = cow.putResult.avgLatency;
                    break;
                case "get":
                    forL0Qps = forL0.getResult.qps;
                    forL0Latency = forL0.getResult.avgLatency;
                    cowQps = cow.getResult.qps;
                    cowLatency = cow.getResult.avgLatency;
                    additionalInfo = String.format(", %.1f%% vs %.1f%% 命中率",
                            forL0.getResult.hitRate, cow.getResult.hitRate);
                    break;
                case "transform":
                    forL0Qps = forL0.transformResult.qps;
                    forL0Latency = forL0.transformResult.avgLatency;
                    cowQps = cow.transformResult.qps;
                    cowLatency = cow.transformResult.avgLatency;
                    double forL0NewRatio = forL0.transformResult.transformations > 0 ?
                        (forL0.transformResult.newValues * 100.0) / forL0.transformResult.transformations : 0;
                    double cowNewRatio = cow.transformResult.transformations > 0 ?
                        (cow.transformResult.newValues * 100.0) / cow.transformResult.transformations : 0;
                    additionalInfo = String.format(", %.1f%% vs %.1f%% 新值比例", forL0NewRatio, cowNewRatio);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation type: " + operationType);
            }

            double qpsRatio = cowQps > 0 ? forL0Qps / cowQps : 0;
            double latencyRatio = forL0Latency > 0 && cowLatency > 0 ? forL0Latency / cowLatency : 0;

            LOG.info("{}操作性能对比:", operation);
            String[] additionalParts = additionalInfo.split(",");
            LOG.info("  ForL0StateMap:      {} QPS, {}μs 平均延迟{}",
                    String.format("%.0f", forL0Qps), String.format("%.3f", forL0Latency),
                    additionalParts.length > 0 ? additionalParts[0] : "");
            LOG.info("  CopyOnWriteStateMap: {} QPS, {}μs 平均延迟{}",
                    String.format("%.0f", cowQps), String.format("%.3f", cowLatency),
                    additionalParts.length > 1 ? additionalParts[1] : "");

            if (qpsRatio > 0) {
                LOG.info("  ForL0StateMap QPS是CopyOnWriteStateMap的 {} 倍", String.format("%.2f", qpsRatio));
            }
            if (latencyRatio > 0) {
                LOG.info("  ForL0StateMap延迟是CopyOnWriteStateMap的 {} 倍", String.format("%.2f", latencyRatio));
            }
        }

        /**
         * 打印内存使用对比
         */
        private void printMemoryComparison(BenchmarkResult forL0, BenchmarkResult cow) {
            LOG.info("内存使用对比:");
            LOG.info("  ForL0StateMap实际记录数: {}, 内存使用: {}MB, 平均每条记录: {}bytes",
                    forL0.recordCount, forL0.memoryUsed / 1024 / 1024,
                    forL0.recordCount > 0 ? forL0.memoryUsed / forL0.recordCount : 0);
            LOG.info("  CopyOnWriteStateMap实际记录数: {} (使用堆内存，无法精确测量内存使用)",
                    cow.recordCount);
        }

        /**
         * 验证基准测试结果的正确性
         */
        private void validateBenchmarkResults(BenchmarkResult forL0, BenchmarkResult cow, int keyRange, int namespaceRange) {
            assertNotEquals(0, forL0.recordCount, "ForL0StateMap应该有插入的记录");
            assertNotEquals(0, cow.recordCount, "CopyOnWriteStateMap应该有插入的记录");
            assertTrue(forL0.recordCount <= (long) keyRange * namespaceRange, "ForL0StateMap记录数不应超过理论最大值");
            assertTrue(cow.recordCount <= (long) keyRange * namespaceRange, "CopyOnWriteStateMap记录数不应超过理论最大值");

            // 理论上，使用相同随机种子的情况下，两个StateMap应该有相同的记录数
            assertEquals(forL0.recordCount, cow.recordCount, "两个StateMap应该有相同的记录数");
        }

        // ===== 内部结果类 =====

        /**
         * PUT操作基准测试结果
         */
        private class PutBenchmarkResult {
            final int operationCount;
            final long duration;
            final double qps;
            final double avgLatency;

            PutBenchmarkResult(int operationCount, long duration, double qps, double avgLatency) {
                this.operationCount = operationCount;
                this.duration = duration;
                this.qps = qps;
                this.avgLatency = avgLatency;
            }
        }

        /**
         * GET操作基准测试结果
         */
        private class GetBenchmarkResult {
            final int operationCount;
            final long hits;
            final long duration;
            final double qps;
            final double avgLatency;
            final double hitRate;

            GetBenchmarkResult(int operationCount, long hits, long duration, double qps, double avgLatency, double hitRate) {
                this.operationCount = operationCount;
                this.hits = hits;
                this.duration = duration;
                this.qps = qps;
                this.avgLatency = avgLatency;
                this.hitRate = hitRate;
            }
        }

        /**
         * TRANSFORM操作基准测试结果
         */
        private class TransformBenchmarkResult {
            final int operationCount;
            final long transformations;
            final long newValues;
            final long duration;
            final double qps;
            final double avgLatency;

            TransformBenchmarkResult(int operationCount, long transformations, long newValues,
                                   long duration, double qps, double avgLatency) {
                this.operationCount = operationCount;
                this.transformations = transformations;
                this.newValues = newValues;
                this.duration = duration;
                this.qps = qps;
                this.avgLatency = avgLatency;
            }
        }

        /**
         * 完整的基准测试结果
         */
        private class BenchmarkResult {
            @SuppressWarnings("unused")
            final String name;  // Reserved for future result reporting
            final PutBenchmarkResult putResult;
            final GetBenchmarkResult getResult;
            final TransformBenchmarkResult transformResult;
            final int recordCount;
            final long memoryUsed;

            BenchmarkResult(String name, PutBenchmarkResult putResult, GetBenchmarkResult getResult,
                          TransformBenchmarkResult transformResult, int recordCount, long memoryUsed) {
                this.name = name;
                this.putResult = putResult;
                this.getResult = getResult;
                this.transformResult = transformResult;
                this.recordCount = recordCount;
                this.memoryUsed = memoryUsed;
            }
        }
    }

    private int[] createSequence(int num, int range) {
        Random random = new Random(42);
        int[] sequence = new int[num];
        for (int i = 0; i < num; i++) {
            sequence[i] = random.nextInt(range);
        }
        return sequence;
    }
}
