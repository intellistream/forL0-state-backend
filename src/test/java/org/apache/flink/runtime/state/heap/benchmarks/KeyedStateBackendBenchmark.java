package org.apache.flink.runtime.state.heap.benchmarks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.ForL0KeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark comparing ForL0KeyedStateBackend vs Flink HeapKeyedStateBackend on ValueState.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(0)
@State(Scope.Benchmark)
public class KeyedStateBackendBenchmark {

    @Param({"forl0","heap"})
    public String backend;

    // 新增：是否启用L0缓存，仅在 backend=forl0 时生效
    @Param({"true","false"})
    public boolean l0CacheEnabled;

    // 放大 key 空间
    @Param({"100000","1000000"})
    public int numKeys;

    // mixed 写入百分比（0-100）
    @Param({"20"})
    public int mixedWritePercent;

    // 键分布：uniform 均匀；hot 热点（按热集/冷集概率）
    @Param({"uniform","hot","zipf"})
    public String keyDistribution;

    // 热点分布参数：热集占比（百分比）与访问概率（百分比），仅在 keyDistribution=hot 时生效
    @Param({"10"})
    public int hotKeyFractionPercent;

    @Param({"90"})
    public int hotAccessPercent;

    // Zipf 分布参数：θ（幂指数）与预生成样本数
    @Param({"0.9"})
    public String zipfTheta;

    @Param({"200000"})
    public int zipfPregen;

    // 值大小（字节）。为 0 则使用 int 值；>0 则使用 byte[] 值并按该大小构造
    @Param({"0","512"})
    public int valueBytes;

    private AbstractKeyedStateBackend<Integer> keyedBackend;

    // 两类状态与开关
    private ValueState<Integer> vsInt;
    private ValueState<byte[]> vsBytes;
    private boolean useBytes;

    private int[] keys;
    private Random rnd;

    // 热点参数派生
    private int hotKeyCount;

    // Zipf 预生成样本
    private int[] zipfKeys;

    // For ForL0 backend resource lifecycle
    private org.apache.flink.runtime.memory.MemoryManager forL0MemoryManager;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        rnd = new Random(123456);
        keys = new int[numKeys];
        for (int i = 0; i < numKeys; i++) { keys[i] = i; }

        hotKeyCount = Math.max(0, Math.min(numKeys, (int) Math.round(numKeys * (hotKeyFractionPercent / 100.0))));
        useBytes = valueBytes > 0;

        final TaskKvStateRegistry kvRegistry = null;
        final ClassLoader cl = getClass().getClassLoader();
        final int totalKeyGroups = 128;
        final KeyGroupRange keyGroupRange = new KeyGroupRange(0, totalKeyGroups - 1);
        final ExecutionConfig executionConfig = new ExecutionConfig();
        final TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        final LatencyTrackingStateConfig latencyConfig = LatencyTrackingStateConfig.disabled();
        final StreamCompressionDecorator compression = UncompressedStreamCompressionDecorator.INSTANCE;
        final HeapPriorityQueueSetFactory pqFactory = new HeapPriorityQueueSetFactory(keyGroupRange, totalKeyGroups, 128);
        final CloseableRegistry cancelRegistry = new CloseableRegistry();
        final LocalRecoveryConfig localRecovery = null; // disable local recovery in bench

        if ("forl0".equalsIgnoreCase(backend)) {

            forL0MemoryManager = MemoryManagerBuilder.newBuilder()
                    .setMemorySize(8192L * 12 * MemoryManager.DEFAULT_PAGE_SIZE)
                    .setPageSize(MemoryManager.DEFAULT_PAGE_SIZE)
                    .build();
            keyedBackend = new ForL0KeyedStateBackendBuilder<Integer>(
                    kvRegistry,
                    IntSerializer.INSTANCE,
                    cl,
                    totalKeyGroups,
                    keyGroupRange,
                    executionConfig,
                    ttlTimeProvider,
                    latencyConfig,
                    Collections.emptyList(),
                    compression,
                    localRecovery,
                    pqFactory,
                    true,
                    cancelRegistry,
                    forL0MemoryManager,
                    l0CacheEnabled // 将开关传入构造
            ).build();
        } else {
            keyedBackend = new HeapKeyedStateBackendBuilder<Integer>(
                    kvRegistry,
                    IntSerializer.INSTANCE,
                    cl,
                    totalKeyGroups,
                    keyGroupRange,
                    executionConfig,
                    ttlTimeProvider,
                    latencyConfig,
                    Collections.emptyList(),
                    compression,
                    localRecovery,
                    pqFactory,
                    true,
                    cancelRegistry
            ).build();
        }

        if (useBytes) {
            ValueStateDescriptor<byte[]> d = new ValueStateDescriptor<>(
                    "vsBytes", BytePrimitiveArraySerializer.INSTANCE, new byte[0]);
            vsBytes = keyedBackend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    d);
        } else {
            ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>(
                    "vsInt", IntSerializer.INSTANCE, 0);
            vsInt = keyedBackend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    d);
        }

        // 预填充所有 key
        for (int k : keys) {
            keyedBackend.setCurrentKey(k);
            if (useBytes) {
                vsBytes.update(new byte[Math.max(1, valueBytes)]);
            } else {
                vsInt.update(k);
            }
        }

        // 预生成 Zipf 样本表（仅当选择 zipf 分布时）
        if ("zipf".equalsIgnoreCase(keyDistribution)) {
            buildZipfSamples();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (keyedBackend != null) keyedBackend.dispose();
        if (forL0MemoryManager != null) { forL0MemoryManager.shutdown(); forL0MemoryManager = null; }
    }

    // 仅写：模拟持续写入
    @Benchmark
    public void put(Blackhole bh) throws Exception {
        final int key = chooseKey();
        keyedBackend.setCurrentKey(key);
        if (useBytes) {
            byte[] payload = new byte[valueBytes];
            rnd.nextBytes(payload);
            vsBytes.update(payload);
        } else {
            vsInt.update(rnd.nextInt());
        }
        bh.consume(key);
    }

    // 仅读：模拟持续读取
    @Benchmark
    public void get(Blackhole bh) throws Exception {
        final int key = chooseKey();
        keyedBackend.setCurrentKey(key);
        if (useBytes) {
            bh.consume(vsBytes.value());
        } else {
            bh.consume(vsInt.value());
        }
    }

    // 读写混合：按 mixedWritePercent 决定写/读
    @Benchmark
    public void mixed(Blackhole bh) throws Exception {
        final int key = chooseKey();
        keyedBackend.setCurrentKey(key);
        if (rnd.nextInt(100) < mixedWritePercent) {
            if (useBytes) {
                byte[] payload = new byte[valueBytes];
                rnd.nextBytes(payload);
                vsBytes.update(payload);
            } else {
                vsInt.update(rnd.nextInt());
            }
            bh.consume(key);
        } else {
            if (useBytes) {
                bh.consume(vsBytes.value());
            } else {
                bh.consume(vsInt.value());
            }
        }
    }

    private int chooseKey() {
        if ("zipf".equalsIgnoreCase(keyDistribution) && zipfKeys != null && zipfKeys.length > 0) {
            return zipfKeys[rnd.nextInt(zipfKeys.length)];
        }
        if ("hot".equalsIgnoreCase(keyDistribution) && hotKeyCount > 0) {
            if (rnd.nextInt(100) < hotAccessPercent) {
                // 热集：[0, hotKeyCount)
                return keys[rnd.nextInt(hotKeyCount)];
            } else {
                // 冷集：[hotKeyCount, numKeys)
                int cold = numKeys - hotKeyCount;
                if (cold <= 0) {
                    return keys[rnd.nextInt(numKeys)];
                }
                return keys[hotKeyCount + rnd.nextInt(cold)];
            }
        }
        // 均匀
        return keys[rnd.nextInt(numKeys)];
    }

    private void buildZipfSamples() {
        // 生成 Zipf 权重与累积分布，然后采样 zipfPregen 个键，存入 zipfKeys
        final int N = numKeys;
        if (N <= 0) { zipfKeys = new int[0]; return; }
        double theta;
        try {
            theta = Double.parseDouble(zipfTheta);
        } catch (Exception e) {
            theta = 1.0;
        }
        if (theta <= 0) { theta = 1.0; }
        double[] cdf = new double[N];
        double sum = 0.0;
        for (int i = 1; i <= N; i++) {
            sum += 1.0 / Math.pow(i, theta);
        }
        double acc = 0.0;
        for (int i = 1; i <= N; i++) {
            acc += (1.0 / Math.pow(i, theta)) / sum;
            cdf[i - 1] = acc;
        }
        int samples = Math.max(1, zipfPregen);
        int[] out = new int[samples];
        for (int s = 0; s < samples; s++) {
            double u = rnd.nextDouble();
            int idx = java.util.Arrays.binarySearch(cdf, u);
            if (idx < 0) { idx = -idx - 1; }
            if (idx >= N) { idx = N - 1; }
            out[s] = keys[idx];
        }
        zipfKeys = out;
    }
}
