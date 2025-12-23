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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.ForL0KeyedStateBackendBuilder;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

/**
 * Utils to create keyed state backend for state micro benchmark.
 * Supports HEAP (Flink's default) and FORL0 (ForL0StateBackend with L0 cache disabled).
 */
public class StateBackendBenchmarkUtils {

    private static final String rootDirName = "benchmark";
    private static final String recoveryDirName = "localRecovery";
    private static File rootDir;

    /** Enum of backend type. */
    public enum StateBackendType {
        HEAP,
        FORL0
    }

    public static KeyedStateBackend<Long> createKeyedStateBackend(
            StateBackendType backendType, File baseDir, TtlTimeProvider ttlTimeProvider)
            throws IOException {
        switch (backendType) {
            case HEAP:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createHeapKeyedStateBackend(rootDir, ttlTimeProvider);
            case FORL0:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return createForL0KeyedStateBackend(rootDir, ttlTimeProvider);
            default:
                throw new IllegalArgumentException("Unknown backend type: " + backendType);
        }
    }

    public static KeyedStateBackend<Long> createKeyedStateBackend(
            StateBackendType backendType, File baseDir) throws IOException {
        return createKeyedStateBackend(backendType, baseDir, TtlTimeProvider.DEFAULT);
    }

    public static KeyedStateBackend<Long> createKeyedStateBackend(StateBackendType backendType)
            throws IOException {
        return createKeyedStateBackend(backendType, null);
    }

    /**
     * Creates ForL0 keyed state backend.
     * L0 cache is DISABLED for fair comparison with HeapKeyedStateBackend.
     */
    private static AbstractKeyedStateBackend<Long> createForL0KeyedStateBackend(
            File rootDir, TtlTimeProvider ttlTimeProvider) throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

        // l0CacheEnabled = false for fair comparison with HeapKeyedStateBackend
        ForL0KeyedStateBackendBuilder<Long> backendBuilder =
                new ForL0KeyedStateBackendBuilder<>(
                        null,
                        new LongSerializer(),
                        Thread.currentThread().getContextClassLoader(),
                        numberOfKeyGroups,
                        keyGroupRange,
                        executionConfig,
                        ttlTimeProvider,
                        LatencyTrackingStateConfig.disabled(),
                        Collections.emptyList(),
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED,
                        priorityQueueSetFactory,
                        false,
                        new CloseableRegistry(),
                        false);  // l0CacheEnabled = false
        return backendBuilder.build();
    }

    /**
     * Creates Heap keyed state backend (Flink's default HashMap-based backend).
     */
    private static HeapKeyedStateBackend<Long> createHeapKeyedStateBackend(
            File rootDir, TtlTimeProvider ttlTimeProvider) throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

        HeapKeyedStateBackendBuilder<Long> backendBuilder =
                new HeapKeyedStateBackendBuilder<>(
                        null,
                        new LongSerializer(),
                        Thread.currentThread().getContextClassLoader(),
                        numberOfKeyGroups,
                        keyGroupRange,
                        executionConfig,
                        ttlTimeProvider,
                        LatencyTrackingStateConfig.disabled(),
                        Collections.emptyList(),
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED,
                        priorityQueueSetFactory,
                        false,
                        new CloseableRegistry());
        return backendBuilder.build();
    }

    public static File prepareDirectory(String prefix, File parentDir) throws IOException {
        File target;
        if (parentDir != null) {
            target = new File(parentDir, prefix);
        } else {
            target = Files.createTempDirectory(prefix).toFile();
        }
        if (!target.exists() && !target.mkdirs()) {
            throw new IOException("Failed to create directory: " + target);
        }
        return target;
    }

    public static <T> ValueState<T> getValueState(
            KeyedStateBackend<Long> backend, ValueStateDescriptor<T> stateDescriptor)
            throws Exception {
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <T> ListState<T> getListState(
            KeyedStateBackend<Long> backend, ListStateDescriptor<T> stateDescriptor)
            throws Exception {
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <K, V> MapState<K, V> getMapState(
            KeyedStateBackend<Long> backend, MapStateDescriptor<K, V> stateDescriptor)
            throws Exception {
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public static <K, S extends State, T> void applyToAllKeys(
            KeyedStateBackend<K> backend,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception {
        backend.applyToAllKeys(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                stateDescriptor,
                function);
    }

    /**
     * Compact state for RocksDB backend. For HEAP and FORL0 backends, this is a no-op.
     * This method is kept for compatibility with existing benchmark classes.
     */
    public static <T> void compactState(
            RocksDBKeyedStateBackend<Long> rocksDBKeyedStateBackend,
            StateDescriptor<?, T> stateDescriptor) throws Exception {
        // This is a no-op since we only support HEAP and FORL0 backends.
        // RocksDB-specific compaction is not needed.
        // The method signature is kept for compatibility with ListStateBenchmark, etc.
    }

    public static void cleanUp(KeyedStateBackend<?> backend) throws IOException {
        backend.dispose();
        if (rootDir != null) {
            Path path = Path.fromLocalFile(rootDir);
            path.getFileSystem().delete(path, true);
        }
    }
}
