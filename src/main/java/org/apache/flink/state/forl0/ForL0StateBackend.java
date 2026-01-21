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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ForL0 State Backend - A high-performance state backend using Swiss Tables.
 *
 * <p>This state backend uses Swiss Tables architecture (aligned with Go 1.24) for
 * efficient hash table operations. It features SWAR (SIMD Within A Register) parallel
 * matching for fast lookups and Extendible Hashing for scalability.
 *
 * <h1>Key Features</h1>
 * <ul>
 *   <li>Swiss Tables with SWAR parallel matching (8 slots compared simultaneously)</li>
 *   <li>Go 1.24 style hash bit allocation (H1 for probe, H2 for control)</li>
 *   <li>Extendible Hashing for graceful scaling</li>
 *   <li>Optional L0 Cache integration for Kunpeng CPUs</li>
 * </ul>
 *
 * <h1>State Size Considerations</h1>
 *
 * <p>Working state is kept on the TaskManager heap (or L0 Cache when available).
 * If a TaskManager executes multiple tasks concurrently, the aggregate state of
 * all tasks needs to fit into that TaskManager's memory.
 *
 * <h1>Configuration</h1>
 *
 * <p>This backend can be configured via application code or Flink configuration:
 * <ul>
 *   <li>{@code state.backend.forl0.async-snapshots} - Enable async snapshots (default: true)</li>
 * </ul>
 *
 * @see ForL0Options
 */
@PublicEvolving
public class ForL0StateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackend.class);

    /** Whether to use async snapshots. */
    private final boolean asyncSnapshots;

    // -----------------------------------------------------------------------

    /**
     * Creates a new ForL0 state backend with default configuration.
     */
    public ForL0StateBackend() {
        this(true);
    }

    /**
     * Creates a new ForL0 state backend.
     *
     * @param asyncSnapshots Whether to use async snapshots.
     */
    public ForL0StateBackend(boolean asyncSnapshots) {
        this.asyncSnapshots = asyncSnapshots;
        LOG.info("[ForL0] ForL0StateBackend created (asyncSnapshots={})", asyncSnapshots);
    }

    private ForL0StateBackend(ForL0StateBackend original, ReadableConfig config) {
        // Configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
        // Configure async snapshots
        this.asyncSnapshots = config.getOptional(ForL0Options.ASYNC_SNAPSHOTS)
                .orElse(original.asyncSnapshots);
    }

    // -----------------------------------------------------------------------
    //  Configuration
    // -----------------------------------------------------------------------

    @Override
    public ForL0StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new ForL0StateBackend(this, config);
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        // We never share any files, all snapshots are full
        return true;
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    // -----------------------------------------------------------------------
    //  State Backend Creation
    // -----------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {

        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(
                        parameters.getKeyGroupRange(),
                        parameters.getNumberOfKeyGroups(),
                        128);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();

        try {
            return new ForL0KeyedStateBackendBuilder<>(
                    parameters.getKvStateRegistry(),
                    parameters.getKeySerializer(),
                    parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                    parameters.getNumberOfKeyGroups(),
                    parameters.getKeyGroupRange(),
                    parameters.getEnv().getExecutionConfig(),
                    parameters.getTtlTimeProvider(),
                    latencyTrackingStateConfig,
                    parameters.getStateHandles(),
                    getCompressionDecorator(parameters.getEnv().getExecutionConfig()),
                    priorityQueueSetFactory,
                    asyncSnapshots,
                    parameters.getCancelStreamRegistry())
                    .build();
        } catch (BackendBuildingException e) {
            throw new IOException("Failed to build ForL0KeyedStateBackend", e);
        }
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws BackendBuildingException {

        // Delegate operator state to Flink's default implementation
        return new DefaultOperatorStateBackendBuilder(
                parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                parameters.getEnv().getExecutionConfig(),
                true,
                parameters.getStateHandles(),
                parameters.getCancelStreamRegistry())
                .build();
    }

    // -----------------------------------------------------------------------
    //  Utilities
    // -----------------------------------------------------------------------

    /**
     * Returns whether async snapshots are enabled.
     */
    public boolean isAsyncSnapshots() {
        return asyncSnapshots;
    }

    @Override
    public String toString() {
        return "ForL0StateBackend{asyncSnapshots=" + asyncSnapshots + "}";
    }
}
