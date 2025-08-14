package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ForL0StateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackend.class);

    // ---------------------------------------------------------------------------------------------

    public ForL0StateBackend() {}

    private ForL0StateBackend(ForL0StateBackend original, ReadableConfig config) {
        // configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
    }

    @Override
    public ForL0StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new ForL0StateBackend(this, config);
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        // all snapshots are full, never share any files
        return true;
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {

        LOG.info("++++++++++++++ Creating ForL0KeyedStateBackend by ForL0StateBackend +++++++++++++++++++");

        Environment env = parameters.getEnv();
        LOG.info("Environment class: {}", env.getClass().getName());

        TaskStateManager taskStateManager = env.getTaskStateManager();
        LocalRecoveryConfig localRecoveryConfig = taskStateManager.createLocalRecoveryConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(
                        parameters.getKeyGroupRange(), parameters.getNumberOfKeyGroups(), 128);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();

        // Get MemoryManager and provide detailed debugging info
        MemoryManager memoryManager = env.getMemoryManager();
        LOG.info("MemoryManager from environment: {}", memoryManager);

        if (memoryManager == null) {
            LOG.error("MemoryManager is null! Environment details:");
            LOG.error("  Environment class: {}", env.getClass().getName());
            LOG.error("  Environment toString: {}", env.toString());
            LOG.error("This suggests the Flink environment is not properly configured for managed memory.");

            throw new IllegalStateException(
                "MemoryManager is null from Environment. " +
                "This indicates that the Flink TaskManager environment is not properly configured. " +
                "Environment class: " + env.getClass().getName());
        }

        LOG.info("MemoryManager successfully obtained: pageSize={}, class={}",
                memoryManager.getPageSize(), memoryManager.getClass().getName());

        return new ForL0KeyedStateBackendBuilder<>(
                        parameters.getKvStateRegistry(),
                        parameters.getKeySerializer(),
                        env.getUserCodeClassLoader().asClassLoader(),
                        parameters.getNumberOfKeyGroups(),
                        parameters.getKeyGroupRange(),
                        env.getExecutionConfig(),
                        parameters.getTtlTimeProvider(),
                        latencyTrackingStateConfig,
                        parameters.getStateHandles(),
                        getCompressionDecorator(env.getExecutionConfig()),
                        localRecoveryConfig,
                        priorityQueueSetFactory,
                        true,
                        parameters.getCancelStreamRegistry(),
                        memoryManager)
                .build();
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws BackendBuildingException {
        return new DefaultOperatorStateBackendBuilder(
                        parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                        parameters.getEnv().getExecutionConfig(),
                        true,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
                .build();
    }
}
