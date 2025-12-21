package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class ForL0StateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackend.class);

    /** Configuration for ForL0StateBackend. */
    private final ForL0StateBackendConfig forl0Config;

    // ---------------------------------------------------------------------------------------------

    /** Creates a ForL0StateBackend with default configuration. */
    public ForL0StateBackend() {
        this.forl0Config = new ForL0StateBackendConfig();
    }

    /** Creates a ForL0StateBackend with the specified configuration. */
    public ForL0StateBackend(ForL0StateBackendConfig config) {
        this.forl0Config = config;
    }

    private ForL0StateBackend(ForL0StateBackend original, ReadableConfig config) {
        // configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
        // parse ForL0 specific configuration
        this.forl0Config = new ForL0StateBackendConfig(config);
    }

    @Override
    public ForL0StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new ForL0StateBackend(this, config);
    }

    /**
     * Gets the ForL0 configuration.
     */
    public ForL0StateBackendConfig getForL0Config() {
        return forl0Config;
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
        
        LOG.info("Using ForL0StateBackend configuration: {}", forl0Config);

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
                        forl0Config)
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
