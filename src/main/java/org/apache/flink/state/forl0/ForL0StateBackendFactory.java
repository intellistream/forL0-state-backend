package org.apache.flink.state.forl0;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link ForL0StateBackend} instances.
 *
 * <p>This factory is used by Flink's SPI mechanism to create the state backend
 * when configured via {@code state.backend: forl0}.
 *
 * <p>Configuration options:
 * <ul>
 *   <li>{@code state.backend.forl0.async-snapshots} - Enable async snapshots (default: true)</li>
 * </ul>
 *
 * @see ForL0StateBackend
 * @see ForL0Options
 */
@PublicEvolving
public class ForL0StateBackendFactory implements StateBackendFactory<ForL0StateBackend> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackendFactory.class);

    /**
     * The name identifier for this state backend factory.
     */
    public static final String IDENTIFIER = "forl0";

    @Override
    public ForL0StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        LOG.info("[ForL0] Creating ForL0StateBackend from configuration.");
        return new ForL0StateBackend().configure(config, classLoader);
    }
}
