package org.apache.flink.runtime.heap;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/**
 * A factory that creates an {@link ForL0StateBackend} from a configuration
 */
public class ForL0StateBackendFactory implements StateBackendFactory<ForL0StateBackend> {

    @Override
    public ForL0StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new ForL0StateBackend().configure(config, classLoader);
    }
}
