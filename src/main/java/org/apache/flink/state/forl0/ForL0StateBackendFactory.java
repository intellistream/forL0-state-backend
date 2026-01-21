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
        
        boolean asyncSnapshots = config.getOptional(ForL0Options.ASYNC_SNAPSHOTS)
                .orElse(true);
        
        return new ForL0StateBackend(asyncSnapshots);
    }
}
