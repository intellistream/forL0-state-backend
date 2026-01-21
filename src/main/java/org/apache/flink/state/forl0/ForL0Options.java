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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options for ForL0StateBackend.
 */
@PublicEvolving
public class ForL0Options {

    // ========== StateBackend Type ==========

    /** The identifier for ForL0StateBackend in configuration. */
    public static final String STATE_BACKEND_TYPE = "forl0";

    // ========== SwissTable Options ==========

    public static final ConfigOption<Integer> INITIAL_TABLE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.initial-table-capacity")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Initial capacity of each SwissTable. Must be a power of 2 and >= 8.");

    public static final ConfigOption<Integer> MAX_TABLE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.max-table-capacity")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum capacity of each SwissTable before triggering split. Must be a power of 2.");

    // ========== Snapshot Options ==========

    public static final ConfigOption<Boolean> ASYNC_SNAPSHOTS =
            ConfigOptions.key("state.backend.forl0.async-snapshots")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use asynchronous snapshots.");

    // ========== L0 Memory Options ==========

    public static final ConfigOption<Boolean> L0_CACHE_ENABLED =
            ConfigOptions.key("state.backend.forl0.l0-cache.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable L0 Cache memory allocation. " +
                            "This requires the native library and Kunpeng CPU with L0 support.");

    public static final ConfigOption<Long> L0_CACHE_SIZE =
            ConfigOptions.key("state.backend.forl0.l0-cache.size")
                    .longType()
                    .defaultValue(256L * 1024 * 1024)  // 256MB default
                    .withDescription("Size of L0 Cache memory pool in bytes.");

    // ========== Internal Constants ==========

    /** Minimum table capacity. */
    public static final int MIN_TABLE_CAPACITY = 8;

    private ForL0Options() {
        // Utility class, no instantiation
    }

    /**
     * Validates the table capacity value.
     * @param capacity the capacity to validate
     * @return true if valid (power of 2 and >= 8)
     */
    public static boolean isValidTableCapacity(int capacity) {
        return capacity >= MIN_TABLE_CAPACITY && (capacity & (capacity - 1)) == 0;
    }
}
