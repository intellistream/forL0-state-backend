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

package com.github.nexmark.flink.source;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class NexmarkUnlimitedRateTest {
    @Test public void zeroRateKeepsFiniteEventTimeWithoutWallclockThrottling() {
        Configuration options = new Configuration();
        options.set(NexmarkSourceOptions.FIRST_EVENT_RATE, 0);
        options.set(NexmarkSourceOptions.NEXT_EVENT_RATE, 0);
        options.set(NexmarkSourceOptions.RATE_LIMITED, true);
        NexmarkConfiguration config = NexmarkSourceOptions.convertToNexmarkConfiguration(options);
        assertFalse(config.isRateLimited);
        assertEquals(new NexmarkConfiguration().firstEventRate, config.firstEventRate);
        GeneratorConfig generator = new GeneratorConfig(config, 1000L, 1L, 100L, 1L);
        long timestamp = generator.timestampForEvent(10000L);
        assertTrue(timestamp >= 1000L && timestamp < 100000L);
        assertFalse(generator.isRateLimited());
    }

    @Test public void positiveRatesKeepTheExplicitThrottleSetting() {
        Configuration options = new Configuration();
        options.set(NexmarkSourceOptions.FIRST_EVENT_RATE, 123);
        options.set(NexmarkSourceOptions.NEXT_EVENT_RATE, 456);
        options.set(NexmarkSourceOptions.RATE_LIMITED, true);
        NexmarkConfiguration config = NexmarkSourceOptions.convertToNexmarkConfiguration(options);
        assertTrue(config.isRateLimited);
        assertEquals(123, config.firstEventRate);
        assertEquals(456, config.nextEventRate);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeRateIsRejected() {
        Configuration options = new Configuration();
        options.set(NexmarkSourceOptions.FIRST_EVENT_RATE, -1);
        NexmarkSourceOptions.convertToNexmarkConfiguration(options);
    }

    @Test(expected = IllegalArgumentException.class)
    public void oneZeroRateIsRejected() {
        Configuration options = new Configuration();
        options.set(NexmarkSourceOptions.FIRST_EVENT_RATE, 0);
        NexmarkSourceOptions.convertToNexmarkConfiguration(options);
    }
}
