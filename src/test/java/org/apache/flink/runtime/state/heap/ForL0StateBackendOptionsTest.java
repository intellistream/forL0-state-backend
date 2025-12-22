package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ForL0StateBackend configuration options.
 */
class ForL0StateBackendOptionsTest {

    @Nested
    class ConfigOptionDefaultsTest {

        @Test
        void testL0CacheEnabledDefault() {
            assertEquals(true, ForL0StateBackendOptions.L0_CACHE_ENABLED.defaultValue());
        }

        @Test
        void testL0CacheSizeDefault() {
            assertEquals(10, ForL0StateBackendOptions.L0_CACHE_SIZE.defaultValue());
        }

        @Test
        void testL0CacheReplacementPolicyDefault() {
            assertEquals("CLOCK", ForL0StateBackendOptions.L0_CACHE_REPLACEMENT_POLICY.defaultValue());
        }

        @Test
        void testMainTableLoadFactorThresholdDefault() {
            assertEquals(1.5, ForL0StateBackendOptions.MAIN_TABLE_LOAD_FACTOR_THRESHOLD.defaultValue());
        }
    }

    @Nested
    class ValidationTest {

        @Test
        void testValidateL0CacheSize_valid() {
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateL0CacheSize(1));
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateL0CacheSize(10));
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateL0CacheSize(20));
        }

        @Test
        void testValidateL0CacheSize_invalid() {
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateL0CacheSize(0));
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateL0CacheSize(-1));
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateL0CacheSize(21));
        }

        @Test
        void testValidateLoadFactorThreshold_valid() {
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateLoadFactorThreshold(0.5));
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateLoadFactorThreshold(1.5));
            assertDoesNotThrow(() -> ForL0StateBackendOptions.validateLoadFactorThreshold(4.0));
        }

        @Test
        void testValidateLoadFactorThreshold_invalid() {
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateLoadFactorThreshold(0.4));
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateLoadFactorThreshold(4.1));
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.validateLoadFactorThreshold(-1.0));
        }

        @Test
        void testParseReplacementPolicy_valid() {
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy("CLOCK"));
            assertEquals(L0Table.ReplacementPolicy.LRU, 
                ForL0StateBackendOptions.parseReplacementPolicy("LRU"));
            assertEquals(L0Table.ReplacementPolicy.LFU, 
                ForL0StateBackendOptions.parseReplacementPolicy("LFU"));
            assertEquals(L0Table.ReplacementPolicy.TINY_LFU, 
                ForL0StateBackendOptions.parseReplacementPolicy("TINY_LFU"));
            assertEquals(L0Table.ReplacementPolicy.SAMPLED_LRU, 
                ForL0StateBackendOptions.parseReplacementPolicy("SAMPLED_LRU"));
        }

        @Test
        void testParseReplacementPolicy_caseInsensitive() {
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy("clock"));
            assertEquals(L0Table.ReplacementPolicy.LRU, 
                ForL0StateBackendOptions.parseReplacementPolicy("lru"));
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy("Clock"));
        }

        @Test
        void testParseReplacementPolicy_nullOrEmpty() {
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy(null));
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy(""));
            assertEquals(L0Table.ReplacementPolicy.CLOCK, 
                ForL0StateBackendOptions.parseReplacementPolicy("   "));
        }

        @Test
        void testParseReplacementPolicy_invalid() {
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.parseReplacementPolicy("INVALID"));
            assertThrows(IllegalArgumentException.class, 
                () -> ForL0StateBackendOptions.parseReplacementPolicy("RANDOM"));
        }
    }

    @Nested
    class ConfigParsingTest {

        @Test
        void testDefaultConfig() {
            ForL0StateBackendConfig config = new ForL0StateBackendConfig();
            
            assertTrue(config.isL0CacheEnabled());
            assertEquals(10, config.getL0CacheSize());
            assertEquals(L0Table.ReplacementPolicy.CLOCK, config.getL0ReplacementPolicy());
            assertEquals(-1, config.getL0MemoryMaxBytes()); // unlimited
            assertEquals(1.5, config.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigFromReadableConfig() {
            Configuration configuration = new Configuration();
            configuration.set(ForL0StateBackendOptions.L0_CACHE_ENABLED, false);
            configuration.set(ForL0StateBackendOptions.L0_CACHE_SIZE, 12);
            configuration.set(ForL0StateBackendOptions.L0_CACHE_REPLACEMENT_POLICY, "LRU");
            configuration.set(ForL0StateBackendOptions.L0_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(256));
            configuration.set(ForL0StateBackendOptions.MAIN_TABLE_LOAD_FACTOR_THRESHOLD, 2.0);

            ForL0StateBackendConfig config = new ForL0StateBackendConfig(configuration);

            assertFalse(config.isL0CacheEnabled());
            assertEquals(12, config.getL0CacheSize());
            assertEquals(L0Table.ReplacementPolicy.LRU, config.getL0ReplacementPolicy());
            assertEquals(256 * 1024 * 1024, config.getL0MemoryMaxBytes());
            assertEquals(2.0, config.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigWithL0CacheDisabled() {
            ForL0StateBackendConfig config = new ForL0StateBackendConfig();
            ForL0StateBackendConfig disabledConfig = config.withL0CacheDisabled();

            assertTrue(config.isL0CacheEnabled());
            assertFalse(disabledConfig.isL0CacheEnabled());
            
            // Other settings should be preserved
            assertEquals(config.getL0CacheSize(), disabledConfig.getL0CacheSize());
            assertEquals(config.getMainTableLoadFactorThreshold(), disabledConfig.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigBuilder() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0CacheEnabled(true)
                    .setL0CacheSize(14)
                    .setL0ReplacementPolicy(L0Table.ReplacementPolicy.LFU)
                    .setL0MemoryMaxBytes(512 * 1024 * 1024L)
                    .setMainTableLoadFactorThreshold(1.8)
                    .build();

            assertTrue(config.isL0CacheEnabled());
            assertEquals(14, config.getL0CacheSize());
            assertEquals(L0Table.ReplacementPolicy.LFU, config.getL0ReplacementPolicy());
            assertEquals(512 * 1024 * 1024L, config.getL0MemoryMaxBytes());
            assertEquals(1.8, config.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigBuilderWithPolicyString() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0ReplacementPolicy("TINY_LFU")
                    .build();

            assertEquals(L0Table.ReplacementPolicy.TINY_LFU, config.getL0ReplacementPolicy());
        }

        @Test
        void testConfigBuilderWithMemorySize() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0MemoryMaxSize(MemorySize.ofMebiBytes(128))
                    .build();

            assertEquals(128 * 1024 * 1024L, config.getL0MemoryMaxBytes());
        }

        @Test
        void testConfigBuilderValidation() {
            assertThrows(IllegalArgumentException.class, () -> 
                ForL0StateBackendConfig.builder()
                    .setL0CacheSize(0)
                    .build());

            assertThrows(IllegalArgumentException.class, () -> 
                ForL0StateBackendConfig.builder()
                    .setMainTableLoadFactorThreshold(0.1)
                    .build());
        }
    }

    @Nested
    class ConfigToStringTest {

        @Test
        void testToString() {
            ForL0StateBackendConfig config = new ForL0StateBackendConfig();
            String str = config.toString();

            assertTrue(str.contains("l0CacheEnabled=true"));
            assertTrue(str.contains("l0CacheSize=10"));
            assertTrue(str.contains("l0ReplacementPolicy=CLOCK"));
            assertTrue(str.contains("mainTableLoadFactorThreshold=1.5"));
        }
    }

    /**
     * End-to-end integration test that verifies configuration flows correctly from
     * Flink Configuration (simulating config.yaml) through ForL0StateBackend.configure()
     * to the final ForL0StateBackendConfig.
     *
     * This test simulates what happens when a user sets these options in config.yaml:
     * <pre>
     * state.backend.forl0.l0-cache.enabled: false
     * state.backend.forl0.l0-cache.size: 14
     * state.backend.forl0.l0-cache.replacement-policy: LRU
     * state.backend.forl0.l0-memory.max-size: 512mb
     * state.backend.forl0.main-table.initial-size: 12
     * state.backend.forl0.main-table.load-factor-threshold: 2.0
     * </pre>
     */
    @Nested
    class EndToEndConfigurationTest {

        @Test
        void testConfigurationFlowThroughStateBackend() {
            // Step 1: Create a Flink Configuration (simulates config.yaml parsing)
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(ForL0StateBackendOptions.L0_CACHE_ENABLED, false);
            flinkConfig.set(ForL0StateBackendOptions.L0_CACHE_SIZE, 14);
            flinkConfig.set(ForL0StateBackendOptions.L0_CACHE_REPLACEMENT_POLICY, "LRU");
            flinkConfig.set(ForL0StateBackendOptions.L0_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(512));
            flinkConfig.set(ForL0StateBackendOptions.MAIN_TABLE_LOAD_FACTOR_THRESHOLD, 2.0);

            // Step 2: Create a default ForL0StateBackend
            ForL0StateBackend originalBackend = new ForL0StateBackend();

            // Step 3: Call configure() - this is what Flink does when initializing the StateBackend
            ForL0StateBackend configuredBackend = originalBackend.configure(flinkConfig, Thread.currentThread().getContextClassLoader());

            // Step 4: Verify the configuration was correctly applied
            ForL0StateBackendConfig config = configuredBackend.getForL0Config();

            assertFalse(config.isL0CacheEnabled(), "L0 cache should be disabled");
            assertEquals(14, config.getL0CacheSize(), "L0 cache size should be 14");
            assertEquals(L0Table.ReplacementPolicy.LRU, config.getL0ReplacementPolicy(), "Replacement policy should be LRU");
            assertEquals(512 * 1024 * 1024L, config.getL0MemoryMaxBytes(), "L0 memory max should be 512MB");
            assertEquals(2.0, config.getMainTableLoadFactorThreshold(), "Load factor threshold should be 2.0");
        }

        @Test
        void testPartialConfigurationWithDefaults() {
            // Only configure some options, others should use defaults
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(ForL0StateBackendOptions.L0_CACHE_SIZE, 15);
            flinkConfig.set(ForL0StateBackendOptions.L0_CACHE_REPLACEMENT_POLICY, "TINY_LFU");

            ForL0StateBackend originalBackend = new ForL0StateBackend();
            ForL0StateBackend configuredBackend = originalBackend.configure(flinkConfig, Thread.currentThread().getContextClassLoader());

            ForL0StateBackendConfig config = configuredBackend.getForL0Config();

            // Explicitly set values
            assertEquals(15, config.getL0CacheSize());
            assertEquals(L0Table.ReplacementPolicy.TINY_LFU, config.getL0ReplacementPolicy());

            // Default values for unset options
            assertTrue(config.isL0CacheEnabled());
            assertEquals(-1, config.getL0MemoryMaxBytes()); // unlimited
            assertEquals(1.5, config.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigurationWithStringKeys() {
            // Test using string keys as they would appear in config.yaml
            Configuration flinkConfig = new Configuration();
            flinkConfig.setString("state.backend.forl0.l0-cache.enabled", "false");
            flinkConfig.setString("state.backend.forl0.l0-cache.size", "16");
            flinkConfig.setString("state.backend.forl0.l0-cache.replacement-policy", "LFU");
            flinkConfig.setString("state.backend.forl0.l0-memory.max-size", "1gb");
            flinkConfig.setString("state.backend.forl0.main-table.load-factor-threshold", "1.8");

            ForL0StateBackend originalBackend = new ForL0StateBackend();
            ForL0StateBackend configuredBackend = originalBackend.configure(flinkConfig, Thread.currentThread().getContextClassLoader());

            ForL0StateBackendConfig config = configuredBackend.getForL0Config();

            assertFalse(config.isL0CacheEnabled());
            assertEquals(16, config.getL0CacheSize());
            assertEquals(L0Table.ReplacementPolicy.LFU, config.getL0ReplacementPolicy());
            assertEquals(1024L * 1024 * 1024, config.getL0MemoryMaxBytes()); // 1GB
            assertEquals(1.8, config.getMainTableLoadFactorThreshold());
        }

        @Test
        void testConfigurationKeysMatchExpected() {
            // Verify the configuration keys are exactly as documented
            assertEquals("state.backend.forl0.l0-cache.enabled", 
                    ForL0StateBackendOptions.L0_CACHE_ENABLED.key());
            assertEquals("state.backend.forl0.l0-cache.size", 
                    ForL0StateBackendOptions.L0_CACHE_SIZE.key());
            assertEquals("state.backend.forl0.l0-cache.replacement-policy", 
                    ForL0StateBackendOptions.L0_CACHE_REPLACEMENT_POLICY.key());
            assertEquals("state.backend.forl0.l0-memory.max-size", 
                    ForL0StateBackendOptions.L0_MEMORY_MAX_SIZE.key());
            assertEquals("state.backend.forl0.main-table.load-factor-threshold", 
                    ForL0StateBackendOptions.MAIN_TABLE_LOAD_FACTOR_THRESHOLD.key());
        }
    }
}
