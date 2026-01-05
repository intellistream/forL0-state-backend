package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ForL0StateBackendOptions and ForL0StateBackendConfig with SwissMap architecture.
 */
class ForL0StateBackendOptionsTest {

    @Nested
    class ConfigParsingTest {

        @Test
        void testDefaultConfig() {
            ForL0StateBackendConfig config = new ForL0StateBackendConfig();
            
            assertEquals(-1, config.getL0MemoryMaxBytes(), "Default memory should be unlimited (-1)");
        }

        @Test
        void testConfigFromReadableConfig() {
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(ForL0StateBackendOptions.L0_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(128));
            
            ForL0StateBackendConfig config = new ForL0StateBackendConfig(flinkConfig);
            
            assertEquals(128 * 1024 * 1024L, config.getL0MemoryMaxBytes());
        }

        @Test
        void testConfigFromReadableConfig_zeroMeansUnlimited() {
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(ForL0StateBackendOptions.L0_MEMORY_MAX_SIZE, MemorySize.ZERO);
            
            ForL0StateBackendConfig config = new ForL0StateBackendConfig(flinkConfig);
            
            assertEquals(-1, config.getL0MemoryMaxBytes(), "Zero should mean unlimited");
        }
    }

    @Nested
    class BuilderTest {

        @Test
        void testBuilder_default() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder().build();
            
            assertEquals(-1, config.getL0MemoryMaxBytes());
        }

        @Test
        void testBuilder_setMemoryMaxBytes() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0MemoryMaxBytes(256 * 1024 * 1024L)
                    .build();
            
            assertEquals(256 * 1024 * 1024L, config.getL0MemoryMaxBytes());
        }

        @Test
        void testBuilder_setMemorySize() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0MemoryMaxSize(MemorySize.ofMebiBytes(64))
                    .build();
            
            assertEquals(64 * 1024 * 1024L, config.getL0MemoryMaxBytes());
        }

    }

    @Nested
    class ToStringTest {

        @Test
        void testToString_unlimited() {
            ForL0StateBackendConfig config = new ForL0StateBackendConfig();
            String str = config.toString();
            
            assertTrue(str.contains("unlimited"));
        }

        @Test
        void testToString_withLimit() {
            ForL0StateBackendConfig config = ForL0StateBackendConfig.builder()
                    .setL0MemoryMaxBytes(1024)
                    .build();
            String str = config.toString();
            
            assertTrue(str.contains("1024"));
        }
    }
}
