package org.apache.flink.state.forl0;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ForL0ConfigurationTest {

    @Test
    void dividesJobWideL0BudgetAcrossExpectedEngines() throws Exception {
        Configuration config = new Configuration();
        config.set(ForL0Options.L0_CACHE_ENABLED, true);
        config.set(ForL0Options.L0_CACHE_TOTAL_SIZE, MemorySize.ofMebiBytes(512));
        config.set(ForL0Options.L0_CACHE_EXPECTED_ENGINES, 8);
        config.set(ForL0Options.MAX_TABLE_CAPACITY, 262144);
        config.set(ForL0Options.MAIN_TABLE_LOAD_FACTOR, 0.8);
        config.set(ForL0Options.NATIVE_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(1024));

        ForL0StateBackend backend = new ForL0StateBackend().configure(
                config, getClass().getClassLoader());

        assertEquals(MemorySize.ofMebiBytes(64).getBytes(), longField(backend, "l0CacheSize"));
        assertEquals(MemorySize.ofMebiBytes(1024).getBytes(), longField(backend, "nativeMemoryMaxSize"));
        assertEquals(262144, intField(backend, "maxTableCapacity"));
    }

    @Test
    void rejectsImpossibleLimitsBeforeNativeConstruction() {
        Configuration config = new Configuration();
        config.set(ForL0Options.INITIAL_TABLE_CAPACITY, 1024);
        config.set(ForL0Options.MAX_TABLE_CAPACITY, 512);
        assertThrows(IllegalConfigurationException.class, () ->
                new ForL0StateBackend().configure(config, getClass().getClassLoader()));
    }

    @Test
    void rejectsTooSmallPerEngineL0Quota() {
        Configuration config = new Configuration();
        config.set(ForL0Options.L0_CACHE_ENABLED, true);
        config.set(ForL0Options.L0_CACHE_TOTAL_SIZE, new MemorySize(192));
        config.set(ForL0Options.L0_CACHE_EXPECTED_ENGINES, 2);
        assertThrows(IllegalConfigurationException.class, () ->
                new ForL0StateBackend().configure(config, getClass().getClassLoader()));
    }

    @Test
    void canonicalUnlimitedNativeMemoryOverridesLegacyLimit() throws Exception {
        Configuration config = new Configuration();
        config.set(ForL0Options.NATIVE_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(0));
        config.set(ForL0Options.LEGACY_L0_MEMORY_MAX_SIZE, MemorySize.ofMebiBytes(64));

        ForL0StateBackend backend = new ForL0StateBackend().configure(
                config, getClass().getClassLoader());

        assertEquals(0L, longField(backend, "nativeMemoryMaxSize"));
    }

    private static long longField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static int intField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }
}
