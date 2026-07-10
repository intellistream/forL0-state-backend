package org.apache.flink.state.forl0;

/**
 * Optional ForL0 fast path for long-valued counters.
 *
 * <p>Benchmark code discovers this interface reflectively or by class name when
 * the ForL0 backend is present. HashMapStateBackend and other backends are not
 * affected.
 */
public interface LongValueStateAddAndGet {
    long addAndGetLong(long delta) throws Exception;
}
