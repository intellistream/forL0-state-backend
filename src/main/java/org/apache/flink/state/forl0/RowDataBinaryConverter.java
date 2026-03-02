package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Runtime adapter that converts RowData state values to BinaryRowData before storing
 * in SwissTable, so that checkpoint serialization becomes O(memcpy) instead of 
 * O(n_fields) per entry.
 * 
 * <p>This class uses reflection to avoid compile-time dependency on flink-table modules.
 * If the flink-table classes are not available at runtime (e.g., DataStream-only jobs),
 * the converter silently disables itself and returns values as-is.
 * 
 * <p>Key mechanism:
 * <ul>
 *   <li>Detects if stateSerializer is RowDataSerializer via class name matching</li>
 *   <li>Calls RowDataSerializer.toBinaryRow(RowData) to convert GenericRowData → BinaryRowData</li>
 *   <li>Calls BinaryRowData.copy() to detach from RowDataSerializer's reuse row</li>
 *   <li>Stores the standalone BinaryRowData in SwissTable</li>
 *   <li>At checkpoint: RowDataSerializer.serialize() sees BinaryRowData → O(1) cast + memcpy</li>
 * </ul>
 * 
 * <p>Thread safety: NOT thread-safe (Flink state access is single-threaded).
 */
final class RowDataBinaryConverter {
    
    private static final Logger LOG = LoggerFactory.getLogger(RowDataBinaryConverter.class);

    private static final String ROW_DATA_SERIALIZER_CLASS = 
            "org.apache.flink.table.runtime.typeutils.RowDataSerializer";
    private static final String BINARY_ROW_DATA_CLASS = 
            "org.apache.flink.table.data.binary.BinaryRowData";
    
    /** Whether this converter is active (RowDataSerializer detected). */
    private final boolean active;
    
    /** Cached RowDataSerializer.toBinaryRow(RowData) method. */
    private final Method toBinaryRowMethod;
    
    /** Cached BinaryRowData.copy() method. */
    private final Method binaryRowCopyMethod;
    
    /** The BinaryRowData class (for instanceof checks). */
    private final Class<?> binaryRowDataClass;

    /** The serializer instance (for toBinaryRow invocation). */
    private final Object serializer;

    /**
     * Creates a converter for the given state serializer.
     * If the serializer is not RowDataSerializer, returns an inactive converter.
     */
    @SuppressWarnings("unchecked")
    static RowDataBinaryConverter create(TypeSerializer<?> stateSerializer) {
        try {
            if (stateSerializer.getClass().getName().equals(ROW_DATA_SERIALIZER_CLASS)) {
                Class<?> binaryRowClass = Class.forName(BINARY_ROW_DATA_CLASS);
                // RowDataSerializer.toBinaryRow takes RowData parameter
                Class<?> rowDataClass = Class.forName("org.apache.flink.table.data.RowData");
                Method toBinaryRow = stateSerializer.getClass().getMethod("toBinaryRow", rowDataClass);
                Method copy = binaryRowClass.getMethod("copy");
                
                LOG.info("[ForL0] RowDataBinaryConverter activated for serializer: {}",
                        stateSerializer.getClass().getSimpleName());
                return new RowDataBinaryConverter(true, toBinaryRow, copy, binaryRowClass, stateSerializer);
            }
        } catch (Exception e) {
            LOG.debug("[ForL0] Failed to initialize RowDataBinaryConverter, falling back to standard path", e);
        }
        return new RowDataBinaryConverter(false, null, null, null, null);
    }

    private RowDataBinaryConverter(
            boolean active, Method toBinaryRowMethod, Method binaryRowCopyMethod, 
            Class<?> binaryRowDataClass, Object serializer) {
        this.active = active;
        this.toBinaryRowMethod = toBinaryRowMethod;
        this.binaryRowCopyMethod = binaryRowCopyMethod;
        this.binaryRowDataClass = binaryRowDataClass;
        this.serializer = serializer;
    }

    /**
     * Returns true if this converter can convert RowData values to BinaryRowData.
     */
    boolean isActive() {
        return active;
    }

    /**
     * Converts a RowData value to BinaryRowData if:
     * 1. The converter is active (state serializer is RowDataSerializer)
     * 2. The value is not already BinaryRowData (avoids unnecessary copy)
     * 
     * @param value the state value (expected to be a RowData instance)
     * @return BinaryRowData copy if conversion needed, or the original value
     */
    @SuppressWarnings("unchecked")
    <V> V convert(V value) {
        if (!active || value == null) {
            return value;
        }
        try {
            // Skip if already BinaryRowData — it's already in optimal form
            if (binaryRowDataClass.isInstance(value)) {
                return value;
            }
            // toBinaryRow returns a REUSE BinaryRowData, so we must copy()
            Object binaryRow = toBinaryRowMethod.invoke(serializer, value);
            Object copied = binaryRowCopyMethod.invoke(binaryRow);
            return (V) copied;
        } catch (Exception e) {
            // Fall back silently — don't break state operations
            return value;
        }
    }
}
