package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * ForL0 specialized restore operation that handles ForL0StateBackend's checkpoint format.
 * This class extends HeapRestoreOperation and uses the same data format for compatibility.
 */
public class ForL0RestoreOperation<K> extends HeapRestoreOperation<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0RestoreOperation.class);

    public ForL0RestoreOperation(
            Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider,
            ClassLoader userCodeClassLoader,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            CloseableRegistry cancelStreamRegistry,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        super(
                restoreStateHandles,
                keySerializerProvider,
                userCodeClassLoader,
                registeredKVStates,
                registeredPQStates,
                cancelStreamRegistry,
                priorityQueueSetFactory,
                keyGroupRange,
                numberOfKeyGroups,
                stateTableFactory,
                keyContext);
    }

    @Override
    public Void restore() throws Exception {
        LOG.info("Starting ForL0RestoreOperation");

        try {
            // Use the standard HeapRestoreOperation restore logic directly
            // ForL0StateBackend should use the same data format as HeapStateBackend
            super.restore();
            LOG.info("Successfully restored ForL0StateBackend using standard restore operation");
            return null;
        } catch (Exception e) {
            // Log the error for debugging but don't try to handle it specially
            // Any format issues should be fixed at the snapshot creation level
            LOG.error("ForL0StateBackend restore failed: {}", e.getMessage(), e);
            throw new Exception("Failed to restore ForL0StateBackend: " + e.getMessage(), e);
        }
    }
}
