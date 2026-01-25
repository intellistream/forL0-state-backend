package org.apache.flink.state.forl0;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Reader for restoring state key-group by key-group.
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreKeyGroupReader<K, N, S> implements StateSnapshotKeyGroupReader {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateStoreKeyGroupReader.class);
    
    private final ForL0StateStore<K, N, S> stateStore;

    private ForL0StateStoreKeyGroupReader(ForL0StateStore<K, N, S> stateStore) {
        this.stateStore = stateStore;
    }

    public static <K, N, S> ForL0StateStoreKeyGroupReader<K, N, S> create(
            ForL0StateStore<K, N, S> stateStore) {
        return new ForL0StateStoreKeyGroupReader<>(stateStore);
    }

    @Override
    public void readMappingsInKeyGroup(@Nonnull DataInputView inView, int keyGroupId)
            throws IOException {
        // Read entries for this key group
        // Uses Flink's standard checkpoint format: namespace -> key -> state order
        int numEntries = inView.readInt();
        
        LOG.debug("[ForL0] Restoring {} entries for key group {} in state '{}'",
                numEntries, keyGroupId, stateStore.getStateName());
        
        for (int i = 0; i < numEntries; i++) {
            // Flink standard order: namespace -> key -> state
            N namespace = stateStore.getNamespaceSerializer().deserialize(inView);
            K key = stateStore.getKeySerializer().deserialize(inView);
            S state = stateStore.getStateSerializer().deserialize(inView);
            
            stateStore.put(key, namespace, state, keyGroupId);
        }
    }
}
