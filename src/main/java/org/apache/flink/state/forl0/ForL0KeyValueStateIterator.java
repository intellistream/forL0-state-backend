package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link KeyValueStateIterator} implementation for ForL0 state backend.
 * 
 * <p>This iterator produces key-value pairs in the format expected by
 * {@link org.apache.flink.runtime.state.FullSnapshotAsyncWriter}.
 * The iteration order is by (key-group, kv-state).
 */
public class ForL0KeyValueStateIterator implements KeyValueStateIterator {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final Map<StateUID, Integer> stateNamesToId;
    private final Map<StateUID, StateSnapshot> stateSnapshots;
    private final int keyGroupPrefixBytes;

    private boolean isValid;
    private boolean newKeyGroup;
    private boolean newKVState;
    private byte[] currentKey;
    private byte[] currentValue;

    /** Iterator over the key groups. */
    private final Iterator<Integer> keyGroupIterator;
    private int currentKeyGroup;

    /** Iterator over all states. */
    private Iterator<StateUID> statesIterator;
    private StateUID currentState;

    /** Current iterator for the active state. */
    private SingleStateIterator currentStateIterator;

    /** Helpers for serialization. */
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final ListDelimitedSerializer listDelimitedSerializer = new ListDelimitedSerializer();
    private final SerializedCompositeKeyBuilder<Object> compositeKeyBuilder;

    @SuppressWarnings("unchecked")
    public ForL0KeyValueStateIterator(
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnull TypeSerializer<?> keySerializer,
            int totalKeyGroups,
            @Nonnull Map<StateUID, Integer> stateNamesToId,
            @Nonnull Map<StateUID, StateSnapshot> stateSnapshots) throws IOException {
        
        this.stateNamesToId = stateNamesToId;
        this.stateSnapshots = stateSnapshots;
        this.statesIterator = stateSnapshots.keySet().iterator();
        this.keyGroupIterator = keyGroupRange.iterator();
        
        this.keyGroupPrefixBytes = CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        this.compositeKeyBuilder = new SerializedCompositeKeyBuilder<>(
                (TypeSerializer<Object>) keySerializer, keyGroupPrefixBytes, 32);

        if (!keyGroupIterator.hasNext() || !statesIterator.hasNext()) {
            isValid = false;
        } else {
            currentKeyGroup = keyGroupIterator.next();
            next();
            this.newKeyGroup = true;
        }
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public boolean isNewKeyValueState() {
        return newKVState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return newKeyGroup;
    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    @Override
    public int kvStateId() {
        return stateNamesToId.get(currentState);
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return currentValue;
    }

    @Override
    public void next() throws IOException {
        this.newKVState = false;
        this.newKeyGroup = false;

        boolean nextElementSet = false;
        do {
            if (currentState == null) {
                boolean hasNextState = moveToNextState();
                if (!hasNextState) {
                    isValid = false;
                    return;
                }
            }

            boolean hasStateEntry = currentStateIterator != null && currentStateIterator.hasNext();
            if (!hasStateEntry) {
                this.currentState = null;
            }

            if (hasStateEntry) {
                nextElementSet = currentStateIterator.writeOutNext();
            }
        } while (!nextElementSet);
        isValid = true;
    }

    private boolean moveToNextState() throws IOException {
        if (statesIterator.hasNext()) {
            this.currentState = statesIterator.next();
            this.newKVState = true;
        } else if (keyGroupIterator.hasNext()) {
            this.currentKeyGroup = keyGroupIterator.next();
            resetStates();
            this.newKeyGroup = true;
            this.newKVState = true;
        } else {
            return false;
        }

        StateSnapshot stateSnapshot = this.stateSnapshots.get(currentState);
        setCurrentStateIterator(stateSnapshot);
        return true;
    }

    private void resetStates() {
        this.statesIterator = stateSnapshots.keySet().iterator();
        this.currentState = statesIterator.next();
    }

    @SuppressWarnings("unchecked")
    private void setCurrentStateIterator(StateSnapshot stateSnapshot) throws IOException {
        if (stateSnapshot instanceof IterableStateSnapshot) {
            RegisteredKeyValueStateBackendMetaInfo<Object, Object> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(stateSnapshot.getMetaInfoSnapshot());
            Iterator<? extends StateEntry<?, ?, ?>> snapshotIterator =
                    ((IterableStateSnapshot<?, ?, ?>) stateSnapshot).getIterator(currentKeyGroup);
            this.currentStateIterator = new StateTableIterator(snapshotIterator, metaInfo);
        } else if (stateSnapshot instanceof HeapPriorityQueueStateSnapshot) {
            Iterator<Object> snapshotIterator =
                    ((HeapPriorityQueueStateSnapshot<Object>) stateSnapshot).getIteratorForKeyGroup(currentKeyGroup);
            RegisteredPriorityQueueStateBackendMetaInfo<Object> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(stateSnapshot.getMetaInfoSnapshot());
            this.currentStateIterator = new QueueIterator<>(snapshotIterator, metaInfo);
        } else {
            throw new IllegalStateException("Unknown snapshot type: " + stateSnapshot);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }

    // ========== Internal Iterators ==========

    private interface SingleStateIterator {
        boolean hasNext();
        boolean writeOutNext() throws IOException;
    }

    private final class StateTableIterator implements SingleStateIterator {
        private final Iterator<? extends StateEntry<?, ?, ?>> entriesIterator;
        private final RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo;

        StateTableIterator(
                Iterator<? extends StateEntry<?, ?, ?>> entriesIterator,
                RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo) {
            this.entriesIterator = entriesIterator;
            this.metaInfo = metaInfo;
        }

        @Override
        public boolean hasNext() {
            return entriesIterator.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            StateEntry<?, ?, ?> entry = entriesIterator.next();
            valueOut.clear();
            compositeKeyBuilder.setKeyAndKeyGroup(entry.getKey(), keyGroup());
            compositeKeyBuilder.setNamespace(entry.getNamespace(), castToType(metaInfo.getNamespaceSerializer()));
            
            TypeSerializer<?> stateSerializer = metaInfo.getStateSerializer();
            switch (metaInfo.getStateType()) {
                case AGGREGATING:
                case REDUCING:
                case FOLDING:
                case VALUE:
                    return writeOutValue(entry, stateSerializer);
                case LIST:
                    return writeOutList(entry, stateSerializer);
                case MAP:
                    return writeOutMap(entry, stateSerializer);
                default:
                    throw new IllegalStateException("Unknown state type: " + metaInfo.getStateType());
            }
        }

        private boolean writeOutValue(StateEntry<?, ?, ?> entry, TypeSerializer<?> stateSerializer) throws IOException {
            currentKey = compositeKeyBuilder.build();
            castToType(stateSerializer).serialize(entry.getState(), valueOut);
            currentValue = valueOut.getCopyOfBuffer();
            return true;
        }

        @SuppressWarnings("unchecked")
        private boolean writeOutList(StateEntry<?, ?, ?> entry, TypeSerializer<?> stateSerializer) throws IOException {
            List<Object> state = (List<Object>) entry.getState();
            if (state.isEmpty()) {
                return false;
            }
            ListSerializer<Object> listSerializer = (ListSerializer<Object>) stateSerializer;
            currentKey = compositeKeyBuilder.build();
            currentValue = listDelimitedSerializer.serializeList(state, listSerializer.getElementSerializer());
            return true;
        }

        @SuppressWarnings("unchecked")
        private boolean writeOutMap(StateEntry<?, ?, ?> entry, TypeSerializer<?> stateSerializer) throws IOException {
            Map<Object, Object> state = (Map<Object, Object>) entry.getState();
            if (state.isEmpty()) {
                return false;
            }
            MapSerializer<Object, Object> mapSerializer = (MapSerializer<Object, Object>) stateSerializer;
            currentStateIterator = new MapStateIterator(
                    state,
                    mapSerializer.getKeySerializer(),
                    mapSerializer.getValueSerializer(),
                    this);
            return currentStateIterator.writeOutNext();
        }
    }

    private final class MapStateIterator implements SingleStateIterator {
        private final Iterator<Map.Entry<Object, Object>> mapEntries;
        private final TypeSerializer<Object> userKeySerializer;
        private final TypeSerializer<Object> userValueSerializer;
        private final StateTableIterator parentIterator;

        MapStateIterator(
                Map<Object, Object> mapEntries,
                TypeSerializer<Object> userKeySerializer,
                TypeSerializer<Object> userValueSerializer,
                StateTableIterator parentIterator) {
            this.mapEntries = mapEntries.entrySet().iterator();
            this.userKeySerializer = userKeySerializer;
            this.userValueSerializer = userValueSerializer;
            this.parentIterator = parentIterator;
        }

        @Override
        public boolean hasNext() {
            return true; // Map iterator should not be queried after exhaustion
        }

        @Override
        public boolean writeOutNext() throws IOException {
            Map.Entry<Object, Object> entry = mapEntries.next();
            valueOut.clear();
            currentKey = compositeKeyBuilder.buildCompositeKeyUserKey(entry.getKey(), userKeySerializer);
            Object userValue = entry.getValue();
            valueOut.writeBoolean(userValue == null);
            userValueSerializer.serialize(userValue, valueOut);
            currentValue = valueOut.getCopyOfBuffer();

            if (!mapEntries.hasNext()) {
                currentStateIterator = parentIterator;
            }
            return true;
        }
    }

    private final class QueueIterator<T> implements SingleStateIterator {
        private final Iterator<T> elementsForKeyGroup;
        private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;
        private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
        private final int afterKeyMark;

        QueueIterator(
                Iterator<T> elementsForKeyGroup,
                RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) throws IOException {
            this.elementsForKeyGroup = elementsForKeyGroup;
            this.metaInfo = metaInfo;
            CompositeKeySerializationUtils.writeKeyGroup(keyGroup(), keyGroupPrefixBytes, keyOut);
            afterKeyMark = keyOut.length();
        }

        @Override
        public boolean hasNext() {
            return elementsForKeyGroup.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            currentValue = EMPTY_BYTE_ARRAY;
            keyOut.setPosition(afterKeyMark);
            T next = elementsForKeyGroup.next();
            metaInfo.getElementSerializer().serialize(next, keyOut);
            currentKey = keyOut.getCopyOfBuffer();
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> castToType(TypeSerializer<?> serializer) {
        return (TypeSerializer<T>) serializer;
    }
}
