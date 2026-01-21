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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readKey;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readKeyGroup;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readNamespace;

/**
 * Savepoint restore operation for ForL0 state backend.
 * Savepoint shares a common unified binary format across all state backends.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForL0SavepointRestoreOperation<K> implements RestoreOperation<Void> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ForL0SavepointRestoreOperation.class);
    
    private final int keyGroupPrefixBytes;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final FullSnapshotRestoreOperation<K> savepointRestoreOperation;
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    private final KeyGroupRange keyGroupRange;
    private final int numberOfKeyGroups;
    
    // Shared wrappers for deserializing entries
    private final DataInputDeserializer entryKeyDeserializer;
    private final DataInputDeserializer entryValueDeserializer;
    private final ListDelimitedSerializer listDelimitedSerializer;

    public ForL0SavepointRestoreOperation(
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull StateSerializerProvider<K> keySerializerProvider,
            @Nonnull ClassLoader userCodeClassLoader,
            @Nonnull Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            @Nonnull Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            @Nonnull HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        
        this.keySerializerProvider = keySerializerProvider;
        this.registeredStores = registeredStores;
        this.registeredPQStates = registeredPQStates;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
        
        this.keyGroupPrefixBytes = computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
        
        this.savepointRestoreOperation = new FullSnapshotRestoreOperation<>(
                keyGroupRange,
                userCodeClassLoader,
                restoreStateHandles,
                keySerializerProvider);
        
        this.entryKeyDeserializer = new DataInputDeserializer();
        this.entryValueDeserializer = new DataInputDeserializer();
        this.listDelimitedSerializer = new ListDelimitedSerializer();
    }

    @Override
    public Void restore() throws Exception {
        LOG.info("[ForL0] Starting savepoint restore");
        
        registeredStores.clear();
        registeredPQStates.clear();

        try (ThrowingIterator<SavepointRestoreResult> restore = savepointRestoreOperation.restore()) {
            while (restore.hasNext()) {
                SavepointRestoreResult restoreResult = restore.next();
                List<StateMetaInfoSnapshot> restoredMetaInfos = restoreResult.getStateMetaInfoSnapshots();

                final Map<Integer, StateMetaInfoSnapshot> kvStatesById = 
                        createOrCheckStateForMetaInfo(restoredMetaInfos);

                try (ThrowingIterator<KeyGroup> keyGroups = restoreResult.getRestoredKeyGroups()) {
                    while (keyGroups.hasNext()) {
                        readKeyGroupStateData(
                                keyGroups.next(),
                                keySerializerProvider.previousSchemaSerializer(),
                                kvStatesById);
                    }
                }
            }
        }

        LOG.info("[ForL0] Savepoint restore completed");
        return null;
    }

    private Map<Integer, StateMetaInfoSnapshot> createOrCheckStateForMetaInfo(
            List<StateMetaInfoSnapshot> restoredMetaInfos) {
        
        final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();
        
        for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfos) {
            switch (metaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    if (!registeredStores.containsKey(metaInfoSnapshot.getName())) {
                        RegisteredKeyValueStateBackendMetaInfo<?, ?> registeredMetaInfo =
                                new RegisteredKeyValueStateBackendMetaInfo<>(metaInfoSnapshot);
                        @SuppressWarnings({"unchecked", "rawtypes"})
                        ForL0StateStore<K, ?, ?> newStore = new ForL0StateStore(
                                keyGroupRange,
                                keySerializerProvider.currentSchemaSerializer(),
                                registeredMetaInfo);
                        registeredStores.put(metaInfoSnapshot.getName(), newStore);
                    }
                    break;
                case PRIORITY_QUEUE:
                    if (!registeredPQStates.containsKey(metaInfoSnapshot.getName())) {
                        registeredPQStates.put(
                                metaInfoSnapshot.getName(),
                                createPriorityQueueWrapper(
                                        new RegisteredPriorityQueueStateBackendMetaInfo<>(metaInfoSnapshot)));
                    }
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected state type: " + metaInfoSnapshot.getBackendStateType());
            }
            kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
        }
        
        return kvStatesById;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends HeapPriorityQueueElement> HeapPriorityQueueSnapshotRestoreWrapper<T> 
            createPriorityQueueWrapper(RegisteredPriorityQueueStateBackendMetaInfo metaInfo) {
        
        final String stateName = metaInfo.getName();
        final HeapPriorityQueueSet<T> priorityQueue =
                priorityQueueSetFactory.create(stateName, metaInfo.getElementSerializer());
        
        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                metaInfo,
                null, // localKeyGroupRangeWriter not needed for restore
                keyGroupRange,
                numberOfKeyGroups);
    }

    private void readKeyGroupStateData(
            KeyGroup keyGroup,
            TypeSerializer<K> keySerializer,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById) throws Exception {
        
        int keyGroupId = keyGroup.getKeyGroupId();
        LOG.debug("[ForL0] Restoring key group {}", keyGroupId);
        
        try (ThrowingIterator<KeyGroupEntry> entries = keyGroup.getKeyGroupEntries()) {
            while (entries.hasNext()) {
                KeyGroupEntry groupEntry = entries.next();
                StateMetaInfoSnapshot infoSnapshot = kvStatesById.get(groupEntry.getKvStateId());
                
                switch (infoSnapshot.getBackendStateType()) {
                    case KEY_VALUE:
                        readKVStateData(keySerializer, groupEntry, infoSnapshot, keyGroupId);
                        break;
                    case PRIORITY_QUEUE:
                        readPriorityQueue(groupEntry, infoSnapshot);
                        break;
                    default:
                        throw new IllegalStateException(
                                "Expected only keyed state. Received: " + infoSnapshot.getBackendStateType());
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void readKVStateData(
            TypeSerializer<K> keySerializer,
            KeyGroupEntry groupEntry,
            StateMetaInfoSnapshot infoSnapshot,
            int keyGroupId) throws IOException {
        
        ForL0StateStore<K, ?, ?> stateStore = registeredStores.get(infoSnapshot.getName());
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo = stateStore.getMetaInfo();
        TypeSerializer<?> namespaceSerializer = metaInfo.getPreviousNamespaceSerializer();
        TypeSerializer<?> stateSerializer = metaInfo.getPreviousStateSerializer();
        
        boolean isAmbiguousKey = keySerializer.getLength() < 0 && namespaceSerializer.getLength() < 0;
        
        entryKeyDeserializer.setBuffer(groupEntry.getKey());
        entryValueDeserializer.setBuffer(groupEntry.getValue());
        
        // Read key group prefix
        int keyGroup = readKeyGroup(keyGroupPrefixBytes, entryKeyDeserializer);
        
        // Read key
        K key = readKey(keySerializer, entryKeyDeserializer, isAmbiguousKey);
        
        // Read namespace
        Object namespace = readNamespace(namespaceSerializer, entryKeyDeserializer, isAmbiguousKey);
        
        // Read and put value based on state type
        switch (metaInfo.getStateType()) {
            case LIST:
                ((ForL0StateStore) stateStore).put(
                        key, 
                        namespace, 
                        listDelimitedSerializer.deserializeList(
                                groupEntry.getValue(),
                                ((ListSerializer<?>) stateSerializer).getElementSerializer()),
                        keyGroup);
                break;
            case VALUE:
            case REDUCING:
            case FOLDING:
            case AGGREGATING:
                ((ForL0StateStore) stateStore).put(
                        key,
                        namespace,
                        stateSerializer.deserialize(entryValueDeserializer),
                        keyGroup);
                break;
            case MAP:
                // For MAP state, each entry in the savepoint represents a single map entry
                // We need to get or create the map, then add this entry to it
                deserializeMapStateEntry(
                        (ForL0StateStore<K, Object, Map<Object, Object>>) stateStore,
                        keyGroup,
                        key,
                        namespace,
                        (MapSerializer<Object, Object>) stateSerializer);
                break;
            default:
                throw new IllegalStateException("Unknown state type: " + metaInfo.getStateType());
        }
    }

    /**
     * Deserializes a single map entry and adds it to the map state.
     * Aligned with HeapSavepointRestoreOperation.deserializeMapStateEntry().
     */
    private void deserializeMapStateEntry(
            ForL0StateStore<K, Object, Map<Object, Object>> stateStore,
            int keyGroup,
            K key,
            Object namespace,
            MapSerializer<Object, Object> stateSerializer) throws IOException {
        
        Object mapEntryKey = stateSerializer.getKeySerializer().deserialize(entryKeyDeserializer);
        boolean isNull = entryValueDeserializer.readBoolean();
        final Object mapEntryValue;
        if (isNull) {
            mapEntryValue = null;
        } else {
            mapEntryValue = stateSerializer.getValueSerializer().deserialize(entryValueDeserializer);
        }

        Map<Object, Object> userMap = stateStore.get(key, namespace, keyGroup);
        if (userMap == null) {
            userMap = new HashMap<>();
            stateStore.put(key, namespace, userMap, keyGroup);
        }
        userMap.put(mapEntryKey, mapEntryValue);
    }

    @SuppressWarnings("unchecked")
    private void readPriorityQueue(
            KeyGroupEntry groupEntry, 
            StateMetaInfoSnapshot infoSnapshot) throws IOException {
        
        entryKeyDeserializer.setBuffer(groupEntry.getKey());
        entryKeyDeserializer.skipBytesToRead(keyGroupPrefixBytes);
        
        HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> wrapper =
                (HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>)
                        registeredPQStates.get(infoSnapshot.getName());
        
        HeapPriorityQueueElement timer = wrapper.getMetaInfo()
                .getElementSerializer()
                .deserialize(entryKeyDeserializer);
        
        HeapPriorityQueueSet<HeapPriorityQueueElement> priorityQueue = wrapper.getPriorityQueue();
        priorityQueue.add(timer);
    }
}
