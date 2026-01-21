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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/**
 * Restore operation for ForL0 state backend (checkpoint format).
 * 
 * <p>This implementation is aligned with HeapRestoreOperation for performance and correctness.
 *
 * @param <K> The type of key
 */
public class ForL0RestoreOperation<K> implements RestoreOperation<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0RestoreOperation.class);

    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final ClassLoader userCodeClassLoader;
    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final CloseableRegistry cancelStreamRegistry;
    @Nonnull private final KeyGroupRange keyGroupRange;
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    private final int numberOfKeyGroups;

    public ForL0RestoreOperation(
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull StateSerializerProvider<K> keySerializerProvider,
            @Nonnull ClassLoader userCodeClassLoader,
            @Nonnull Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            @Nonnull Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        this.restoreStateHandles = restoreStateHandles;
        this.keySerializerProvider = keySerializerProvider;
        this.userCodeClassLoader = userCodeClassLoader;
        this.registeredStores = registeredStores;
        this.registeredPQStates = registeredPQStates;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
    }

    @Override
    public Void restore() throws Exception {
        LOG.info("[ForL0] Starting checkpoint restore from {} handles", restoreStateHandles.size());

        registeredStores.clear();
        registeredPQStates.clear();

        boolean keySerializerRestored = false;

        for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
            if (keyedStateHandle == null) {
                continue;
            }

            if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                throw unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, keyedStateHandle.getClass());
            }

            LOG.info("[ForL0] Restoring from state handle: {}", keyedStateHandle);
            KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
            FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(fsDataInputStream);

            try {
                DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

                KeyedBackendSerializationProxy<K> serializationProxy =
                        new KeyedBackendSerializationProxy<>(userCodeClassLoader);
                serializationProxy.read(inView);

                if (!keySerializerRestored) {
                    // Check key serializer compatibility
                    TypeSerializer<K> currentSerializer = keySerializerProvider.currentSchemaSerializer();
                    TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
                            keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                                    serializationProxy.getKeySerializerSnapshot());
                    if (keySerializerSchemaCompat.isCompatibleAfterMigration()
                            || keySerializerSchemaCompat.isIncompatible()) {
                        throw new StateMigrationException(
                                "The new key serializer (" + currentSerializer + 
                                ") must be compatible with the previous key serializer (" +
                                keySerializerProvider.previousSchemaSerializer() + ").");
                    }
                    keySerializerRestored = true;
                }

                List<StateMetaInfoSnapshot> restoredMetaInfos =
                        serializationProxy.getStateMetaInfoSnapshots();

                final Map<Integer, StateMetaInfoSnapshot> kvStatesById =
                        createOrCheckStateForMetaInfo(restoredMetaInfos);

                readStateHandleStateData(
                        fsDataInputStream,
                        inView,
                        keyGroupsStateHandle.getGroupRangeOffsets(),
                        kvStatesById,
                        restoredMetaInfos.size(),
                        serializationProxy.getReadVersion(),
                        serializationProxy.isUsingKeyGroupCompression());
                        
                LOG.info("[ForL0] Finished restoring from state handle: {}", keyedStateHandle);
            } finally {
                if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
                    IOUtils.closeQuietly(fsDataInputStream);
                }
            }
        }

        LOG.info("[ForL0] Checkpoint restore completed");
        return null;
    }

    /**
     * Creates or checks state stores and priority queues for the given meta info snapshots.
     * Aligned with HeapMetaInfoRestoreOperation.createOrCheckStateForMetaInfo().
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<Integer, StateMetaInfoSnapshot> createOrCheckStateForMetaInfo(
            List<StateMetaInfoSnapshot> restoredMetaInfo) {
        
        final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();
        
        for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfo) {
            final StateSnapshotRestore registeredState;

            switch (metaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    registeredState = registeredStores.get(metaInfoSnapshot.getName());
                    if (registeredState == null) {
                        RegisteredKeyValueStateBackendMetaInfo<?, ?> registeredMetaInfo =
                                new RegisteredKeyValueStateBackendMetaInfo<>(metaInfoSnapshot);
                        registeredStores.put(
                                metaInfoSnapshot.getName(),
                                new ForL0StateStore(
                                        keyGroupRange,
                                        keySerializerProvider.currentSchemaSerializer(),
                                        registeredMetaInfo));
                    }
                    break;
                case PRIORITY_QUEUE:
                    registeredState = registeredPQStates.get(metaInfoSnapshot.getName());
                    if (registeredState == null) {
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
    private <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            HeapPriorityQueueSnapshotRestoreWrapper<T> createPriorityQueueWrapper(
                    RegisteredPriorityQueueStateBackendMetaInfo metaInfo) {
        
        final String stateName = metaInfo.getName();
        final HeapPriorityQueueSet<T> priorityQueue =
                priorityQueueSetFactory.create(stateName, metaInfo.getElementSerializer());

        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                metaInfo,
                KeyExtractorFunction.forKeyedObjects(),
                keyGroupRange,
                numberOfKeyGroups);
    }

    private void readStateHandleStateData(
            FSDataInputStream fsDataInputStream,
            DataInputViewStreamWrapper inView,
            KeyGroupRangeOffsets keyGroupOffsets,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById,
            int numStates,
            int readVersion,
            boolean isCompressed) throws IOException {

        final StreamCompressionDecorator streamCompressionDecorator =
                isCompressed
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;

        for (Tuple2<Integer, Long> groupOffset : keyGroupOffsets) {
            int keyGroupIndex = groupOffset.f0;
            long offset = groupOffset.f1;

            if (!keyGroupRange.contains(keyGroupIndex)) {
                LOG.debug("[ForL0] Key group {} doesn't belong to this backend with range: {}",
                        keyGroupIndex, keyGroupRange);
                continue;
            }

            LOG.debug("[ForL0] Restoring key group {} at offset {}", keyGroupIndex, offset);
            fsDataInputStream.seek(offset);

            int writtenKeyGroupIndex = inView.readInt();
            LOG.debug("[ForL0] Read key group index {} (expected {})", writtenKeyGroupIndex, keyGroupIndex);
            Preconditions.checkState(
                    writtenKeyGroupIndex == keyGroupIndex, 
                    "Unexpected key-group in restore: expected %s but got %s", 
                    keyGroupIndex, writtenKeyGroupIndex);

            try (InputStream kgCompressionInStream =
                    streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {
                readKeyGroupStateData(
                        kgCompressionInStream, kvStatesById, keyGroupIndex, numStates, readVersion);
            }
        }
    }

    private void readKeyGroupStateData(
            InputStream inputStream,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById,
            int keyGroupIndex,
            int numStates,
            int readVersion) throws IOException {

        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inputStream);

        for (int i = 0; i < numStates; i++) {
            final int kvStateId = inView.readShort();
            final StateMetaInfoSnapshot stateMetaInfoSnapshot = kvStatesById.get(kvStateId);
            final StateSnapshotRestore registeredState;

            switch (stateMetaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    registeredState = registeredStores.get(stateMetaInfoSnapshot.getName());
                    break;
                case PRIORITY_QUEUE:
                    registeredState = registeredPQStates.get(stateMetaInfoSnapshot.getName());
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected state type: " + stateMetaInfoSnapshot.getBackendStateType());
            }

            StateSnapshotKeyGroupReader keyGroupReader = registeredState.keyGroupReader(readVersion);
            keyGroupReader.readMappingsInKeyGroup(inView, keyGroupIndex);
        }
    }
}
