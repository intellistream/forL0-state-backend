package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.levelhash.LevelHashStateMap;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    private final MemoryManager memoryManager;

    ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer,
                    MemoryManager memoryManager) {
        super(keyContext, metaInfo, keySerializer);
        this.memoryManager = memoryManager;
    }

    @Override
    protected ForL0StateMap<K, N, S> createStateMap() {
        return new ForL0StateMap<>(
                new MemoryManagerAllocator(memoryManager, this),
                4, // MainTable: 16 buckets
                3, // L0Table: 8 buckets
                getKeySerializer(),
                getNamespaceSerializer(),
                getStateSerializer(),
                true // L0 cache enabled
        );
    }

    @Override
    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        super.setMetaInfo(metaInfo);
        for (StateMap<K, N, S> keyGroupedStateMap : keyGroupedStateMaps) {
            ((CopyOnWriteStateMap<K, N, S>) keyGroupedStateMap)
                    .setStateSerializer(metaInfo.getStateSerializer());
        }
    }

    @Nonnull
    @Override
    public ForL0StateTableSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateTableSnapshot<>(
                this,
                getKeySerializer().duplicate(),
                getNamespaceSerializer().duplicate(),
                getStateSerializer().duplicate(),
                getMetaInfo()
                        .getStateSnapshotTransformFactory()
                        .createForDeserializedState()
                        .orElse(null));
    }

    List<ForL0StateMapSnapshot<K, N, S>> getStateMapSnapshotList() {
        List<ForL0StateMapSnapshot<K, N, S>> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
        for (StateMap<K, N, S> keyGroupedStateMap : keyGroupedStateMaps) {
            snapshotList.add(((LevelHashStateMap<K, N, S>) keyGroupedStateMap).stateSnapshot());
        }
        return snapshotList;
    }
}
