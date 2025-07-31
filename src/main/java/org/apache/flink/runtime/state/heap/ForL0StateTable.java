package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.levelhash.LevelHashStateMap;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    private static final MemoryManager currentMemoryManager = new ThreadLocal<MemoryManager>().get();
    private static final ThreadLocal<MemoryManager> MEMORY_MANAGER_HOLDER = new ThreadLocal<>();

    private final MemoryManager memoryManager;

    // Private constructor that uses the pre-stored MemoryManager
    private ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
        this.memoryManager = MEMORY_MANAGER_HOLDER.get();
        MEMORY_MANAGER_HOLDER.remove(); // Clean up after use

        if (this.memoryManager == null) {
            throw new IllegalStateException("MemoryManager not found in ThreadLocal. This is a programming error.");
        }
    }

    // Public static factory method
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            MemoryManager memoryManager) {

        // Store the MemoryManager in ThreadLocal before calling constructor
        MEMORY_MANAGER_HOLDER.set(memoryManager);
        try {
            return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
        } catch (Exception e) {
            MEMORY_MANAGER_HOLDER.remove(); // Clean up on error
            throw e;
        }
    }

    @Override
    protected ForL0StateMap<K, N, S> createStateMap() {
        // Now memoryManager is guaranteed to be available
        MemoryManager mm = MEMORY_MANAGER_HOLDER.get();
        if (mm == null) {
            mm = this.memoryManager; // Fallback to instance field if available
        }

        if (mm == null) {
            throw new IllegalStateException("MemoryManager is not available in createStateMap()");
        }

        return new ForL0StateMap<>(
                new MemoryManagerAllocator(mm, this),
                10, // MainTable
                10, // L0Table
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
