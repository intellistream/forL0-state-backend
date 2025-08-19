package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    private static final MemoryManager currentMemoryManager = new ThreadLocal<MemoryManager>().get();
    private static final ThreadLocal<MemoryManager> MEMORY_MANAGER_HOLDER = new ThreadLocal<>();
    // 新增：通过ThreadLocal在构造阶段传递是否启用L0缓存
    private static final ThreadLocal<Boolean> L0_CACHE_ENABLED_HOLDER = new ThreadLocal<>();

    private final MemoryManager memoryManager;
    // 新增：记录是否启用L0缓存
    private final boolean l0CacheEnabled;

    // Private constructor that uses the pre-stored MemoryManager
    private ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
        this.memoryManager = MEMORY_MANAGER_HOLDER.get();
        // 读取并清理开关的ThreadLocal；默认启用
        Boolean enabled = L0_CACHE_ENABLED_HOLDER.get();
        this.l0CacheEnabled = enabled == null ? true : enabled;
        L0_CACHE_ENABLED_HOLDER.remove();
        MEMORY_MANAGER_HOLDER.remove(); // Clean up after use

        if (this.memoryManager == null) {
            throw new IllegalStateException("MemoryManager not found in ThreadLocal. This is a programming error.");
        }
    }

    // Public static factory method（保持兼容，默认启用L0）
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

    // 新增：可显式指定是否启用L0缓存
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            MemoryManager memoryManager,
            boolean l0CacheEnabled) {
        MEMORY_MANAGER_HOLDER.set(memoryManager);
        L0_CACHE_ENABLED_HOLDER.set(l0CacheEnabled);
        try {
            return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
        } catch (Exception e) {
            MEMORY_MANAGER_HOLDER.remove();
            L0_CACHE_ENABLED_HOLDER.remove();
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
                this.l0CacheEnabled // 根据构造时传入的开关决定是否启用L0
        );
    }

    @Override
    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        // 仅更新元信息，ForL0StateMap 内部序列化器为构造时冻结，避免错误的类型强转
        super.setMetaInfo(metaInfo);
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
            snapshotList.add(((ForL0StateMap<K, N, S>) keyGroupedStateMap).stateSnapshot());
        }
        return snapshotList;
    }
}
