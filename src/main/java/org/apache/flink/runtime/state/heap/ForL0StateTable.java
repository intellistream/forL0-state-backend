package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    @SuppressWarnings("unused")
    private static final MemoryManager currentMemoryManager = new ThreadLocal<MemoryManager>().get();  // Actually this is used by reflection
    private static final ThreadLocal<MemoryManager> MEMORY_MANAGER_HOLDER = new ThreadLocal<>();
    // 通过ThreadLocal在构造阶段传递是否启用L0缓存
    private static final ThreadLocal<Boolean> L0_CACHE_ENABLED_HOLDER = new ThreadLocal<>();
    // 通过ThreadLocal传递共享的L0Allocator
    private static final ThreadLocal<L0MemoryAllocator> L0_ALLOCATOR_HOLDER = new ThreadLocal<>();

    private final MemoryManager memoryManager;
    // 记录是否启用L0缓存
    private final boolean l0CacheEnabled;
    // 共享的L0Allocator（由Backend提供，所有StateTable共享）
    @Nullable
    private final L0MemoryAllocator sharedL0Allocator;

    // Private constructor that uses the pre-stored MemoryManager
    private ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
        this.memoryManager = MEMORY_MANAGER_HOLDER.get();
        // 读取并清理开关的ThreadLocal；默认启用
        Boolean enabled = L0_CACHE_ENABLED_HOLDER.get();
        this.l0CacheEnabled = enabled == null || enabled;
        // 读取共享的L0Allocator
        this.sharedL0Allocator = L0_ALLOCATOR_HOLDER.get();
        
        // Clean up ThreadLocals
        L0_CACHE_ENABLED_HOLDER.remove();
        L0_ALLOCATOR_HOLDER.remove();
        MEMORY_MANAGER_HOLDER.remove();

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

    // 可显式指定是否启用L0缓存
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
    
    /**
     * Factory method with shared L0Allocator.
     * This is the preferred method when L0 cache is enabled at Backend level.
     * All StateTables created by the same Backend should share the same L0Allocator.
     *
     * @param keyContext Key context
     * @param metaInfo Metadata info
     * @param keySerializer Key serializer
     * @param memoryManager Flink memory manager for MainTable
     * @param l0CacheEnabled Whether L0 cache is enabled
     * @param sharedL0Allocator The shared L0 allocator (may be null if L0 disabled)
     */
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            MemoryManager memoryManager,
            boolean l0CacheEnabled,
            @Nullable L0MemoryAllocator sharedL0Allocator) {
        MEMORY_MANAGER_HOLDER.set(memoryManager);
        L0_CACHE_ENABLED_HOLDER.set(l0CacheEnabled);
        L0_ALLOCATOR_HOLDER.set(sharedL0Allocator);
        try {
            return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
        } catch (Exception e) {
            MEMORY_MANAGER_HOLDER.remove();
            L0_CACHE_ENABLED_HOLDER.remove();
            L0_ALLOCATOR_HOLDER.remove();
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

        // Use the shared L0Allocator from Backend level
        // sharedL0Allocator is null if L0 cache is disabled
        L0MemoryAllocator l0Allocator = this.l0CacheEnabled ? this.sharedL0Allocator : null;

        return new ForL0StateMap<>(
                new MemoryManagerAllocator(mm, this),
                l0Allocator,  // L0 memory allocator (null if L0 disabled)
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
