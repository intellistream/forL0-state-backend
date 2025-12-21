package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    // 通过ThreadLocal传递配置对象
    private static final ThreadLocal<ForL0StateBackendConfig> CONFIG_HOLDER = new ThreadLocal<>();
    // 通过ThreadLocal传递共享的L0Allocator
    private static final ThreadLocal<L0MemoryAllocator> L0_ALLOCATOR_HOLDER = new ThreadLocal<>();

    // ForL0 configuration
    private final ForL0StateBackendConfig config;
    // 共享的L0Allocator（由Backend提供，所有StateTable共享）
    @Nullable
    private final L0MemoryAllocator sharedL0Allocator;

    // Private constructor that uses the pre-stored values from ThreadLocal
    private ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
        // 读取配置对象，如果没有则使用默认配置
        ForL0StateBackendConfig cfg = CONFIG_HOLDER.get();
        this.config = cfg != null ? cfg : new ForL0StateBackendConfig();
        // 读取共享的L0Allocator
        this.sharedL0Allocator = L0_ALLOCATOR_HOLDER.get();
        
        // Clean up ThreadLocals
        CONFIG_HOLDER.remove();
        L0_ALLOCATOR_HOLDER.remove();
    }

    // Public static factory method（保持兼容，默认启用L0）
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {

        try {
            return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
        } catch (Exception e) {
            CONFIG_HOLDER.remove();
            L0_ALLOCATOR_HOLDER.remove();
            throw e;
        }
    }

    /**
     * Factory method with explicit l0CacheEnabled flag.
     * Kept for backward compatibility.
     */
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            boolean l0CacheEnabled) {
        ForL0StateBackendConfig cfg = l0CacheEnabled 
            ? new ForL0StateBackendConfig() 
            : new ForL0StateBackendConfig().withL0CacheDisabled();
        return create(keyContext, metaInfo, keySerializer, cfg, null);
    }
    
    /**
     * Factory method with shared L0Allocator.
     * Kept for backward compatibility.
     */
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            boolean l0CacheEnabled,
            @Nullable L0MemoryAllocator sharedL0Allocator) {
        ForL0StateBackendConfig cfg = l0CacheEnabled 
            ? new ForL0StateBackendConfig() 
            : new ForL0StateBackendConfig().withL0CacheDisabled();
        return create(keyContext, metaInfo, keySerializer, cfg, sharedL0Allocator);
    }

    /**
     * Factory method with full configuration.
     * This is the preferred method for production use.
     *
     * @param keyContext Key context
     * @param metaInfo Metadata info
     * @param keySerializer Key serializer
     * @param config ForL0 StateBackend configuration
     * @param sharedL0Allocator The shared L0 allocator (may be null if L0 disabled)
     */
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            ForL0StateBackendConfig config,
            @Nullable L0MemoryAllocator sharedL0Allocator) {
        CONFIG_HOLDER.set(config);
        L0_ALLOCATOR_HOLDER.set(sharedL0Allocator);
        try {
            return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
        } catch (Exception e) {
            CONFIG_HOLDER.remove();
            L0_ALLOCATOR_HOLDER.remove();
            throw e;
        }
    }

    @Override
    protected ForL0StateMap<K, N, S> createStateMap() {
        // Note: This method is called from parent constructor, before our fields are initialized.
        // So we MUST read from ThreadLocal, not from instance fields.

        // Get config from ThreadLocal, fallback to instance field, then to default
        ForL0StateBackendConfig cfg = CONFIG_HOLDER.get();
        if (cfg == null) {
            cfg = this.config; // Fallback to instance field if available
        }
        if (cfg == null) {
            cfg = new ForL0StateBackendConfig(); // Fallback to default config
        }

        // Get L0Allocator from ThreadLocal
        L0MemoryAllocator l0Allocator = L0_ALLOCATOR_HOLDER.get();
        if (l0Allocator == null) {
            l0Allocator = this.sharedL0Allocator; // Fallback to instance field
        }

        // Use the shared L0Allocator from Backend level
        // l0Allocator is null if L0 cache is disabled
        boolean l0CacheEnabled = cfg.isL0CacheEnabled();
        L0MemoryAllocator effectiveL0Allocator = l0CacheEnabled ? l0Allocator : null;

        return new ForL0StateMap<>(
                effectiveL0Allocator,  // L0 memory allocator (null if L0 disabled)
                cfg.getMainTableInitialSize(),     // MainTable initial size from config
                cfg.getL0CacheSize(),              // L0Table size from config
                getKeySerializer(),
                getNamespaceSerializer(),
                getStateSerializer(),
                l0CacheEnabled,
                cfg.getL0ReplacementPolicy(),       // Replacement policy from config
                cfg.getMainTableLoadFactorThreshold() // Load factor from config
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
