package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * ForL0 implementation of StateTable using SwissMap architecture.
 * 
 * <p>Automatically selects the optimal SwissTable specialization based on
 * key and namespace types for maximum performance.
 */
public class ForL0StateTable<K, N, S> extends StateTable<K, N, S> {

    private ForL0StateTable(InternalKeyContext<K> keyContext,
                    RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
                    TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
    }

    /**
     * Factory method to create a ForL0StateTable.
     */
    public static <K, N, S> ForL0StateTable<K, N, S> create(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        return new ForL0StateTable<>(keyContext, metaInfo, keySerializer);
    }

    @Override
    protected ForL0StateMap<K, N, S> createStateMap() {
        // Resolve types from parent's fields (already initialized in super constructor)
        Class<?> keyClass = resolveKeyClass(keySerializer);
        Class<?> nsClass = resolveNamespaceClass(metaInfo.getNamespaceSerializer());
        return new ForL0StateMap<>(keyClass, nsClass);
    }

    /**
     * Resolves the key class from the serializer.
     */
    private static Class<?> resolveKeyClass(TypeSerializer<?> serializer) {
        if (serializer instanceof LongSerializer) {
            return Long.class;
        }
        if (serializer instanceof StringSerializer) {
            return String.class;
        }
        return Object.class;  // fallback to generic
    }

    /**
     * Resolves the namespace class from the serializer.
     */
    private static Class<?> resolveNamespaceClass(TypeSerializer<?> serializer) {
        if (serializer instanceof VoidNamespaceSerializer) {
            return VoidNamespace.class;
        }
        // TimeWindow.Serializer is inner class, check enclosing class or full name
        if (serializer != null) {
            Class<?> serializerClass = serializer.getClass();
            // Check if it's TimeWindow$Serializer (inner class of TimeWindow)
            Class<?> enclosing = serializerClass.getEnclosingClass();
            if (enclosing != null && enclosing == TimeWindow.class) {
                return TimeWindow.class;
            }
            // Fallback: check full class name
            if (serializerClass.getName().contains("TimeWindow")) {
                return TimeWindow.class;
            }
        }
        return Object.class;  // fallback to generic
    }

    @Override
    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
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
