package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * ForL0 implementation of StateTable using SwissMap architecture.
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
        return new ForL0StateMap<>();
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
