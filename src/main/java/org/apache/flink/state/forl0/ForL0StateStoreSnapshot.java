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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Snapshot of a {@link ForL0StateStore} for checkpointing.
 * 
 * <p>This implements {@link IterableStateSnapshot} to work with Flink's
 * standard checkpoint writers like {@link org.apache.flink.runtime.state.FullSnapshotAsyncWriter}.
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreSnapshot<K, N, S> implements IterableStateSnapshot<K, N, S> {

    /** The source store (for snapshot-on-write, we just reference the live data). */
    private final ForL0StateStore<K, N, S> store;

    /** Snapshot of the meta info. */
    private final StateMetaInfoSnapshot metaInfoSnapshot;

    public ForL0StateStoreSnapshot(ForL0StateStore<K, N, S> store) {
        this.store = store;
        this.metaInfoSnapshot = store.getMetaInfo().snapshot();
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot getMetaInfoSnapshot() {
        return metaInfoSnapshot;
    }

    @Nonnull
    @Override
    public StateKeyGroupWriter getKeyGroupWriter() {
        return new ForL0StateKeyGroupWriter();
    }

    @Override
    public void release() {
        // No resources to release - we don't make a copy
    }

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> getIterator(int keyGroupId) {
        Iterable<StateEntry<K, N, S>> entries = store.entries(keyGroupId);
        if (entries == null) {
            return Collections.emptyIterator();
        }
        return entries.iterator();
    }

    /**
     * Gets the key serializer.
     */
    public TypeSerializer<K> getKeySerializer() {
        return store.getKeySerializer();
    }

    /**
     * Gets the namespace serializer.
     */
    public TypeSerializer<N> getNamespaceSerializer() {
        return store.getNamespaceSerializer();
    }

    /**
     * Gets the state serializer.
     */
    public TypeSerializer<S> getStateSerializer() {
        return store.getStateSerializer();
    }

    /**
     * Gets the state name.
     */
    public String getStateName() {
        return store.getStateName();
    }

    /**
     * Writer for key-group data.
     * <p>Uses Flink's standard checkpoint format: namespace -> key -> state order.
     */
    private class ForL0StateKeyGroupWriter implements StateKeyGroupWriter {
        @Override
        public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId)
                throws IOException {
            Iterable<StateEntry<K, N, S>> entries = store.entries(keyGroupId);
            
            // Count entries first
            int count = 0;
            for (@SuppressWarnings("unused") StateEntry<K, N, S> entry : entries) {
                count++;
            }
            
            // Write count and entries (namespace -> key -> state order, same as Flink standard)
            dov.writeInt(count);
            for (StateEntry<K, N, S> entry : entries) {
                store.getNamespaceSerializer().serialize(entry.getNamespace(), dov);
                store.getKeySerializer().serialize(entry.getKey(), dov);
                store.getStateSerializer().serialize(entry.getState(), dov);
            }
        }
    }
}
