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

import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;

import javax.annotation.Nonnull;

/**
 * Lightweight KeyContext with public fields for direct access.
 *
 * <p>This class eliminates virtual method call overhead on the hot path by exposing
 * {@link #currentKey} and {@link #currentKeyGroupIndex} as public fields that can be
 * accessed directly without method invocation.
 *
 * <p>It implements {@link InternalKeyContext} for compatibility with Flink's
 * {@link org.apache.flink.runtime.state.AbstractKeyedStateBackend}, but ForL0 State
 * implementations access the public fields directly instead of using the interface methods.
 *
 * @param <K> The type of the key.
 */
public final class ForL0KeyContext<K> implements InternalKeyContext<K> {

    // ===== Public fields for direct access (hot path optimization) =====
    
    /** The currently active key. Directly accessed by State implementations. */
    public K currentKey;
    
    /** The key group of the currently active key. Directly accessed by State implementations. */
    public int currentKeyGroupIndex;

    // ===== Immutable configuration =====
    
    /** Range of key-groups for which this backend is responsible. */
    public final KeyGroupRange keyGroupRange;
    
    /** The number of key-groups aka max parallelism. */
    public final int numberOfKeyGroups;

    /**
     * Creates a new ForL0KeyContext.
     *
     * @param keyGroupRange the key group range
     * @param numberOfKeyGroups the total number of key groups
     */
    public ForL0KeyContext(@Nonnull KeyGroupRange keyGroupRange, int numberOfKeyGroups) {
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
    }

    // ===== InternalKeyContext interface implementation (for framework compatibility) =====

    @Override
    public K getCurrentKey() {
        return currentKey;
    }

    @Override
    public int getCurrentKeyGroupIndex() {
        return currentKeyGroupIndex;
    }

    @Override
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public void setCurrentKey(@Nonnull K currentKey) {
        this.currentKey = currentKey;
    }

    @Override
    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        if (!keyGroupRange.contains(currentKeyGroupIndex)) {
            throw KeyGroupRangeOffsets.newIllegalKeyGroupException(
                    currentKeyGroupIndex, keyGroupRange);
        }
        this.currentKeyGroupIndex = currentKeyGroupIndex;
    }
}
