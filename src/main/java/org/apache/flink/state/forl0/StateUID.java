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

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;

import java.util.Objects;

/**
 * A unique identifier for a state consisting of state name and type.
 * <p>
 * This class wraps state identification used in checkpoint/restore operations.
 * It uses Flink's {@link BackendStateType} internally for compatibility.
 */
public class StateUID {

    private final String stateName;
    private final BackendStateType stateType;

    private StateUID(String stateName, BackendStateType stateType) {
        this.stateName = stateName;
        this.stateType = stateType;
    }

    public static StateUID of(String stateName, BackendStateType stateType) {
        return new StateUID(stateName, stateType);
    }

    public String getStateName() {
        return stateName;
    }

    public BackendStateType getStateType() {
        return stateType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateUID stateUID = (StateUID) o;
        return Objects.equals(stateName, stateUID.stateName) && stateType == stateUID.stateType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateName, stateType);
    }

    @Override
    public String toString() {
        return "StateUID{" + "stateName='" + stateName + '\'' + ", stateType=" + stateType + '}';
    }
}
