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

package org.apache.flink.benchmark.unittest;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

/**
 * A stateful map function that performs a fixed operation: read + write.
 * 
 * <p>For each input key, this function:
 * <ol>
 *   <li>Reads the current state value (GET)</li>
 *   <li>Writes a new state value (PUT)</li>
 * </ol>
 * 
 * <p>The state is a byte array of configurable size, simulating arbitrary state payloads.
 */
public class StatefulMapFunction extends RichMapFunction<Integer, Integer> {

    private static final long serialVersionUID = 1L;

    private final int stateSize;

    private transient ValueState<byte[]> state;
    private transient byte[] valueBuffer;

    /**
     * Creates a stateful map function.
     *
     * @param stateSize size of the state value in bytes
     */
    public StatefulMapFunction(int stateSize) {
        this.stateSize = stateSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>(
                "state",
                TypeInformation.of(new TypeHint<byte[]>() {}));
        state = getRuntimeContext().getState(descriptor);
        
        // Pre-allocate value buffer to avoid GC during benchmark
        valueBuffer = new byte[stateSize];
        for (int i = 0; i < stateSize; i++) {
            valueBuffer[i] = (byte) (i & 0xFF);
        }
    }

    @Override
    public Integer map(Integer keyIndex) throws Exception {
        // Fixed operation: GET + PUT
        byte[] oldValue = state.value();  // GET (may return null for new keys)
        state.update(valueBuffer);         // PUT
        
        return keyIndex;
    }
}
