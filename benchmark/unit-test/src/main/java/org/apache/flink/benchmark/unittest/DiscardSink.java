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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * A sink that discards all input and prints summary statistics on close.
 */
public class DiscardSink extends RichSinkFunction<Long> {

    private static final long serialVersionUID = 1L;

    private transient long count;
    private transient long startTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        count = 0;
        startTime = System.currentTimeMillis();
    }

    @Override
    public void invoke(Long value, Context context) throws Exception {
        count++;
    }

    @Override
    public void close() throws Exception {
        long elapsed = System.currentTimeMillis() - startTime;
        double opsPerSec = elapsed > 0 ? count * 1000.0 / elapsed : 0;
        
        System.out.println("===========================");
        System.out.printf("Completed %d operations in %d ms%n", count, elapsed);
        System.out.printf("Throughput: %.0f ops/sec%n", opsPerSec);
        System.out.println("===========================");
        
        super.close();
    }
}
