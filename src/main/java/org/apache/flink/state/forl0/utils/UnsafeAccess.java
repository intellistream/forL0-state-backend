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

package org.apache.flink.state.forl0.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Utility class providing global access to {@link Unsafe} instance.
 * 
 * <p>This avoids repeated initialization of Unsafe in multiple classes
 * and provides a single point of access for all low-level memory operations.
 */
@SuppressWarnings("restriction")
public final class UnsafeAccess {

    /** Global Unsafe instance. */
    public static final Unsafe UNSAFE;

    /** Base offset for byte arrays. */
    public static final long BYTE_ARRAY_BASE_OFFSET;

    /** Base offset for Object arrays. */
    public static final long OBJECT_ARRAY_BASE_OFFSET;

    /** Scale (element size) for Object arrays. */
    public static final int OBJECT_ARRAY_INDEX_SCALE;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            OBJECT_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(Object[].class);
            OBJECT_ARRAY_INDEX_SCALE = UNSAFE.arrayIndexScale(Object[].class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Unsafe instance", e);
        }
    }

    private UnsafeAccess() {
        // Utility class, no instantiation
    }
}
