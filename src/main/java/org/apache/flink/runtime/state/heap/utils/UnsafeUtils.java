package org.apache.flink.runtime.state.heap.space;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

final public class UnsafeUtils {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to obtain sun.misc.Unsafe", e);
        }
    }

    private UnsafeUtils() {}

    static Unsafe unsafe() {
        return UNSAFE;
    }
}
