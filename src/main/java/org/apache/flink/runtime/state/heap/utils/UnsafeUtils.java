package org.apache.flink.runtime.state.heap.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Utility class to access sun.misc.Unsafe.
 * This is intentionally used for off-heap memory operations in ForL0 backend.
 */
@SuppressWarnings("restriction")
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

    public static Unsafe unsafe() {
        return UNSAFE;
    }
}
