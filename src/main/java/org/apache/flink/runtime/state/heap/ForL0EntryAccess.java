package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

/**
 * **Deprecated** A util class for convenient off-heap state map entry access
 */
final class ForL0EntryAccess {
    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    // Offsets k=key n=namespace v=value
    static final int HASH = 0, KL = 4, NL = 8, VL = 12, NEXT = 16, HEADER = 24;

    static int hash(long addr) {return UNSAFE.getInt(addr + HASH);}
    static void hash(long addr, int value) {UNSAFE.putInt(addr + HASH, value);}

    static int kl(long addr) {return UNSAFE.getInt(addr + KL);}
    static void kl(long addr, int value) {UNSAFE.putInt(addr + KL, value);}
    static int nl(long addr) {return UNSAFE.getInt(addr + NL);}
    static void nl(long addr, int value) {UNSAFE.putInt(addr + NL, value);}
    static int vl(long addr) {return UNSAFE.getInt(addr + VL);}
    static void vl(long addr, int value) {UNSAFE.putInt(addr + VL, value);}

    static long next(long addr) {return UNSAFE.getLong(addr + NEXT);}
    static void next(long addr, long value) {UNSAFE.putLong(addr + NEXT, value);}

    // For comparison

    static boolean equalKN(long addr, byte[] k, byte[] n) {
        if (k.length != kl(addr) || n.length != nl(addr)) return false;
        long p = addr + HEADER;
        if (!equalBytes(p, k)) return false;
        return equalBytes(p + k.length, n);
    }

    private static boolean equalBytes(long addr, byte[] src) {
        long base = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        for (int i = 0; i < src.length; i++) {
            if (UNSAFE.getByte(addr+i) != UNSAFE.getByte(src, base+i)) return false;
        }
        return true;
    }
}
