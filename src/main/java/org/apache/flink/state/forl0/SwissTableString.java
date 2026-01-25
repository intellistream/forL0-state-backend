package org.apache.flink.state.forl0;

import org.apache.flink.state.forl0.utils.UnsafeAccess;

import sun.misc.Unsafe;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Specialized SwissTable for String keys.
 * 
 * <p>This implementation uses strong-typed String[] array instead of Object[],
 * avoiding checkcast in JIT compiled code:
 * <ul>
 *   <li>keys[]: String array for direct reference access</li>
 *   <li>values[]: Object array for state values</li>
 *   <li>String.equals() is JIT-friendly and often inlined</li>
 * </ul>
 * 
 * <p>Note: String keys still involve Pointer Chasing for char[] comparison,
 * but strong typing eliminates checkcast overhead and enables better JIT optimization.
 * 
 * @param <S> state type
 */
@SuppressWarnings("restriction")
public class SwissTableString<S> {

    // ========== Return value encoding for put() ==========
    public static final int NEW_FLAG = 1 << 16;
    public static final int SLOT_MASK = 0xFFFF;
    public static final int NEED_REHASH = -1;
    public static final int NEED_GROW = -2;

    // ========== Control byte values ==========
    private static final byte CTRL_EMPTY = (byte) 0x80;
    private static final byte CTRL_DELETED = (byte) 0xFE;

    // ========== SWAR constants ==========
    private static final long LSB = 0x0101010101010101L;
    private static final long MSB = 0x8080808080808080L;

    // ========== Unsafe for fast group loading ==========
    private static final Unsafe UNSAFE = UnsafeAccess.UNSAFE;
    private static final long BYTE_ARRAY_BASE_OFFSET = UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    // ========== Counts ==========
    int used;
    int tomb;
    int capacity;
    int growthLeft;
    int groupMask;

    // ========== Storage ==========
    byte[] ctrl;
    String[] keys;      // Strong-typed String array
    Object[] values;
    int[] hashes;

    public SwissTableString(int slotCount) {
        if (slotCount < 8 || (slotCount & (slotCount - 1)) != 0 || (slotCount & 7) != 0) {
            throw new IllegalArgumentException("slotCount must be a power of 2 and >= 8");
        }
        this.capacity = slotCount;
        this.groupMask = (slotCount >>> 3) - 1;
        this.ctrl = new byte[slotCount];
        Arrays.fill(this.ctrl, CTRL_EMPTY);
        this.keys = new String[slotCount];
        this.values = new Object[slotCount];
        this.hashes = new int[slotCount];
        this.used = 0;
        this.tomb = 0;
        this.growthLeft = maxOcc(slotCount);
    }

    long loadCtrlWord(int groupIdx) {
        return UNSAFE.getLong(ctrl, BYTE_ARRAY_BASE_OFFSET + ((long) groupIdx << 3));
    }

    @SuppressWarnings("unchecked")
    public S get(int hash, String key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                // Direct String.equals() - JIT can inline this
                if (key.equals(keys[slot])) {
                    return (S) values[slot];
                }
            }

            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    public int put(int hash, String key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;
        int firstDeletedSlot = -1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (key.equals(keys[slot])) {
                    return slot;
                }
            }

            if (firstDeletedSlot == -1) {
                long delMask = matchDeleted(ctrlWord);
                if (delMask != 0) {
                    firstDeletedSlot = base + laneFromTz(Long.numberOfTrailingZeros(delMask));
                }
            }

            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int emptySlot = base + laneFromTz(Long.numberOfTrailingZeros(emptyMask));

                int insertSlot;
                boolean useDeleted;

                if (firstDeletedSlot != -1) {
                    insertSlot = firstDeletedSlot;
                    useDeleted = true;
                } else {
                    insertSlot = emptySlot;
                    useDeleted = false;

                    if (growthLeft == 0) {
                        if (tomb > 0) {
                            return NEED_REHASH;
                        }
                        return NEED_GROW;
                    }
                }

                keys[insertSlot] = key;
                ctrl[insertSlot] = (byte) (hash & 0x7F);
                hashes[insertSlot] = hash;
                used++;

                if (useDeleted) {
                    tomb--;
                } else {
                    growthLeft--;
                }

                return insertSlot | NEW_FLAG;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @SuppressWarnings("unchecked")
    public S remove(int hash, String key) {
        long h2Pattern = LSB * (hash & 0x7FL);  // h2 = hash & 0x7F, broadcast to 8 lanes
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            int base = group << 3;

            for (long match = matchH2(ctrlWord, h2Pattern); match != 0; match &= match - 1) {
                int slot = base + laneFromTz(Long.numberOfTrailingZeros(match));
                if (key.equals(keys[slot])) {
                    boolean hasEmpty = matchEmpty(ctrlWord) != 0;

                    S oldValue = (S) values[slot];
                    keys[slot] = null;
                    values[slot] = null;

                    if (hasEmpty) {
                        ctrl[slot] = CTRL_EMPTY;
                        used--;
                        growthLeft++;
                    } else {
                        ctrl[slot] = CTRL_DELETED;
                        used--;
                        tomb++;
                    }

                    return oldValue;
                }
            }

            if (matchEmpty(ctrlWord) != 0) {
                return null;
            }

            group = (group + stride) & groupMask;
            stride++;
        }
    }

    void putDirect(int hash, String key, S value) {
        int group = (hash >>> 7) & groupMask;   // h1 = hash >>> 7
        int stride = 1;

        while (true) {
            long ctrlWord = loadCtrlWord(group);
            long emptyMask = matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int slot = (group << 3) + laneFromTz(Long.numberOfTrailingZeros(emptyMask));
                keys[slot] = key;
                values[slot] = value;
                ctrl[slot] = (byte) (hash & 0x7F);  // h2 = hash & 0x7F
                hashes[slot] = hash;
                used++;
                growthLeft--;
                return;
            }
            group = (group + stride) & groupMask;
            stride++;
        }
    }

    @SuppressWarnings("unchecked")
    public void rehash() {
        int cap = capacity;
        SwissTableString<S> newTable = new SwissTableString<>(cap);

        for (int i = 0; i < cap; i++) {
            if (isFull(ctrl[i])) {
                newTable.putDirect(hashes[i], keys[i], (S) values[i]);
            }
        }

        this.ctrl = newTable.ctrl;
        this.keys = newTable.keys;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(cap) - used;
    }

    @SuppressWarnings("unchecked")
    public void grow() {
        int newCapacity = capacity * 2;
        SwissTableString<S> newTable = new SwissTableString<>(newCapacity);

        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                newTable.putDirect(hashes[i], keys[i], (S) values[i]);
            }
        }

        this.capacity = newCapacity;
        this.groupMask = newTable.groupMask;
        this.ctrl = newTable.ctrl;
        this.keys = newTable.keys;
        this.values = newTable.values;
        this.hashes = newTable.hashes;
        this.tomb = 0;
        this.growthLeft = maxOcc(newCapacity) - used;
    }

    public int size() {
        return used;
    }

    public boolean isEmpty() {
        return used == 0;
    }

    public int getCapacity() {
        return capacity;
    }

    public boolean containsKey(int hash, String key) {
        return get(hash, key) != null;
    }

    public void collectKeys(java.util.List<String> result) {
        for (int i = 0; i < capacity; i++) {
            if (isFull(ctrl[i])) {
                result.add(keys[i]);
            }
        }
    }

    public Iterator<Entry<S>> iterator() {
        return new EntryIterator();
    }

    public static class Entry<S> {
        private final String key;
        private final S state;

        Entry(String key, S state) {
            this.key = key;
            this.state = state;
        }

        public String getKey() { return key; }
        public S getState() { return state; }
    }

    private class EntryIterator implements Iterator<Entry<S>> {
        private int nextIndex = 0;

        EntryIterator() {
            advanceToNext();
        }

        private void advanceToNext() {
            while (nextIndex < capacity && !isFull(ctrl[nextIndex])) {
                nextIndex++;
            }
        }

        @Override
        public boolean hasNext() {
            return nextIndex < capacity;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<S> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String key = keys[nextIndex];
            S value = (S) values[nextIndex];
            nextIndex++;
            advanceToNext();
            return new Entry<>(key, value);
        }
    }

    // ========== SWAR Matching ==========

    private static long matchH2(long ctrlWord, long pattern) {
        long x = ctrlWord ^ pattern;
        return (x - LSB) & ~x & MSB;
    }

    private static long matchEmpty(long ctrlWord) {
        return (ctrlWord & ~(ctrlWord << 6)) & MSB;
    }

    private static long matchDeleted(long ctrlWord) {
        return (ctrlWord & (ctrlWord << 6)) & MSB;
    }

    private static int laneFromTz(int trailingZeros) {
        return trailingZeros >>> 3;
    }

    private static int maxOcc(int capacity) {
        return (capacity * 7) >>> 3;
    }

    private static boolean isFull(byte ctrl) {
        return (ctrl & 0x80) == 0;
    }
}
