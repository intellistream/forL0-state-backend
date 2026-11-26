package org.apache.flink.state.forl0;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/** Task-thread lookup in the controls of a resident table or the optional filter of a DRAM root. */
final class NamespaceReadIndex {
    private static final int CELL_BYTES = 32;
    private static final long COMPACT_SLOTS_TAG = 1L << 40;
    private static final long COMPACT_BITS = 0xffL << 32;
    private static final long VALID_ABSENCE = 1L << 62;
    static final int CONTROL_BYTES = 128;
    static final int UNKNOWN = 0, ABSENT = 1, NUMBER = 2, ROW = 3, MAP = 4;
    private static final int EIGHT_BYTE_WINDOW_ROW = 247, SIXTEEN_BYTE_WINDOW_ROW = 248;
    private static final int WIDE_ROW_INT_MAP = 249, LONG_ROW_MAP = 250, ROW_INT_MAP = 251, PACKED_ROW = 252, LONG_MAP = 253, WINDOW_MAP = 254;
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    static final int ROOT = 1, LONG = 2, WINDOW = 3;
    private static final long ONES = 0x0101010101010101L, HIGH_BITS = ONES * 0x80;
    private final ByteBuffer cells;
    private final ByteBuffer controls;
    private final long base, membershipAddress, controlsAddress;
    private final boolean packedMapTags;
    private long mapTagsPresent;
    private final MemorySegment pool;
    private ByteBuffer comparisonView;
    private final int firstGroup;
    // Per call results are consumed on the owning Flink task thread.
    long result;
    int rowBytes;
    long secondResult;
    private int mapSize, mapEntryBytes, mapTagBytes, mapFormat, rowKeyInlineBytes, lastMapEntry = -1;
    private int untilNativeHit = 64;
    private final short[] rootSamples;
    private final byte[] rootResolutions;

    private NamespaceReadIndex(ByteBuffer membership, ByteBuffer controls, MemorySegment pool, long base, int firstGroup, boolean root, boolean packedMapTags) {
        this.cells = membership == null ? null : membership.order(ByteOrder.nativeOrder());
        this.controls = controls == null ? null : controls.order(ByteOrder.nativeOrder());
        this.membershipAddress = MemorySegmentBridge.nativeCounterBase(this.cells);
        this.controlsAddress = MemorySegmentBridge.nativeCounterBase(this.controls);
        this.pool = pool;
        this.packedMapTags = packedMapTags;
        this.base = base;
        this.firstGroup = firstGroup;
        int groups = controls != null ? controls.capacity() / CONTROL_BYTES : membership.capacity() / CELL_BYTES;
        this.rootSamples = root ? new short[groups] : null;
        this.rootResolutions = root ? new byte[groups] : null;
    }

    static NamespaceReadIndex open(long handle, MemorySegment pool, long base, int firstGroup, boolean root, boolean packedMapTags) {
        ByteBuffer membership = root ? NativeEngine.rootMembershipViews(handle) : null;
        ByteBuffer resident = NativeEngine.residentControlViews(handle);
        return membership == null && resident == null ? null : new NamespaceReadIndex(membership, resident, pool, base, firstGroup, root, packedMapTags);
    }

    static NamespaceReadIndex forMap(long handle, int firstGroup) {
        return forState(handle, firstGroup, true);
    }

    static NamespaceReadIndex forState(long handle, int firstGroup, boolean root) {
        long[] range = NativeEngine.stateCacheMemoryRange(handle);
        if (range == null || range.length < 4 || range[2] == 0 || range[3] <= 0
                || range[3] > Integer.MAX_VALUE) return null;
        return open(handle, MemorySegmentBridge.wrapNativeAddress(range[2], (int) range[3]), range[2], firstGroup, root,
                range.length >= 5 && (range[4] & 1) != 0);
    }

    byte[] copyCurrentRow() {
        byte[] bytes = new byte[rowBytes];
        pool.get(Math.toIntExact(result - base), bytes);
        return bytes;
    }

    // ListSerializer<RowData> stores a count followed by length-prefixed BinaryRows.
    // Each returned row owns its bytes, including after a table move or checkpoint.
    List<RowData> copyCurrentRowList(int arity) {
        if (rowBytes < Integer.BYTES) return null;
        int offset = Math.toIntExact(result - base);
        int end = offset + rowBytes;
        int count = pool.getIntBigEndian(offset);
        offset += Integer.BYTES;
        int minimum = BinaryRowData.calculateFixPartSizeInBytes(arity);
        if (count < 0 || count > (end - offset) / (minimum + Integer.BYTES)) return null;
        List<RowData> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (end - offset < Integer.BYTES) return null;
            int length = pool.getIntBigEndian(offset);
            offset += Integer.BYTES;
            if (length < minimum || length > end - offset) return null;
            byte[] owned = new byte[length];
            pool.get(offset, owned);
            rows.add(RowDataKeyAccessor.wrapBinaryRowData(owned, arity));
            offset += length;
        }
        return offset == end ? rows : null;
    }

    boolean absentNumber(int group, long key) {
        return readRoot(group, key, 0, false, false) == ABSENT;
    }

    int readNumber(int group, long key) {
        return readRoot(group, key, 0, false, true);
    }

    int readMapEntry(int group, long key, long first, long second, boolean windows) {
        int status = readRoot(group, key, 0, false, true);
        if (status != MAP) return status == ABSENT ? ABSENT : UNKNOWN;
        if (mapFormat != (windows ? WINDOW_MAP : LONG_MAP)) return UNKNOWN;
        int firstEntry = Math.toIntExact(result - base);
        int index = numericMapIndex(firstEntry, first, second, windows);
        if (index < 0) return ABSENT;
        lastMapEntry = index;
        int entry = firstEntry + index * mapEntryBytes;
        result = pool.getLong(entry + mapEntryBytes / 2);
        if (windows) secondResult = pool.getLong(entry + 24);
        return NUMBER;
    }

    private int numericMapIndex(int firstEntry, long key, long end, boolean window) {
        // The packed native map derives its fingerprint from the same complete integer or window hash.
        long hash = window ? key ^ (end + 0x9e3779b97f4a7c15L + (key << 6) + (key >>> 2)) : key;
        long tag = (hash ^ (hash >>> 16)) & 127;
        if ((mapTagsPresent & (1L << tag)) == 0) return -1;
        for (int first = 0; first < mapSize; first += 16) {
            int count = Math.min(16, mapSize - first);
            int matches = mapSize > 4 ? mapCandidates(firstEntry - mapTagBytes + first, tag, count) : (1 << count) - 1;
            while (matches != 0) {
                int index = first + Integer.numberOfTrailingZeros(matches);
                int entry = firstEntry + index * mapEntryBytes;
                if (pool.getLong(entry) == key && (!window || pool.getLong(entry + 8) == end)) return index;
                matches &= matches - 1;
            }
        }
        return -1;
    }

    private boolean openMap(int slot, long layout, int format) {
        int extent = slot + (int) ((layout >>> 24) & 0xffff);
        mapSize = pool.getInt(extent);
        int capacity = pool.getInt(extent + 4);
        if (mapSize < 0 || capacity < 1 || capacity > 64 || mapSize > capacity) return false;
        mapTagBytes = (capacity + 15) & ~15;
        mapFormat = format;
        rowKeyInlineBytes = 0;
        switch (format) {
            case WIDE_ROW_INT_MAP: mapEntryBytes = 32; rowKeyInlineBytes = 20; break;
            case LONG_ROW_MAP: mapEntryBytes = 40; break;
            case ROW_INT_MAP: mapEntryBytes = 56; rowKeyInlineBytes = 44; break;
            case WINDOW_MAP: mapEntryBytes = 32; break;
            default: mapEntryBytes = 16;
        }
        int storage = slot + (int) ((layout >>> 40) & 0xffff);
        mapTagsPresent = packedMapTags && mapSize > 4 ? pool.getLong(storage + Long.BYTES) : -1L;
        result = capacity == 1 ? base + storage : pool.getLong(storage);
        long offset = result - base;
        return offset >= 0 && offset <= pool.size() - mapSize * mapEntryBytes;
    }

    int readMapPayload(int group, long key, long inner) {
        int status = readRoot(group, key, 0, false, true);
        if (status != MAP) return status == ABSENT ? ABSENT : UNKNOWN;
        if (mapFormat != LONG_ROW_MAP) return UNKNOWN;
        int first = Math.toIntExact(result - base);
        int index = numericMapIndex(first, inner, 0, false);
        if (index < 0) return ABSENT;
        int entry = first + index * mapEntryBytes;
        if (!openRow(entry + 8, entry + 16)) return UNKNOWN;
        lastMapEntry = index;
        return ROW;
    }

    private int writablePayload(int group, long key, long inner, int length) {
        int entry = writableMapEntry(group, key, LONG_ROW_MAP);
        if (entry < 0 || pool.getLong(entry) != inner || !openRow(entry + 8, entry + 16) || rowBytes != length) return -1;
        return Math.toIntExact(result - base);
    }

    boolean updateMapBytes(int group, long key, long inner, byte[] bytes, int length) {
        int target = writablePayload(group, key, inner, length);
        if (target < 0) return false;
        pool.put(target, bytes, 0, length);
        recordUpdate((group - firstGroup) * CONTROL_BYTES);
        return true;
    }

    boolean updateMapRow(int group, long key, long inner, MemorySegment source, int offset, int length, boolean list) {
        int prefix = list ? 8 : 4;
        // The caller owns this contiguous on-heap BinaryRow. Use Flink's canonical length framing.
        if (source.isOffHeap() || length > Integer.MAX_VALUE - prefix) return false;
        int target = writablePayload(group, key, inner, length + prefix);
        if (target < 0) return false;
        if (list) pool.putIntBigEndian(target, 1);
        pool.putIntBigEndian(target + prefix - 4, length);
        source.copyTo(offset, pool, target + prefix, length);
        recordUpdate((group - firstGroup) * CONTROL_BYTES);
        return true;
    }

    int readRowMap(int group, long key) {
        int status = readRoot(group, key, 0, false, true);
        return status == MAP && rowKeyInlineBytes == 0 ? UNKNOWN : status;
    }

    int readWholeMap(int group, long key) {
        return readRoot(group, key, 0, false, true);
    }

    boolean currentMapIsEmpty() { return mapSize == 0; }

    // Export only the requested column while the complete map is still owned by the task.
    long[] copyMapNumbers(boolean keys) {
        int entry = Math.toIntExact(result - base);
        if (mapFormat == WINDOW_MAP) {
            long[] windows = new long[mapSize * 2];
            if (!keys) entry += 16;
            for (int i = 0; i < windows.length; i += 2, entry += 32) {
                windows[i] = pool.getLong(entry);
                windows[i + 1] = pool.getLong(entry + 8);
            }
            return windows;
        }
        long[] numbers = new long[mapSize];
        if (rowKeyInlineBytes != 0 && !keys) {
            for (int i = 0; i < numbers.length; i++, entry += mapEntryBytes) numbers[i] = pool.getIntBigEndian(entry + rowKeyInlineBytes + Integer.BYTES);
        } else if (mapFormat == LONG_MAP || (mapFormat == LONG_ROW_MAP && keys)) {
            if (!keys) entry += 8;
            for (int i = 0; i < numbers.length; i++, entry += mapEntryBytes) numbers[i] = pool.getLong(entry);
        } else return null;
        return numbers;
    }

    List<BinaryRowData> copyMapRowKeys(int arity) {
        if (rowKeyInlineBytes == 0) return null;
        int entry = Math.toIntExact(result - base);
        List<BinaryRowData> keys = new ArrayList<>(mapSize);
        for (int i = 0; i < mapSize; i++, entry += mapEntryBytes) {
            int length = pool.getInt(entry + rowKeyInlineBytes);
            if (length < Integer.BYTES || length > pool.size()) return null;
            long start = length <= rowKeyInlineBytes ? entry : pool.getLong(entry) - base;
            if (start < 0 || start > pool.size() - length || pool.getIntBigEndian((int) start) != length - 4) return null;
            byte[] owned = new byte[length - 4];
            pool.get((int) start + 4, owned);
            keys.add(RowDataKeyAccessor.wrapBinaryRowData(owned, arity));
        }
        return keys;
    }

    // A Flink iterator may escape the current event. Export one owned array
    // from the packed authoritative entries before allowing any subsequent mutation.
    byte[] copyMapEntries(boolean rowKeys) {
        if (rowKeys ? rowKeyInlineBytes == 0 : mapFormat != LONG_ROW_MAP) return null;
        final int first = Math.toIntExact(result - base);
        long bytes = 4;
        for (int i = 0, entry = first; i < mapSize; i++, entry += mapEntryBytes) {
            if (rowKeys) {
                int length = pool.getInt(entry + rowKeyInlineBytes);
                if (length < 4 || length > pool.size()) return null;
                bytes += 4L + length;
            } else {
                if (!openRow(entry + 8, entry + 16)) return null;
                bytes += 12L + rowBytes;
            }
        }
        if (bytes > Integer.MAX_VALUE) return null;
        byte[] data = new byte[(int) bytes];
        MemorySegment snapshot = MemorySegmentFactory.wrap(data);
        snapshot.putIntBigEndian(0, mapSize);
        int dest = 4;
        for (int i = 0, entry = first; i < mapSize; i++, entry += mapEntryBytes) {
            if (rowKeys) {
                int length = pool.getInt(entry + rowKeyInlineBytes);
                long position = length <= rowKeyInlineBytes ? entry : pool.getLong(entry) - base;
                if (position < 0 || position > pool.size() - length) return null;
                pool.copyTo((int) position, snapshot, dest, length);
                pool.copyTo(entry + rowKeyInlineBytes + Integer.BYTES, snapshot, dest + length, 4);
                dest += length + 4;
            } else {
                // Task-thread ownership keeps this entry stable across both passes.
                if (!openRow(entry + 8, entry + 16)) return null;
                snapshot.putLongBigEndian(dest, pool.getLong(entry));
                snapshot.putIntBigEndian(dest + 8, rowBytes);
                pool.copyTo(Math.toIntExact(result - base), snapshot, dest + 12, rowBytes);
                dest += 12 + rowBytes;
            }
        }
        return data;
    }

    private boolean sameRowKey(int entry, MemorySegment source, int offset, int length) {
        int storedLength = pool.getInt(entry + rowKeyInlineBytes);
        if (storedLength < 4 || storedLength - 4 != length) return false;
        long stored = storedLength <= rowKeyInlineBytes ? entry : pool.getLong(entry) - base;
        if (stored < 0 || stored > pool.size() - storedLength || pool.getIntBigEndian((int) stored) != length)
            return false;
        int payload = (int) stored + Integer.BYTES;
        if (length < 64) return pool.equalTo(source, payload, offset, length);
        // ByteBuffer compares the entire range using the VM's bulk mismatch primitive.
        // Only the task-owned pool view is retained. A Flink input row may be reused immediately.
        if (comparisonView == null) comparisonView = pool.wrap(0, pool.size());
        selectBytes(comparisonView, payload, length);
        if (!source.isOffHeap()) return comparisonView.equals(source.wrap(offset, length));
        // Some Flink off-heap segments permit a buffer only for the duration of a callback.
        return source.processAsByteBuffer(input -> {
            selectBytes(input, offset, length);
            return comparisonView.equals(input);
        });
    }

    private static void selectBytes(Buffer range, int offset, int length) {
        range.limit(offset + length);
        range.position(offset);
    }

    // StateMap constructs groups of 16 tags immediately before its packed entries once
    // a fifth key is inserted. The same native tags also filter lookup from the Flink task.
    private int mapCandidates(int tagOffset, long tag, int count) {
        long pattern = tag * ONES;
        long low = zeroBytes(pool.getLong(tagOffset) ^ pattern);
        long high = zeroBytes(pool.getLong(tagOffset + 8) ^ pattern);
        if (!LITTLE_ENDIAN) { low = Long.reverseBytes(low); high = Long.reverseBytes(high); }
        // Pack bit 7 of each byte into its entry position. A carry in zeroBytes can
        // add a candidate, so every selected entry still undergoes full-key comparison.
        long gather = 0x0002040810204081L;
        int mask = (int) (low * gather >>> 56) | ((int) (high * gather >>> 56) << 8);
        return mask & ((1 << Math.min(count, 16)) - 1);
    }

    private static long rowKeyTag(MemorySegment source, int offset, int length) {
        int tail = length - Long.BYTES;
        long first = source.getLong(offset + Math.min(Long.BYTES, tail));
        long middle = source.getLong(offset + tail / 2);
        long last = source.getLong(offset + tail);
        long bits = first ^ Long.rotateLeft(middle, 17) ^ Long.rotateLeft(last, 37)
                ^ ((length + 4L) * 0x9e3779b97f4a7c15L);
        return ((bits ^ (bits >>> 33)) * 0xff51afd7ed558ccdL) >>> 57;
    }

    int readRowMapValue(MemorySegment source, int offset, int length) {
        int first = Math.toIntExact(result - base);
        boolean tagged = mapSize > 4 && length >= Long.BYTES;
        long tag = tagged ? rowKeyTag(source, offset, length) : 0;
        if (tagged && (mapTagsPresent & (1L << tag)) == 0) return ABSENT;
        for (int start = 0; start < mapSize; start += 16) {
            int count = Math.min(16, mapSize - start);
            int matches = tagged ? mapCandidates(first - mapTagBytes + start, tag, count) : (1 << count) - 1;
            while (matches != 0) {
                int index = start + Integer.numberOfTrailingZeros(matches);
                int entry = first + index * mapEntryBytes;
                if (sameRowKey(entry, source, offset, length)) {
                    lastMapEntry = index;
                    result = pool.getIntBigEndian(entry + rowKeyInlineBytes + Integer.BYTES);
                    return NUMBER;
                }
                matches &= matches - 1;
            }
        }
        return ABSENT;
    }

    // A remove or resize can relocate all inner entries without moving the outer
    // table. Reopen the live representation and recheck the full inner key.
    private int writableMapEntry(int group, long key, int format) {
        if (controls == null || lastMapEntry < 0) return -1;
        int cell = (group - firstGroup) * CONTROL_BYTES;
        long layout = controls.getLong(cell + 56);
        if ((layout >>> 56) != format) return -1;
        int slot = writableSlot(cell, ROOT, 0, 0, key, 0, layout);
        if (slot < 0 || !openMap(slot, layout, format) || lastMapEntry >= mapSize) return -1;
        return Math.toIntExact(result - base) + lastMapEntry * mapEntryBytes;
    }

    boolean updateFixedMap(int group, long key, long first, long second, long value, long end, boolean windows) {
        int entry = writableMapEntry(group, key, windows ? WINDOW_MAP : LONG_MAP);
        if (entry < 0 || pool.getLong(entry) != first || (windows && pool.getLong(entry + 8) != second)) return false;
        pool.putLong(entry + mapEntryBytes / 2, value);
        if (windows) pool.putLong(entry + 24, end);
        recordUpdate((group - firstGroup) * CONTROL_BYTES);
        return true;
    }

    boolean updateRowMap(int group, long key, MemorySegment source, int offset, int length, int value) {
        if (rowKeyInlineBytes == 0) return false;
        int entry = writableMapEntry(group, key, mapFormat);
        if (entry < 0 || !sameRowKey(entry, source, offset, length)) return false;
        pool.putIntBigEndian(entry + rowKeyInlineBytes + Integer.BYTES, value);
        recordUpdate((group - firstGroup) * CONTROL_BYTES);
        return true;
    }

    int readPair(int group, long first, long second) {
        return readRoot(group, first, second, true, true);
    }

    private int readRoot(int group, long key, long other, boolean pair, boolean values) {
        int index = group - firstGroup;
        // The descriptor withdraws its slot address before a table moves. A whole
        // resident table can answer hits immediately, even while an adjacent group
        // is bypassing an ineffective negative-only index.
        int control = index * CONTROL_BYTES;
        long slots = values && controls != null ? controls.getLong(control + 48) : 0;
        if (slots != 0) {
            // Only a complete VoidNamespace table can publish slots in this state's root cell.
            return probeResident(control, controls.getLong(control), controls.getLong(control + 8),
                    slots, key, other, ROOT, pair, true);
        }
        if (rootSamples[index] < 0) {
            ++rootSamples[index];
            return UNKNOWN;
        }
        long hash = rootHash(key, other, pair);
        int result = UNKNOWN;
        if (cells != null) {
            int cell = index * CELL_BYTES;
            long offset = cells.getLong(cell);
            if (offset != 0 && excludes(cell, offset, hash)) result = ABSENT;
        }
        if (result == UNKNOWN) result = probe(group, ROOT, 0, 0, key, other, hash, false, false);
        if (result != UNKNOWN) ++rootResolutions[index];
        if (++rootSamples[index] == 64) {
            rootSamples[index] = (short) (rootResolutions[index] < 16 ? -256 : 0);
            rootResolutions[index] = 0;
        }
        return result;
    }

    int readWindow(int group, int kind, long first, long second, long key, long header, boolean allowRow) {
        return probe(group, kind, first, second, key, header, 0, true, allowRow);
    }

    private int controlCell(int group, int kind, long first, long second) {
        int index = group - firstGroup;
        if (kind == ROOT) return index * CONTROL_BYTES;
        long key = first ^ Long.rotateLeft(second, 21);
        int bucket = (int) (((key ^ (key >>> 32)) * 0x9e3779b97f4a7c15L) >>> 62);
        return (4 * index + bucket) * CONTROL_BYTES;
    }

    private int probe(int group, int kind, long first, long second, long key, long other,
                      long hash, boolean values, boolean allowRow) {
        if (controls == null) return UNKNOWN;
        int cell = controlCell(group, kind, first, second);
        long address = controls.getLong(cell);
        if (address == 0) {
            if (kind != ROOT && controls.getLong(cell + 16) == (kind | 8)
                    && controls.getLong(cell + 24) == first && controls.getLong(cell + 32) == second) {
                MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 112);
                return ABSENT;
            }
            return UNKNOWN;
        }
        if (controls.getLong(cell + 16) != kind
                || (kind != ROOT && (controls.getLong(cell + 24) != first
                        || controls.getLong(cell + 32) != second))) return UNKNOWN;
        long mask = controls.getLong(cell + 8);
        long slots = values ? controls.getLong(cell + 48) : 0;
        if (slots != 0) return probeResident(cell, address, mask, slots, key, other, kind, false, allowRow);
        if (kind != ROOT) hash = windowRowHash(key, other);
        return probeGroup(cell, address, mask, 0, key, other, hash, allowRow, kind == ROOT);
    }

    private static long rootHash(long key, long other, boolean pair) {
        return pair ? ((0xcbf29ce484222325L ^ key) * 0x100000001b3L ^ other) * 0x100000001b3L : mix(key);
    }

    private static long windowRowHash(long key, long header) {
        return mix(key) ^ (header == 0 ? 0 : mix(header));
    }

    // This word belongs to the selected namespace table. It stores a slot ordinal and a
    // fingerprint, never a value or address. Recheck occupancy and the entire stored key.
    private static long rowFingerprint(long key, long other) {
        long combined = key ^ Long.rotateLeft(other, 23);
        return (combined ^ (combined >>> 32)) << 32;
    }

    private int probeResident(int cell, long address, long mask, long slots, long key, long other,
                              int kind, boolean pair, boolean allowRow) {
        long previous = controls.getLong(cell + 120);
        if (mask < 8 && (previous & ~COMPACT_BITS) == COMPACT_SLOTS_TAG) {
            int live = (int) ((previous & COMPACT_BITS) >>> 32);
            long layout = controls.getLong(cell + 56);
            while (live != 0) {
                int position = Integer.numberOfTrailingZeros(live);
                int slot = Math.toIntExact(slots - base + position * (layout & 0xffff));
                if (matchesKey(slot, layout, key, other)) return readSlot(cell, slots, layout, position, allowRow);
                live &= live - 1;
            }
            MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 40);
            return ABSENT;
        }
        if (mask <= 3) return readSmallTable(cell, Math.toIntExact(address - base), mask, slots, key, other, allowRow);
        int index = (int) previous - 1;
        if (index >= 0 && index <= mask && (previous & 0xffffffff00000000L) == rowFingerprint(key, other)
                && pool.get(Math.toIntExact(address - base + index)) >= 0) {
            long layout = controls.getLong(cell + 56);
            int slot = Math.toIntExact(slots - base + index * (layout & 0xffff));
            if (matchesKey(slot, layout, key, other)) return readSlot(cell, slots, layout, index, allowRow);
        }
        long hash = kind == ROOT ? rootHash(key, other, pair) : windowRowHash(key, other);
        return probeGroup(cell, address, mask, slots, key, other, hash, allowRow, false);
    }

    private int probeGroup(int cell, long address, long mask, long slots, long key, long other,
                           long hash, boolean allowRow, boolean rootControls) {
        long start = mask < 15 ? 0 : (hash >>> 7) & mask;
        long tag = (hash & 127) * ONES;
        long stride = 0;
        while (true) {
            int pos = Math.toIntExact(address - base + start);
            long lo = pool.getLong(pos), hi = pool.getLong(pos + 8);
            long lowMatches = zeroBytes(lo ^ tag), highMatches = zeroBytes(hi ^ tag);
            if ((lowMatches | highMatches) != 0) {
                if (slots == 0) {
                    // Native validates occupancy and the full key in the DRAM table. A
                    // single candidate saves it from loading the same probe group again.
                    if (rootControls && mask >= 511 && (lowMatches == 0 || highMatches == 0)) {
                        long candidate = lowMatches | highMatches;
                        if ((candidate & (candidate - 1)) == 0) {
                            if (!LITTLE_ENDIAN) candidate = Long.reverseBytes(candidate);
                            long half = lowMatches == 0 ? 8 : 0;
                            long index = (start + half + (Long.numberOfTrailingZeros(candidate) >>> 3)) & mask;
                            controls.putLong(cell + 120, rowFingerprint(key, other) | (index + 1));
                        }
                    }
                    return UNKNOWN;
                }
                long layout = controls.getLong(cell + 56);
                int index = matchingSlot(lowMatches, pos, start, mask, slots, layout, key, other, (byte) hash);
                if (index < 0) index = matchingSlot(highMatches, pos + 8, start + 8, mask, slots, layout, key, other, (byte) hash);
                if (index >= 0) {
                    controls.putLong(cell + 120, rowFingerprint(key, other) | (index + 1L));
                    return readSlot(cell, slots, layout, index, allowRow);
                }
            }
            if ((zeroBytes(lo ^ HIGH_BITS) | zeroBytes(hi ^ HIGH_BITS)) == 0) {
                // Follow the canonical triangular sequence only for a complete resident table.
                // A control-only view delegates to its native owner when the first group is full.
                stride += 16;
                if (slots == 0 || stride > mask) return UNKNOWN;
                start = (start + stride) & mask;
                continue;
            }
            // Without any matching H2 the miss applies to every key with this full hash.
            // A miss after comparing colliding keys is only valid for that particular key.
            if (stride == 0 && (lowMatches | highMatches) == 0 && mask >= 15) {
                long available = lo & HIGH_BITS;
                int half = 0;
                if (available == 0) { half = 8; available = hi & HIGH_BITS; }
                if (!LITTLE_ENDIAN) available = Long.reverseBytes(available);
                int delta = half + (Long.numberOfTrailingZeros(available) >>> 3);
                if (pool.get(pos + delta) < -1) {
                    controls.putLong(cell + 80, hash);
                    controls.putLong(cell + 72, ~((start + delta) & mask));
                }
            }
            MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 40);
            return ABSENT;
        }
    }

    private int readSmallTable(int cell, int control, long mask, long slots, long key, long other, boolean allowRow) {
        // Only the first capacity control bytes describe actual slots. The following
        // bytes mirror the beginning of the table for SIMD reads across its end.
        long controlWord = pool.getLong(control);
        if (!LITTLE_ENDIAN) controlWord = Long.reverseBytes(controlWord);
        long live = ~controlWord & (HIGH_BITS >>> (64 - 8 * (mask + 1)));
        long layout = controls.getLong(cell + 56);
        for (; live != 0; live &= live - 1) {
            int index = Long.numberOfTrailingZeros(live) >>> 3;
            int slot = Math.toIntExact(slots - base + index * (layout & 0xffff));
            if (matchesKey(slot, layout, key, other)) return readSlot(cell, slots, layout, index, allowRow);
        }
        MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 40);
        return ABSENT;
    }

    private int readSlot(int cell, long slots, long layout, int index, boolean allowRow) {
        // Periodic native hits feed the whole-table replacement and read/write locality policy.
        if (--untilNativeHit == 0) { untilNativeHit = 64; return UNKNOWN; }
        int slot = Math.toIntExact(slots - base + index * (layout & 0xffff));
        int value = slot + (int) ((layout >>> 24) & 0xffff);
        int format = (int) (layout >>> 56);
        int outcome = NUMBER;
        if (format == LONG_MAP || format == WINDOW_MAP || format == ROW_INT_MAP || format == WIDE_ROW_INT_MAP || format == LONG_ROW_MAP) {
            if (!openMap(slot, layout, format)) return UNKNOWN;
            outcome = MAP;
        } else {
            if (format == 0) result = pool.getLong(value);
            else {
                if (!allowRow) return UNKNOWN;
                int storage = slot + (int) ((layout >>> 40) & 0xffff);
                if (!openSlotRow(format, value, storage) || rowBytes == 0) return UNKNOWN;
                outcome = ROW;
            }
        }
        controls.putLong(cell + 72, index + 1L);
        MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 64);
        return outcome;
    }

    private int matchingSlot(long matches, int ctrl, long start, long mask, long slots, long layout,
                             long key, long other, byte hash) {
        if (!LITTLE_ENDIAN) matches = Long.reverseBytes(matches);
        for (; matches != 0; matches &= matches - 1) {
            int i = Long.numberOfTrailingZeros(matches) >>> 3;
            // SWAR can flag a neighbor of a zero byte through a borrow. Only exact H2 bytes identify constructed slots.
            if (pool.get(ctrl + i) != (byte) (hash & 127)) continue;
            int index = Math.toIntExact((start + i) & mask);
            int slot = Math.toIntExact(slots - base + index * (layout & 0xffff));
            if (matchesKey(slot, layout, key, other)) return index;
        }
        return -1;
    }

    private boolean matchesKey(int slot, long layout, long key, long other) {
        switch ((int) ((layout >>> 16) & 0x7f)) {
            case 1: return pool.getLong(slot) == key;
            case 2: return pool.getInt(slot) == key;
            case 3: return pool.getLong(slot) == key && pool.getLong(slot + 8) == other;
            case 4: return pool.getLong(slot) == key && pool.getLong(slot + 8) == other
                    && pool.getShort(slot + 16) == 0;
            default: throw new IllegalStateException("Unknown resident key layout");
        }
    }

    // The native table withdraws its address on move and its write permission at a
    // snapshot barrier. Revalidate the complete key even when a position survived an erase.
    private int writableSlot(int cell, int kind, long first, long second, long key, long other, long layout) {
        if ((layout & (1L << 23)) == 0) return -1;
        long position = controls.getLong(cell + 72) - 1;
        if (position < 0 || position > controls.getLong(cell + 8)) return -1;
        long address = controls.getLong(cell);
        if (address == 0 || controls.getLong(cell + 16) != kind
                || controls.getLong(cell + 24) != first || controls.getLong(cell + 32) != second) return -1;
        int ctrl = Math.toIntExact(address - base + position);
        if (pool.get(ctrl) < 0) return -1;
        int slot = Math.toIntExact(controls.getLong(cell + 48) - base + position * (layout & 0xffff));
        return matchesKey(slot, layout, key, other) ? slot : -1;
    }

    boolean updateNumber(int group, int kind, long first, long second, long key, long other, long bits) {
        if (controls == null) return false;
        int cell = controlCell(group, kind, first, second);
        long layout = controls.getLong(cell + 56);
        if ((layout >>> 56) != 0) return false;
        int slot = writableSlot(cell, kind, first, second, key, other, layout);
        if (slot < 0) return false;
        pool.putLong(slot + (int) ((layout >>> 24) & 0xffff), bits);
        recordUpdate(cell);
        return true;
    }

    // A single BIGINT BinaryRowData consists of the eight-byte header and one fixed field.
    // Its 16-byte canonical value is inline even when the rest of this namespace moves.
    boolean updateLongRow(int group, int kind, long first, long second, long key, long other,
                          byte rowKind, boolean isNull, long number) {
        if (controls == null) return false;
        int cell = controlCell(group, kind, first, second);
        long layout = controls.getLong(cell + 56);
        int format = (int) (layout >>> 56);
        if (format != PACKED_ROW && format != SIXTEEN_BYTE_WINDOW_ROW) return false;
        int slot = writableSlot(cell, kind, first, second, key, other, layout);
        if (slot < 0) return false;
        int encoding = (int) pool.getLong(slot + (int) ((layout >>> 24) & 0xffff)) & 3;
        if (encoding != (format == PACKED_ROW ? 2 : 1)) return false;
        int storage = slot + (int) ((layout >>> 40) & 0xffff);
        long header = (rowKind & 255L) | (isNull ? 256L : 0L);
        pool.putLong(storage, LITTLE_ENDIAN ? header : Long.reverseBytes(header));
        pool.putLong(storage + Long.BYTES, number);
        recordUpdate(cell);
        return true;
    }

    boolean updateRow(int group, int kind, long first, long second, long key, long other,
                      MemorySegment source, int offset, int length) {
        if (controls == null || length <= 0) return false;
        int cell = controlCell(group, kind, first, second);
        long layout = controls.getLong(cell + 56);
        int format = (int) (layout >>> 56);
        int slot = writableSlot(cell, kind, first, second, key, other, layout);
        if (slot < 0) return false;
        int storage = slot + (int) ((layout >>> 40) & 0xffff);
        if (!openSlotRow(format, slot + (int) ((layout >>> 24) & 0xffff), storage) || rowBytes != length) return false;
        long address = result;
        long target = address - base;
        if (source.isOffHeap()) {
            long start = source.getAddress() + offset;
            if (start != address && start < address + length && address < start + length) return false;
        }
        source.copyTo(offset, pool, (int) target, length);
        recordUpdate(cell);
        return true;
    }

    private boolean openSlotRow(int format, int representation, int storage) {
        if (format == PACKED_ROW) return openRow(representation, storage);
        if (format != EIGHT_BYTE_WINDOW_ROW && format != SIXTEEN_BYTE_WINDOW_ROW) return false;
        // The exceptional representation keeps every byte but requires a native lookup.
        if ((pool.getLong(representation) & 3) != 1) return false;
        rowBytes = format == EIGHT_BYTE_WINDOW_ROW ? 8 : 16;
        result = base + storage;
        return storage >= 0 && storage <= pool.size() - rowBytes;
    }

    private boolean openRow(int representation, int storage) {
        int encoding = (int) pool.getLong(representation) & 3;
        long length = packedRowLength(encoding, storage);
        if (length < 0 || length > pool.size()) return false;
        rowBytes = (int) length;
        result = encoding == 1 ? pool.getLong(storage) : base + storage;
        long offset = result - base;
        return offset >= 0 && offset <= pool.size() - rowBytes;
    }

    private long packedRowLength(int encoding, int storage) {
        if (encoding >= 2) return encoding * 8;
        return encoding == 1 ? pool.getLong(storage + 8) : pool.get(storage + 23) & 0xff;
    }

    private void recordUpdate(int cell) {
        controls.putLong(cell + 72, 0);
        MemorySegmentBridge.incrementNativeCounter(controlsAddress + cell + 104);
    }

    private static long zeroBytes(long word) {
        return (word - ONES) & ~word & HIGH_BITS;
    }

    private boolean excludes(int cell, long start, long hash) {
        long wordMask = cells.getLong(cell + 8);
        if ((wordMask & VALID_ABSENCE) != 0 && cells.getLong(cell + 16) == hash) {
            MemorySegmentBridge.incrementNativeCounter(membershipAddress + cell + 24);
            return true;
        }
        long bits = mix(hash);
        long index = (bits >>> 24) & wordMask;
        int address = Math.toIntExact(start - 1 + index * Long.BYTES);
        long mask = (1L << (bits & 63)) | (1L << ((bits >>> 6) & 63)) | (1L << ((bits >>> 12) & 63));
        if ((pool.getLong(address) & mask) == mask) return false;
        cells.putLong(cell + 16, hash);
        // The native publisher advertises support in bit 63 and revokes on insert.
        if (wordMask < 0 && (wordMask & VALID_ABSENCE) == 0) cells.putLong(cell + 8, wordMask | VALID_ABSENCE);
        MemorySegmentBridge.incrementNativeCounter(membershipAddress + cell + 24);
        return true;
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
