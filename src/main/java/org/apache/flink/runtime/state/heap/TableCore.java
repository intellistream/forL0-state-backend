// filepath: /Users/jinyunyang/IdeaProjects/forL0-state-backend/src/main/java/org/apache/flink/runtime/state/heap/TableCore.java
package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

/**
 * Package-private facade that encapsulates MainTable and optional L0Table.
 * It provides a minimal, coarse-grained API for ForL0StateMap to interact with
 * without exposing table internals. No behavior change vs direct delegation.
 */
class TableCore implements AutoCloseable {
    private final MainTable mainTable;
    private final L0Table l0Table; // may be null if L0 disabled

    TableCore(MemoryManagerAllocator allocator,
              int mainTableInitPow2,
              double loadFactor,
              boolean l0Enabled,
              int l0CacheSizePow2,
              L0Table.ReplacementPolicy policy) {
        this.mainTable = new MainTable(allocator, mainTableInitPow2, loadFactor);
        this.l0Table = l0Enabled ? new L0Table(allocator, l0CacheSizePow2, policy) : null;
    }

    boolean isL0Enabled() { return l0Table != null; }

    // L0 delegates
    long l0Get(int hash, short tag, L0Table.EntryMatcher matcher) {
        return l0Table == null ? 0L : l0Table.get(hash, tag, matcher);
    }

    long l0Put(int hash, short tag, long entryAddr, L0Table.EntryMatcher matcher) {
        return l0Table == null ? 0L : l0Table.put(hash, tag, entryAddr, matcher);
    }

    long l0Remove(int hash, short tag, L0Table.EntryMatcher matcher) {
        return l0Table == null ? 0L : l0Table.remove(hash, tag, matcher);
    }

    void l0Clear() {
        if (l0Table != null) {
            l0Table.clear();
        }
    }

    L0Table.L0TableStats l0Stats() {
        return l0Table == null ? null : l0Table.getStats();
    }

    // Main table delegates
    long mainGet(int hash, short tag, MainTable.EntryMatcher matcher) {
        return mainTable.get(hash, tag, matcher);
    }

    long mainPut(int hash, short tag, long entryAddr, MainTable.EntryMatcher matcher) {
        return mainTable.put(hash, tag, entryAddr, matcher);
    }

    long mainRemove(int hash, short tag, MainTable.EntryMatcher matcher) {
        return mainTable.remove(hash, tag, matcher);
    }

    boolean mainNeedsResize() {
        return mainTable.needsResize();
    }

    boolean mainTryResize(EntryArena arena) throws Exception {
        return mainTable.tryResize(arena);
    }

    void mainForceResize(EntryArena arena) throws Exception {
        mainTable.forceResize(arena);
    }

    void mainForEachEntry(MainTable.EntryVisitor visitor) {
        mainTable.forEachEntry(visitor);
    }

    MainTable.TableStats mainStats() {
        return mainTable.getStats();
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        try { mainTable.close(); } catch (Exception e) { first = e; }
        if (l0Table != null) {
            try { l0Table.close(); } catch (Exception e) { if (first == null) first = e; }
        }
        if (first != null) throw first;
    }
}

