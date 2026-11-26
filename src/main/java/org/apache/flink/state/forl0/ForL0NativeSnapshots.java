package org.apache.flink.state.forl0;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Owns the engine's single native copy-on-write generation for checkpoints and savepoints. */
final class ForL0NativeSnapshots {
    private final long engine;
    private final KeyGroupRange groups;
    private Snapshot active;
    private boolean disposed;

    ForL0NativeSnapshots(long engine, KeyGroupRange groups) {
        this.engine = engine;
        this.groups = groups;
    }

    synchronized Snapshot prepare(boolean canonical, Collection<Long> stateHandles) {
        checkOpen();
        detach();
        Snapshot next = new Snapshot(canonical, stateHandles);
        if (!stateHandles.isEmpty()) {
            try {
                NativeEngine.prepareSnapshot(engine);
                active = next;
            } catch (Throwable failure) {
                NativeEngine.releaseSnapshot(engine);
                throw failure;
            }
        }
        return next;
    }

    // A later checkpoint or a change to the native registry needs the engine. Preserve
    // the earlier cut before handing it over. Ordinary snapshots stream directly from JNI.
    synchronized void detach() {
        checkOpen();
        if (active != null) {
            active.detach();
        }
    }

    synchronized void dispose() {
        if (disposed) return;
        disposed = true;
        try {
            if (active != null) active.release();
        } finally {
            NativeEngine.destroyEngine(engine);
        }
    }

    private void checkOpen() {
        if (disposed) throw new IllegalStateException("State backend is disposed");
    }

    final class Snapshot {
        private final boolean canonical;
        private final Map<Long, byte[][]> chunks = new LinkedHashMap<>();
        private boolean released;
        private int nextCheckpointGroup = groups.getStartKeyGroup();

        private Snapshot(boolean canonical, Collection<Long> stateHandles) {
            this.canonical = canonical;
            if (canonical) {
                for (long handle : stateHandles) chunks.put(handle, null);
            } else if (!stateHandles.isEmpty()) {
                // A native checkpoint encodes all registered states in one block per group.
                chunks.put(0L, null);
            }
        }

        byte[] checkpointGroup(int group) {
            synchronized (ForL0NativeSnapshots.this) {
                if (canonical || group != nextCheckpointGroup || group > groups.getEndKeyGroup()) {
                    throw new IllegalStateException("Unexpected checkpoint key group " + group);
                }
                byte[] result = read(0, group);
                if (active == this) {
                    NativeEngine.releaseCheckpointGroup(engine, group);
                } else if (result != null) {
                    chunks.get(0L)[group - groups.getStartKeyGroup()] = null;
                }
                nextCheckpointGroup++;
                return result;
            }
        }

        byte[] stateGroup(long handle, int group) {
            synchronized (ForL0NativeSnapshots.this) {
                if (!canonical) throw new IllegalStateException("Expected a canonical savepoint");
                return read(handle, group);
            }
        }

        private byte[] read(long handle, int group) {
            if (released) throw new IllegalStateException("Snapshot has been released");
            if (active == this) return nativeGroup(handle, group);
            byte[][] rows = chunks.get(handle);
            return rows == null ? null : rows[group - groups.getStartKeyGroup()];
        }

        private byte[] nativeGroup(long handle, int group) {
            return canonical ? NativeEngine.writeStateKeyGroupEntries(handle, group)
                    : NativeEngine.writeKeyGroupData(engine, group);
        }

        private void detach() {
            for (Map.Entry<Long, byte[][]> entry : chunks.entrySet()) {
                byte[][] rows = new byte[groups.getNumberOfKeyGroups()][];
                int begin = canonical ? groups.getStartKeyGroup() : nextCheckpointGroup;
                for (int group = begin; group <= groups.getEndKeyGroup(); group++) {
                    rows[group - groups.getStartKeyGroup()] = nativeGroup(entry.getKey(), group);
                }
                entry.setValue(rows);
            }
            NativeEngine.releaseSnapshot(engine);
            active = null;
        }

        void release() {
            synchronized (ForL0NativeSnapshots.this) {
                if (released) return;
                released = true;
                chunks.clear();
                if (active == this) {
                    active = null;
                    NativeEngine.releaseSnapshot(engine);
                }
            }
        }
    }
}
