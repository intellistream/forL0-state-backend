package org.apache.flink.state.forl0;

import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Immutable native read snapshot, reusable until this list is modified. */
final class NativeLongListSnapshot implements Iterable<Long> {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final sun.misc.Unsafe MEMORY = org.apache.flink.core.memory.MemoryUtils.UNSAFE;
    private final long address;
    private final long[] array;
    final long version;
    final int keyGroup;
    private final int size;
    private final Cleaner.Cleanable cleanable;

    private static final class Release implements Runnable {
        private final long token;
        Release(long token) { this.token=token; }
        public void run() { NativeEngine.releaseLongListView(token); }
    }

    NativeLongListSnapshot(long[] array) {
        this.array=array;this.address=0;this.size=array.length;
        this.version=0;this.keyGroup=-1;this.cleanable=null;
    }

    NativeLongListSnapshot(long token,long address,long version,int size,int keyGroup) {
        this.array=null;
        this.version=version;this.keyGroup=keyGroup;
        this.address=address;
        this.size=size;
        this.cleanable=CLEANER.register(this,new Release(token));
    }

    public Iterator<Long> iterator() { return new Values(this); }

    private static final class Values implements Iterator<Long> {
        private final NativeLongListSnapshot owner;
        private int index;
        Values(NativeLongListSnapshot owner) { this.owner=owner; }
        public boolean hasNext() { return index<owner.size; }
        public Long next() {
            if(index>=owner.size)throw new NoSuchElementException();
            int i=index++;
            long value=owner.array!=null?owner.array[i]:MEMORY.getLong(owner.address+8L*i);
            Reference.reachabilityFence(owner);
            return new Long(value);
        }
    }
}
