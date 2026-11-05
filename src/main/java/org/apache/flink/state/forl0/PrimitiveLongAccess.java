package org.apache.flink.state.forl0;

/** Native pages have stable addresses for the lifetime of a keyed backend. */
final class PrimitiveLongAccess {
    private static final sun.misc.Unsafe M=org.apache.flink.core.memory.MemoryUtils.UNSAFE;
    private static final long ARRAY_BASE=M.arrayBaseOffset(long[].class);
    private final long pages,blocked,counts,stride,valueOffset,epoch;
    private final int startGroup;
    PrimitiveLongAccess(long handle) {
        long[] info=NativeEngine.primitiveLongDirectory(handle);
        pages=info==null?0:info[0];blocked=info==null?0:info[1];counts=info==null?0:info[2];
        epoch=info==null?0:info[6];
        startGroup=info==null?0:(int)info[3];stride=info==null?0:info[4];valueOffset=info==null?0:info[5];
    }
    private long page(long key) {
        return pages!=0 && key>=0 && key<(1L<<24)?M.getLong(pages+(key>>>8)*8):0;
    }
    long row(long key,int group) {
        long p=page(key);int index=(int)key&255;
        return p!=0 && M.getInt(p+index*4L)==group?p+valueOffset+index*stride:0;
    }
    // Direct mutations run on the mailbox thread, and are blocked throughout
    // asynchronous snapshots. Snapshot preparation publishes preceding writes
    // through the per-group mutex before handing work to the snapshot thread.
    // Existing page tags therefore need ordinary aligned stores on this path;
    // a per-record release store adds no synchronization with a concurrent reader.
    boolean put(long key,int group,long value) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long p=page(key);if(p==0)return false;
        int index=(int)key&255;long tag=p+index*4L;
        int owner=M.getInt(tag);
        if(owner==group) {M.putLong(p+valueOffset+index*stride,value);return true;}
        // A tombstone for this group proves that no fallback entry exists for
        // this key/group. Virgin slots and group collisions use native insertion.
        if(owner!=-2-group)return false;
        M.putLong(p+valueOffset+index*stride,value);
        M.putInt(tag,group);
        long counter=counts+(group-startGroup)*8L;M.putLong(counter,M.getLong(counter)+1);
        return true;
    }
    boolean clear(long key,int group) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long p=page(key);if(p==0)return false;
        long tag=p+((int)key&255)*4L;int owner=M.getInt(tag);
        if(owner==-2-group)return true;
        if(owner!=group)return false;
        M.putInt(tag,-2-group);
        long counter=counts+(group-startGroup)*8L;M.putLong(counter,M.getLong(counter)-1);
        return true;
    }
    boolean append(long key,int group,long value) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long row=row(key,group);
        if(row==0 || (M.getLong(row+24)&1)!=0)return false;
        long end=M.getLong(row+8),limit=M.getLong(row+16);
        if(end==limit)return false;
        M.putLong(end,value);M.putLong(row+8,end+8);listChanged(row);return true;
    }
    boolean writeList(long key,int group,long[] values,boolean append) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long row=row(key,group);
        if(row==0 || (M.getLong(row+24)&1)!=0)return false;
        long begin=M.getLong(row),end=append?M.getLong(row+8):begin;
        long bytes=values.length*8L;
        if(M.getLong(row+16)-end<bytes)return false;
        M.copyMemory(values,ARRAY_BASE,null,end,bytes);M.putLong(row+8,end+bytes);listChanged(row);return true;
    }
    static long[] copyList(long row) {
        long first=M.getLong(row),bytes=M.getLong(row+8)-first;
        if(bytes==0)return null;
        long[] copy=new long[(int)(bytes>>>3)];
        M.copyMemory(null,first,copy,ARRAY_BASE,bytes);
        return copy;
    }
    private static void listChanged(long row) {
        M.putLong(row+24,0);M.putLong(row+32,M.getLong(row+32)+1);
    }
    static boolean cacheListRead(long row) {
        long reads=M.getLong(row+24);
        if((reads&1)!=0 || reads>=16)return true;
        M.putLong(row+24,reads+2);return false;
    }
    private void mapChanged() {M.putOrderedLong(null,epoch,M.getLong(epoch)+1);}
    boolean fullMapView(long key,int group,long[] out) {
        if(pages==0)return false;
        long p=page(key);if(p==0)return false;
        int i=(int)key&255;int owner=M.getInt(p+i*4L);
        if(owner!=group && owner!=-2-group)return false;
        out[0]=0;out[1]=0;out[2]=epoch;out[3]=M.getLongVolatile(null,epoch);
        if(owner!=group || M.getLongVolatile(null,blocked)!=0)return true;
        long row=p+valueOffset+i*stride;
        long span=M.getLong(row+32),used=M.getLong(row+16);
        if(span!=0 && M.getLong(row+24)==0 && M.getLong(used)==span) {
            out[0]=M.getLong(row);out[1]=span;
        }
        return true;
    }
    long mapValueSlot(long key,int group,long mapKey) {
        long row=row(key,group);if(row==0)return 0;
        long span=M.getLong(row+32);if(span==0)return 0;
        long keys=M.getLong(row+24),values=M.getLong(row);
        if(keys!=0) {
            int n=(int)M.getLong(M.getLong(row+16));
            for(int i=0;i<n;i++)if(M.getLong(keys+i*8L)==mapKey)return values+i*8L;
            return 0;
        }
        if(mapKey<0 || mapKey>=span)return 0;
        long bits=M.getLong(M.getLong(row+8)+(mapKey>>>6)*8);
        return (bits&(1L<<(int)mapKey))!=0?values+mapKey*8:0;
    }
    boolean putMap(long key,int group,long mapKey,long value) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long p=page(key);if(p==0)return false;
        int i=(int)key&255;long tag=p+i*4L;int owner=M.getInt(tag);
        if(owner!=group && owner!=-2-group)return false;
        long row=p+valueOffset+i*stride,span=M.getLong(row+32);
        if(span==0)return false;
        long keys=M.getLong(row+24),values=M.getLong(row),used=M.getLong(row+16);
        if(keys!=0) {
            int n=(int)M.getLong(used);
            for(int j=0;j<n;j++)if(M.getLong(keys+j*8L)==mapKey) {
                M.putLong(values+j*8L,value);return true;
            }
            if(n==4)return false;
            M.putLong(keys+n*8L,mapKey);M.putLong(values+n*8L,value);M.putLong(used,n+1);
            if(owner!=group) {
                M.putInt(tag,group);
                long counter=counts+(group-startGroup)*8L;M.putLong(counter,M.getLong(counter)+1);
            }
            mapChanged();return true;
        }
        if(owner!=group || mapKey<0 || mapKey>=span)return false;
        long word=M.getLong(row+8)+(mapKey>>>6)*8,mask=1L<<(int)mapKey,bits=M.getLong(word);
        M.putLong(values+mapKey*8,value);
        if((bits&mask)==0){M.putLong(word,bits|mask);M.putLong(used,M.getLong(used)+1);mapChanged();}
        return true;
    }
    boolean removeMap(long key,int group,long mapKey) {
        if(pages==0 || M.getLongVolatile(null,blocked)!=0)return false;
        long row=row(key,group);if(row==0)return false;
        long span=M.getLong(row+32);if(span==0)return false;
        long used=M.getLong(row+16),n=M.getLong(used),keys=M.getLong(row+24);
        if(keys!=0) {
            long values=M.getLong(row);
            for(int j=0;j<n;j++)if(M.getLong(keys+j*8L)==mapKey) {
                if(n==1)return false;
                M.putLong(keys+j*8L,M.getLong(keys+(n-1)*8));
                M.putLong(values+j*8L,M.getLong(values+(n-1)*8));
                M.putLong(used,n-1);mapChanged();return true;
            }
            return true;
        }
        if(mapKey<0 || mapKey>=span)return true;
        long word=M.getLong(row+8)+(mapKey>>>6)*8,mask=1L<<(int)mapKey,bits=M.getLong(word);
        if((bits&mask)==0)return true;
        if(n==1)return false;
        M.putLong(word,bits&~mask);M.putLong(used,n-1);mapChanged();return true;
    }
    static long read(long address) {return M.getLong(address);}
}
