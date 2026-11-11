package org.apache.flink.state.forl0;

/** Direct view of the native shared row pool; the owning subtask is its sole writer. */
final class PrimitiveRowCacheAccess {
    private static final sun.misc.Unsafe M=org.apache.flink.core.memory.MemoryUtils.UNSAFE;
    private final long handle,base,count,lookups,hits,fills,evictions,hitLines,bypasses,viewEpoch,blocked,stateId,contentEpoch,frozen,closed;
    private final boolean resident;
    private PrimitiveRowCacheAccess(long handle,long[] d) {
        this.handle=handle;
        base=d[0];count=d[1];lookups=d[2];hits=d[3];fills=d[4];evictions=d[5];hitLines=d[6];bypasses=d[7];
        viewEpoch=d[8];blocked=d[9];stateId=d[10];contentEpoch=d[11];frozen=d[12];closed=d[13];resident=d[14]!=0;
    }
    static PrimitiveRowCacheAccess create(long handle) {
        long[] d=NativeEngine.mapRowCacheLayout(handle);return d==null?null:new PrimitiveRowCacheAccess(handle,d);
    }
    private int admissionClock;
    private final int admissionInterval=readAdmissionInterval();
    private static int readAdmissionInterval() {
        String value=System.getenv("FORL0_L0_ROW_ADMISSION_INTERVAL");
        int interval=value==null?16:Integer.parseInt(value);
        if(interval<1 || (interval&(interval-1))!=0)throw new IllegalArgumentException("L0 row admission interval must be a power of two");
        return interval;
    }
    private boolean admit(){return (++admissionClock&(admissionInterval-1))==0;}
    private static long mix(long k){k^=k>>>33;k*=0xff51afd7ed558ccdL;k^=k>>>33;k*=0xc4ceb9fe1a85ec53L;return k^(k>>>33);}
    private long set(long key){long h=mix(key^mix(stateId));return base+(((h&0xffffffffL)*count)>>>32)*2304;}
    private static void add(long address,long value){M.putOrderedLong(null,address,M.getLong(address)+value);}
    boolean admissionClosed(){return resident && M.getLongVolatile(null,closed)!=0;}
    boolean resolve(PrimitiveLongAccess directory,long key,int group,long[] out) {
        if(admissionClosed())return directory.fullMapView(key,group,out);
        add(lookups,1);
        if(directory.fullMapView(key,group,out)) {
            if(out[0]==0 || out[1]!=64 || M.getLongVolatile(null,blocked)!=0 || M.getLongVolatile(null,frozen)!=0) {
                add(bypasses,1);return true;
            }
            if(out[0]>=base && out[0]<base+count*2304) {add(hits,1);add(hitLines,8);return true;}
            // Keep the original authoritative home view on rejected misses.
            // Sampling admissions reduces copy/write-back churn on scan traffic.
            return !admit();
        }
        if(M.getLongVolatile(null,blocked)!=0 || M.getLongVolatile(null,frozen)!=0) {
            NativeEngine.mapLongLongDenseView(handle,key,group,out);add(bypasses,1);return true;
        }
        long s=set(key),epoch=M.getLongVolatile(null,contentEpoch);int valid=M.getInt(s+152);
        for(int i=0;i<4;i++)if((valid&(1<<i))!=0 && M.getLong(s+i*8L)==key && M.getLong(s+32+i*8L)==stateId
                && M.getInt(s+128+i*4L)==group && M.getLong(s+64+i*8L)==epoch) {
            add(hits,1);add(hitLines,8);
            out[0]=s+256+i*512L;out[1]=64;out[2]=viewEpoch;out[3]=M.getLongVolatile(null,viewEpoch);
            return true;
        }
        if(!admit()) {
            NativeEngine.mapLongLongDenseView(handle,key,group,out);
            if(out[0]==0 || out[1]!=64)add(bypasses,1);
            return true;
        }
        return false;
    }
}
