package org.apache.flink.benchmark.benchset;

/** Numeric encoding of a task record. All fields have built-in primitive serializers. */
public class BenchsetEvent {
    public long key;
    public long peer;
    public long sequence;
    public long sample;
    public long metric;
    public long features;
    public int kind;
    public int flags;
    public BenchsetEvent() { }
    public BenchsetEvent copy() {
        BenchsetEvent e = new BenchsetEvent();
        e.key=key; e.peer=peer; e.sequence=sequence; e.sample=sample;
        e.metric=metric; e.features=features; e.kind=kind; e.flags=flags;
        return e;
    }
    public static long mix(long x) {
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        return x ^ (x >>> 31);
    }
}
