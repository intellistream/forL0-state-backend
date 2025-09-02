// filepath: /Users/jinyunyang/IdeaProjects/forL0-state-backend/src/main/java/org/apache/flink/runtime/state/heap/io/SerializerPack.java
package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.IOException;

/**
 * SerializerPack 聚合 key/namespace/state 的序列化与复用缓冲，
 * 统一零拷贝输出视图，避免各处重复模板代码。
 * 不改变外部二进制格式，仅封装常用操作。
 */
public class SerializerPack<K, N, S> {
    private final TypeSerializer<K> keySer;
    private final TypeSerializer<N> nsSer;
    private final TypeSerializer<S> stateSer;

    private final ReusableBufferDataOutputView keyOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView nsOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView valOut = new ReusableBufferDataOutputView(128);

    public SerializerPack(TypeSerializer<K> keySer,
                          TypeSerializer<N> nsSer,
                          TypeSerializer<S> stateSer) {
        this.keySer = keySer;
        this.nsSer = nsSer;
        this.stateSer = stateSer;
    }

    public void writeKey(K key) throws IOException {
        keyOut.clear();
        keySer.serialize(key, keyOut);
    }

    public void writeNamespace(N ns) throws IOException {
        nsOut.clear();
        nsSer.serialize(ns, nsOut);
    }

    public void writeState(S state) throws IOException {
        valOut.clear();
        if (state != null) {
            stateSer.serialize(state, valOut);
        }
    }

    public byte[] keyBuffer() { return keyOut.getBuffer(); }
    public int keyLength() { return keyOut.getLength(); }

    public byte[] namespaceBuffer() { return nsOut.getBuffer(); }
    public int namespaceLength() { return nsOut.getLength(); }

    public byte[] stateBuffer() { return valOut.getBuffer(); }
    public int stateLength() { return valOut.getLength(); }

    public TypeSerializer<S> stateSerializer() { return stateSer; }
}

