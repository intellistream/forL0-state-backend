package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;

/**
 * SerializerPack 聚合 key/namespace/state 的序列化与复用缓冲，
 * 支持传统缓冲区模式和零拷贝输出视图，避免各处重复模板代码。
 * 不改变外部二进制格式，仅封装常用操作。
 */
public class SerializerPack<K, N, S> {
    private final TypeSerializer<K> keySer;
    private final TypeSerializer<N> nsSer;
    private final TypeSerializer<S> stateSer;

    // 传统缓冲区模式的输出视图
    private final ReusableBufferDataOutputView keyOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView nsOut = new ReusableBufferDataOutputView(128);
    private final ReusableBufferDataOutputView valOut = new ReusableBufferDataOutputView(128);

    // 零拷贝模式的输出视图（复用实例）
    private final MemorySegmentDataOutputView directOut = new MemorySegmentDataOutputView();

    // 零拷贝模式的输入视图（复用实例）
    private final MemorySegmentDataInputView segInput = new MemorySegmentDataInputView();

    public SerializerPack(TypeSerializer<K> keySer,
                          TypeSerializer<N> nsSer,
                          TypeSerializer<S> stateSer) {
        this.keySer = keySer;
        this.nsSer = nsSer;
        this.stateSer = stateSer;
    }

    // ========== 传统缓冲区模式方法 ==========

    public void writeKey(K key) throws IOException {
        keyOut.clear();
        keySer.serialize(key, keyOut);
    }

    public void writeNamespace(N namespace) throws IOException {
        nsOut.clear();
        nsSer.serialize(namespace, nsOut);
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

    // ========== 零拷贝模式方法 ==========

    /**
     * 直接将key序列化到指定的MemorySegment区域
     * @param key 要序列化的key
     * @param segment 目标MemorySegment
     * @param offset 写入偏移量
     * @param length 可用空间大小
     * @param spaceAllocator 当空间不足时的分配器（可选）
     * @return 实际写入的字节数
     */
    public int writeKeyDirect(K key, MemorySegment segment, int offset, int length,
                             MemorySegmentDataOutputView.SpaceAllocator spaceAllocator) throws IOException {
        directOut.reset(segment, offset, length, spaceAllocator);
        keySer.serialize(key, directOut);
        return directOut.getBytesWritten();
    }

    /**
     * 直接将namespace序列化到指定的MemorySegment区域
     */
    public int writeNamespaceDirect(N namespace, MemorySegment segment, int offset, int length,
                                   MemorySegmentDataOutputView.SpaceAllocator spaceAllocator) throws IOException {
        directOut.reset(segment, offset, length, spaceAllocator);
        nsSer.serialize(namespace, directOut);
        return directOut.getBytesWritten();
    }

    /**
     * 直接将state序列化到指定的MemorySegment区域
     */
    public int writeStateDirect(S state, MemorySegment segment, int offset, int length,
                               MemorySegmentDataOutputView.SpaceAllocator spaceAllocator) throws IOException {
        if (state == null) {
            return 0;
        }

        directOut.reset(segment, offset, length, spaceAllocator);
        stateSer.serialize(state, directOut);
        return directOut.getBytesWritten();
    }

    // ========== 零拷贝输入视图访问器 ==========

    /**
     * 重置内部复用的 MemorySegmentDataInputView 到指定片段/偏移/长度。
     * 调用者随后可以通过 getInputView() 获取已经reset好的视图用于反序列化。
     */
    public void resetInputView(MemorySegment segment, int offset, int length) {
        segInput.reset(segment, offset, length);
    }

    /**
     * 便捷方法：使用内部复用的输入视图对 state 进行反序列化（支持复用实例）。
     * 如果 reuse 为 null，会调用不带复用参数的反序列化方法。
     */
    public S deserializeState(S reuse) throws IOException {
        if (reuse != null) {
            return stateSer.deserialize(reuse, segInput);
        } else {
            return stateSer.deserialize(segInput);
        }
    }

    /**
     * 便捷方法：先重置内部输入视图到指定片段，然后反序列化（支持复用实例）。
     */
    public S deserializeStateFrom(MemorySegment segment, int offset, int length, S reuse) throws IOException {
        resetInputView(segment, offset, length);
        return deserializeState(reuse);
    }

    // ========== 访问器方法 ==========

    public TypeSerializer<K> keySerializer() { return keySer; }
    public TypeSerializer<N> namespaceSerializer() { return nsSer; }
    public TypeSerializer<S> stateSerializer() { return stateSer; }
}
