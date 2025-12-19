package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;

/**
 * SerializerPack 聚合 key/namespace/state 的序列化与复用缓冲，
 * 支持传统缓冲区模式和零拷贝输出视图，避免各处重复模板代码。
 * 不改变外部二进制格式，仅封装常用操作。
 * 
 * <p>Performance optimizations:
 * <ul>
 *   <li>Pre-allocated 256-byte buffers to avoid grow() calls for most key/namespace sizes</li>
 *   <li>Fast path for TimeWindow namespace serialization (fixed 16 bytes)</li>
 *   <li>Cached lengths returned directly from write methods</li>
 * </ul>
 */
public class SerializerPack<K, N, S> {
    private final TypeSerializer<K> keySer;
    private final TypeSerializer<N> nsSer;
    private final TypeSerializer<S> stateSer;

    // Pre-allocated buffers with larger initial capacity (256 bytes)
    // to avoid grow() calls for most key/namespace combinations.
    // Typical sizes: String key ~20-100 bytes, TimeWindow namespace = 16 bytes
    private static final int DEFAULT_BUFFER_SIZE = 256;
    
    // 传统缓冲区模式的输出视图
    private final ReusableBufferDataOutputView keyOut = new ReusableBufferDataOutputView(DEFAULT_BUFFER_SIZE);
    private final ReusableBufferDataOutputView nsOut = new ReusableBufferDataOutputView(DEFAULT_BUFFER_SIZE);
    private final ReusableBufferDataOutputView valOut = new ReusableBufferDataOutputView(DEFAULT_BUFFER_SIZE);
    
    // Flag to enable TimeWindow fast path
    private final boolean isTimeWindowNamespace;

    // 零拷贝模式的输入视图（复用实例）
    private final MemorySegmentDataInputView segInput = new MemorySegmentDataInputView();

    public SerializerPack(TypeSerializer<K> keySer,
                          TypeSerializer<N> nsSer,
                          TypeSerializer<S> stateSer) {
        this.keySer = keySer;
        this.nsSer = nsSer;
        this.stateSer = stateSer;
        
        // Detect TimeWindow namespace for fast path optimization
        // TimeWindow.Serializer class name pattern matching
        String nsSerClassName = nsSer.getClass().getName();
        this.isTimeWindowNamespace = nsSerClassName.contains("TimeWindow");
    }

    // ========== 传统缓冲区模式方法 ==========

    // Cached lengths to avoid repeated getLength() calls
    private int cachedKeyLength;
    private int cachedNsLength;
    private int cachedStateLength;

    /**
     * Serialize key and return the serialized length.
     * The length is cached for subsequent calls to keyLength().
     */
    public int writeKey(K key) throws IOException {
        keyOut.clear();
        keySer.serialize(key, keyOut);
        cachedKeyLength = keyOut.getLength();
        return cachedKeyLength;
    }

    /**
     * Serialize namespace and return the serialized length.
     * The length is cached for subsequent calls to namespaceLength().
     * 
     * <p>Fast path for TimeWindow: directly writes 2 longs (16 bytes) without
     * going through the generic serializer, avoiding virtual method calls.
     */
    public int writeNamespace(N namespace) throws IOException {
        nsOut.clear();
        
        // Fast path for TimeWindow: fixed 16 bytes (start + end as 2 longs)
        if (isTimeWindowNamespace && namespace instanceof TimeWindow) {
            TimeWindow tw = (TimeWindow) namespace;
            nsOut.writeLong(tw.getStart());
            nsOut.writeLong(tw.getEnd());
            cachedNsLength = 16;
            return cachedNsLength;
        }
        
        // Generic path for other namespace types
        nsSer.serialize(namespace, nsOut);
        cachedNsLength = nsOut.getLength();
        return cachedNsLength;
    }

    /**
     * Serialize state and return the serialized length.
     * The length is cached for subsequent calls to stateLength().
     */
    public int writeState(S state) throws IOException {
        valOut.clear();
        if (state != null) {
            stateSer.serialize(state, valOut);
        }
        cachedStateLength = valOut.getLength();
        return cachedStateLength;
    }

    public byte[] keyBuffer() { return keyOut.getBuffer(); }
    public int keyLength() { return cachedKeyLength; }

    public byte[] namespaceBuffer() { return nsOut.getBuffer(); }
    public int namespaceLength() { return cachedNsLength; }

    public byte[] stateBuffer() { return valOut.getBuffer(); }
    public int stateLength() { return cachedStateLength; }

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
    
    /**
     * 从 byte[] 反序列化状态。用于大对象跨多个 segment 的情况。
     */
    public S deserializeStateFromBytes(byte[] bytes) throws IOException {
        org.apache.flink.core.memory.DataInputDeserializer input = new org.apache.flink.core.memory.DataInputDeserializer(bytes);
        return stateSer.deserialize(input);
    }

    // ========== 访问器方法 ==========

    public TypeSerializer<K> keySerializer() { return keySer; }
    public TypeSerializer<N> namespaceSerializer() { return nsSer; }
    public TypeSerializer<S> stateSerializer() { return stateSer; }
}
