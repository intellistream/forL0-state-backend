package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

import java.io.IOException;

/**
 * KVNode represents a key-value entry stored in heap-off memory.
 * Layout: keyLen(4B) + namespaceLen(4B) + valueLen(4B) + key + namespace + value
 */
public class KVNode {

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    // Field offsets in KVNode layout
    private static final int KEY_LEN_OFFSET = 0;        // 4 bytes
    private static final int NAMESPACE_LEN_OFFSET = 4;  // 4 bytes
    private static final int VALUE_LEN_OFFSET = 8;      // 4 bytes
    private static final int DATA_OFFSET = 12;          // Start of actual data

    private final long baseAddress;
    private final int totalSize;

    /**
     * Creates a KVNode wrapper around existing memory.
     *
     * @param baseAddress Memory address of the KVNode
     */
    public KVNode(long baseAddress) {
        this.baseAddress = baseAddress;

        // Calculate total size from header
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        int namespaceLen = UNSAFE.getInt(baseAddress + NAMESPACE_LEN_OFFSET);
        int valueLen = UNSAFE.getInt(baseAddress + VALUE_LEN_OFFSET);

        this.totalSize = DATA_OFFSET + keyLen + namespaceLen + valueLen;
    }

    /**
     * Creates a new KVNode with serialized data.
     *
     * @param baseAddress Memory address where to create the KVNode
     * @param keySerializer Serializer for key
     * @param namespaceSerializer Serializer for namespace
     * @param stateSerializer Serializer for state value
     * @param key Key object
     * @param namespace Namespace object
     * @param state State value object
     * @return Total size of the created KVNode
     */
    public static <K, N, S> int create(long baseAddress,
                                       TypeSerializer<K> keySerializer,
                                       TypeSerializer<N> namespaceSerializer,
                                       TypeSerializer<S> stateSerializer,
                                       K key, N namespace, S state) throws IOException {

        // Serialize key, namespace, and value
        byte[] keyBytes = serialize(keySerializer, key);
        byte[] namespaceBytes = serialize(namespaceSerializer, namespace);
        byte[] valueBytes = serialize(stateSerializer, state);

        // Write header
        UNSAFE.putInt(baseAddress + KEY_LEN_OFFSET, keyBytes.length);
        UNSAFE.putInt(baseAddress + NAMESPACE_LEN_OFFSET, namespaceBytes.length);
        UNSAFE.putInt(baseAddress + VALUE_LEN_OFFSET, valueBytes.length);

        // Write data
        long dataPtr = baseAddress + DATA_OFFSET;
        copyBytes(keyBytes, dataPtr);
        dataPtr += keyBytes.length;

        copyBytes(namespaceBytes, dataPtr);
        dataPtr += namespaceBytes.length;

        copyBytes(valueBytes, dataPtr);

        return DATA_OFFSET + keyBytes.length + namespaceBytes.length + valueBytes.length;
    }

    /**
     * Updates the value part of this KVNode.
     * Note: This only works if the new value has the same or smaller serialized size.
     *
     * @param stateSerializer Serializer for state value
     * @param newState New state value
     * @return true if update succeeded, false if new value is too large
     */
    public <S> boolean updateValue(TypeSerializer<S> stateSerializer, S newState) throws IOException {
        byte[] newValueBytes = serialize(stateSerializer, newState);
        int currentValueLen = UNSAFE.getInt(baseAddress + VALUE_LEN_OFFSET);

        if (newValueBytes.length > currentValueLen) {
            return false; // Cannot fit larger value
        }

        // Update value length
        UNSAFE.putInt(baseAddress + VALUE_LEN_OFFSET, newValueBytes.length);

        // Update value data
        long valuePtr = getValueAddress();
        copyBytes(newValueBytes, valuePtr);

        return true;
    }

    /**
     * Deserializes and returns the key.
     */
    public <K> K getKey(TypeSerializer<K> keySerializer) throws IOException {
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        byte[] keyBytes = new byte[keyLen];

        long keyPtr = baseAddress + DATA_OFFSET;
        copyFromMemory(keyPtr, keyBytes);

        return deserialize(keySerializer, keyBytes);
    }

    /**
     * Deserializes and returns the namespace.
     */
    public <N> N getNamespace(TypeSerializer<N> namespaceSerializer) throws IOException {
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        int namespaceLen = UNSAFE.getInt(baseAddress + NAMESPACE_LEN_OFFSET);

        byte[] namespaceBytes = new byte[namespaceLen];

        long namespacePtr = baseAddress + DATA_OFFSET + keyLen;
        copyFromMemory(namespacePtr, namespaceBytes);

        return deserialize(namespaceSerializer, namespaceBytes);
    }

    /**
     * Deserializes and returns the state value.
     */
    public <S> S getValue(TypeSerializer<S> stateSerializer) throws IOException {
        int valueLen = UNSAFE.getInt(baseAddress + VALUE_LEN_OFFSET);
        byte[] valueBytes = new byte[valueLen];

        long valuePtr = getValueAddress();
        copyFromMemory(valuePtr, valueBytes);

        return deserialize(stateSerializer, valueBytes);
    }

    /**
     * Checks if this KVNode matches the given key and namespace.
     */
    public <K, N> boolean matches(TypeSerializer<K> keySerializer,
                                  TypeSerializer<N> namespaceSerializer,
                                  K key, N namespace) throws IOException {

        // Quick length check first
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        int namespaceLen = UNSAFE.getInt(baseAddress + NAMESPACE_LEN_OFFSET);

        byte[] keyBytes = serialize(keySerializer, key);
        byte[] namespaceBytes = serialize(namespaceSerializer, namespace);

        if (keyBytes.length != keyLen || namespaceBytes.length != namespaceLen) {
            return false;
        }

        // Compare key bytes
        long keyPtr = baseAddress + DATA_OFFSET;
        if (!compareBytes(keyPtr, keyBytes)) {
            return false;
        }

        // Compare namespace bytes
        long namespacePtr = keyPtr + keyLen;
        return compareBytes(namespacePtr, namespaceBytes);
    }

    /**
     * Gets the raw key bytes for hash calculation.
     */
    public byte[] getRawKeyBytes() {
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        byte[] keyBytes = new byte[keyLen];

        long keyPtr = baseAddress + DATA_OFFSET;
        copyFromMemory(keyPtr, keyBytes);

        return keyBytes;
    }

    /**
     * Gets the raw namespace bytes for hash calculation.
     */
    public byte[] getRawNamespaceBytes() {
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        int namespaceLen = UNSAFE.getInt(baseAddress + NAMESPACE_LEN_OFFSET);

        byte[] namespaceBytes = new byte[namespaceLen];

        long namespacePtr = baseAddress + DATA_OFFSET + keyLen;
        copyFromMemory(namespacePtr, namespaceBytes);

        return namespaceBytes;
    }

    /**
     * Gets the total size of this KVNode.
     */
    public int getTotalSize() {
        return totalSize;
    }

    /**
     * Gets the base address of this KVNode.
     */
    public long getBaseAddress() {
        return baseAddress;
    }

    private long getValueAddress() {
        int keyLen = UNSAFE.getInt(baseAddress + KEY_LEN_OFFSET);
        int namespaceLen = UNSAFE.getInt(baseAddress + NAMESPACE_LEN_OFFSET);

        return baseAddress + DATA_OFFSET + keyLen + namespaceLen;
    }

    private static <T> byte[] serialize(TypeSerializer<T> serializer, T object) throws IOException {
        if (object == null) {
            return new byte[0];
        }

        try {
            org.apache.flink.core.memory.DataOutputSerializer outputSerializer =
                new org.apache.flink.core.memory.DataOutputSerializer(128);
            serializer.serialize(object, outputSerializer);
            return outputSerializer.getCopyOfBuffer();
        } catch (Exception e) {
            throw new IOException("Failed to serialize object", e);
        }
    }

    private static <T> T deserialize(TypeSerializer<T> serializer, byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return null;
        }

        try {
            org.apache.flink.core.memory.DataInputDeserializer deserializer =
                new org.apache.flink.core.memory.DataInputDeserializer(bytes);
            return serializer.deserialize(deserializer);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize object", e);
        }
    }

    private static void copyBytes(byte[] source, long destAddress) {
        for (int i = 0; i < source.length; i++) {
            UNSAFE.putByte(destAddress + i, source[i]);
        }
    }

    private static void copyFromMemory(long sourceAddress, byte[] dest) {
        for (int i = 0; i < dest.length; i++) {
            dest[i] = UNSAFE.getByte(sourceAddress + i);
        }
    }

    private static boolean compareBytes(long memoryAddress, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (UNSAFE.getByte(memoryAddress + i) != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
