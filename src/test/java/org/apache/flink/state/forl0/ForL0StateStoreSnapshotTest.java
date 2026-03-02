package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ForL0StateStoreSnapshot}, specifically the optimized 
 * writeStateInKeyGroup that uses O(1) getEntryCount and zero-allocation forEachInKeyGroup.
 */
class ForL0StateStoreSnapshotTest {

    private static final int NUM_KEY_GROUPS = 128;
    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, NUM_KEY_GROUPS - 1);

    // ========== General Namespace Mode ==========

    @Test
    void testWriteStateInKeyGroupEmptyKeyGroup() throws IOException {
        ForL0StateStore<String, Integer, String> store = createGeneralNsStore();
        ForL0StateStoreSnapshot<String, Integer, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        DataOutputSerializer out = new DataOutputSerializer(64);
        snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, 0);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        int count = in.readInt();
        assertEquals(0, count, "Empty key group should write count=0");
    }

    @Test
    void testWriteStateInKeyGroupSingleEntry() throws IOException {
        ForL0StateStore<String, Integer, String> store = createGeneralNsStore();
        String key = "testKey";
        int kg = computeKeyGroup(key);
        store.put(key, 42, "hello", kg);

        ForL0StateStoreSnapshot<String, Integer, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        DataOutputSerializer out = new DataOutputSerializer(256);
        snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        int count = in.readInt();
        assertEquals(1, count);

        // Read: namespace, key, state
        int ns = IntSerializer.INSTANCE.deserialize(in);
        String k = StringSerializer.INSTANCE.deserialize(in);
        String s = StringSerializer.INSTANCE.deserialize(in);

        assertEquals(42, ns);
        assertEquals("testKey", k);
        assertEquals("hello", s);
    }

    @Test
    void testWriteStateInKeyGroupMultipleEntries() throws IOException {
        ForL0StateStore<String, Integer, String> store = createGeneralNsStore();
        
        // Insert multiple entries into same key group with different namespaces
        String key = "testKey";
        int kg = computeKeyGroup(key);
        store.put(key, 1, "v1", kg);
        store.put(key, 2, "v2", kg);
        store.put(key, 3, "v3", kg);

        ForL0StateStoreSnapshot<String, Integer, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        DataOutputSerializer out = new DataOutputSerializer(512);
        snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        int count = in.readInt();
        assertEquals(3, count);

        // Read all entries and verify
        Map<Integer, String> entries = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int ns = IntSerializer.INSTANCE.deserialize(in);
            String k = StringSerializer.INSTANCE.deserialize(in);
            String s = StringSerializer.INSTANCE.deserialize(in);
            assertEquals(key, k);
            entries.put(ns, s);
        }

        assertEquals("v1", entries.get(1));
        assertEquals("v2", entries.get(2));
        assertEquals("v3", entries.get(3));
    }

    @Test
    void testWriteStateInKeyGroupAllKeyGroups() throws IOException {
        ForL0StateStore<String, Integer, String> store = createGeneralNsStore();
        
        // Fill store with many entries across key groups
        Map<String, String> allEntries = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            String value = "value" + i;
            store.put(key, 0, value, computeKeyGroup(key));
            allEntries.put(key, value);
        }

        ForL0StateStoreSnapshot<String, Integer, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        // Write and read all key groups; collect all entries
        Map<String, String> readEntries = new HashMap<>();
        for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
            DataOutputSerializer out = new DataOutputSerializer(1024);
            snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

            DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
            int count = in.readInt();

            for (int j = 0; j < count; j++) {
                IntSerializer.INSTANCE.deserialize(in); // namespace
                String k = StringSerializer.INSTANCE.deserialize(in);
                String s = StringSerializer.INSTANCE.deserialize(in);
                readEntries.put(k, s);
            }
        }

        assertEquals(allEntries, readEntries, "All entries should be serialized and deserialized correctly");
    }

    // ========== VoidNamespace Mode ==========

    @Test
    void testWriteStateInKeyGroupVoidNamespaceEmpty() throws IOException {
        ForL0StateStore<String, VoidNamespace, String> store = createVoidNsStore();
        ForL0StateStoreSnapshot<String, VoidNamespace, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        DataOutputSerializer out = new DataOutputSerializer(64);
        snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, 0);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        assertEquals(0, in.readInt());
    }

    @Test
    void testWriteStateInKeyGroupVoidNamespaceSingle() throws IOException {
        ForL0StateStore<String, VoidNamespace, String> store = createVoidNsStore();
        String key = "testKey";
        int kg = computeKeyGroup(key);
        store.put(key, VoidNamespace.INSTANCE, "hello", kg);

        ForL0StateStoreSnapshot<String, VoidNamespace, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        DataOutputSerializer out = new DataOutputSerializer(256);
        snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        int count = in.readInt();
        assertEquals(1, count);

        VoidNamespace ns = VoidNamespaceSerializer.INSTANCE.deserialize(in);
        String k = StringSerializer.INSTANCE.deserialize(in);
        String s = StringSerializer.INSTANCE.deserialize(in);

        assertSame(VoidNamespace.INSTANCE, ns);
        assertEquals("testKey", k);
        assertEquals("hello", s);
    }

    @Test
    void testWriteStateInKeyGroupVoidNamespaceMultiple() throws IOException {
        ForL0StateStore<String, VoidNamespace, String> store = createVoidNsStore();
        
        Map<String, String> allEntries = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            String key = "key" + i;
            String value = "value" + i;
            store.put(key, VoidNamespace.INSTANCE, value, computeKeyGroup(key));
            allEntries.put(key, value);
        }

        ForL0StateStoreSnapshot<String, VoidNamespace, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        Map<String, String> readEntries = new HashMap<>();
        for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
            DataOutputSerializer out = new DataOutputSerializer(1024);
            snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

            DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
            int count = in.readInt();

            for (int j = 0; j < count; j++) {
                VoidNamespaceSerializer.INSTANCE.deserialize(in); // VoidNamespace byte
                String k = StringSerializer.INSTANCE.deserialize(in);
                String s = StringSerializer.INSTANCE.deserialize(in);
                readEntries.put(k, s);
            }
        }

        assertEquals(allEntries, readEntries);
    }

    // ========== Count Consistency ==========

    @Test
    void testWrittenCountMatchesActualEntries() throws IOException {
        ForL0StateStore<String, Integer, String> store = createGeneralNsStore();
        
        // Insert with deletions to create a complex state
        for (int i = 0; i < 200; i++) {
            String key = "key" + i;
            store.put(key, i % 10, "v" + i, computeKeyGroup(key));
        }
        // Delete some entries
        for (int i = 0; i < 50; i++) {
            String key = "key" + i;
            store.remove(key, i % 10, computeKeyGroup(key));
        }

        ForL0StateStoreSnapshot<String, Integer, String> snapshot = new ForL0StateStoreSnapshot<>(store);

        int totalRead = 0;
        for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
            DataOutputSerializer out = new DataOutputSerializer(4096);
            snapshot.getKeyGroupWriter().writeStateInKeyGroup(out, kg);

            DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
            int count = in.readInt();

            // Read count entries, ensure we can read exactly that many
            for (int j = 0; j < count; j++) {
                IntSerializer.INSTANCE.deserialize(in);
                StringSerializer.INSTANCE.deserialize(in);
                StringSerializer.INSTANCE.deserialize(in);
            }
            totalRead += count;
        }

        assertEquals(150, totalRead, "Should have 200 - 50 = 150 entries total");
        assertEquals(store.size(), totalRead);
    }

    // ========== Helpers ==========

    private ForL0StateStore<String, Integer, String> createGeneralNsStore() {
        RegisteredKeyValueStateBackendMetaInfo<Integer, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "testState",
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        return new ForL0StateStore<>(KEY_GROUP_RANGE, StringSerializer.INSTANCE, metaInfo);
    }

    private ForL0StateStore<String, VoidNamespace, String> createVoidNsStore() {
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "voidNsState",
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        return new ForL0StateStore<>(KEY_GROUP_RANGE, StringSerializer.INSTANCE, metaInfo);
    }

    private int computeKeyGroup(String key) {
        return Math.abs(key.hashCode() % NUM_KEY_GROUPS);
    }
}
