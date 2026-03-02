package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ForL0StateStore implementation with Swiss Tables architecture.
 * Migrated from ForL0StateMapTest to adapt to the new ForL0StateStore API.
 */
class ForL0StateStoreTest {

    private static final int NUM_KEY_GROUPS = 128;
    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, NUM_KEY_GROUPS - 1);

    private ForL0StateStore<String, Integer, String> stateStore;

    @BeforeEach
    void setUp() {
        RegisteredKeyValueStateBackendMetaInfo<Integer, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "testState",
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        stateStore = new ForL0StateStore<>(KEY_GROUP_RANGE, StringSerializer.INSTANCE, metaInfo);
    }

    private int computeKeyGroup(String key) {
        return Math.abs(key.hashCode() % NUM_KEY_GROUPS);
    }

    @Nested
    class BasicFunctionalityTests {

        @Test
        void testPutAndGet() {
            // Test basic put and get operations
            String key = "testKey";
            Integer namespace = 1;
            String value = "testValue";
            int keyGroup = computeKeyGroup(key);

            // Initially should be empty
            assertEquals(0, stateStore.size());
            assertNull(stateStore.get(key, namespace, keyGroup));

            // Put a value
            stateStore.put(key, namespace, value, keyGroup);
            assertEquals(1, stateStore.size());

            // Get the value back
            String retrievedValue = stateStore.get(key, namespace, keyGroup);
            assertEquals(value, retrievedValue);
        }

        @Test
        void testPutAndGetMultiple() {
            // Test multiple key-value pairs
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                Integer namespace = i;
                String value = "value" + i;
                int keyGroup = computeKeyGroup(key);

                stateStore.put(key, namespace, value, keyGroup);
                assertEquals(value, stateStore.get(key, namespace, keyGroup));
            }

            assertEquals(10, stateStore.size());

            // Verify all values are still there
            for (int i = 0; i < 10; i++) {
                String key = "key" + i;
                Integer namespace = i;
                String expectedValue = "value" + i;
                int keyGroup = computeKeyGroup(key);
                assertEquals(expectedValue, stateStore.get(key, namespace, keyGroup));
            }
        }

        @Test
        void testUpdate() {
            String key = "updateKey";
            Integer namespace = 100;
            String initialValue = "initialValue";
            String updatedValue = "updatedValue";
            int keyGroup = computeKeyGroup(key);

            // Put initial value
            stateStore.put(key, namespace, initialValue, keyGroup);
            assertEquals(1, stateStore.size());
            assertEquals(initialValue, stateStore.get(key, namespace, keyGroup));

            // Update the value
            stateStore.put(key, namespace, updatedValue, keyGroup);
            assertEquals(1, stateStore.size()); // Size should remain the same
            assertEquals(updatedValue, stateStore.get(key, namespace, keyGroup));
        }

        @Test
        void testContainsKey() {
            String key = "containsKey";
            Integer namespace = 200;
            String value = "containsValue";
            int keyGroup = computeKeyGroup(key);

            // Initially should not contain the key
            assertFalse(stateStore.containsKey(key, namespace, keyGroup));

            // Put a value
            stateStore.put(key, namespace, value, keyGroup);

            // Now should contain the key
            assertTrue(stateStore.containsKey(key, namespace, keyGroup));
        }

        @Test
        void testRemove() {
            String key = "removeKey";
            Integer namespace = 300;
            String value = "removeValue";
            int keyGroup = computeKeyGroup(key);

            // Put a value
            stateStore.put(key, namespace, value, keyGroup);
            assertEquals(1, stateStore.size());
            assertTrue(stateStore.containsKey(key, namespace, keyGroup));

            // Remove the value
            String removed = stateStore.remove(key, namespace, keyGroup);
            assertEquals(value, removed);
            assertEquals(0, stateStore.size());
            assertFalse(stateStore.containsKey(key, namespace, keyGroup));
            assertNull(stateStore.get(key, namespace, keyGroup));
        }

        @Test
        void testRemoveNonExistent() {
            String key = "nonExistentKey";
            Integer namespace = 999;
            int keyGroup = computeKeyGroup(key);

            String removed = stateStore.remove(key, namespace, keyGroup);
            assertNull(removed);
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testEmptyStringValues() {
            String key = "emptyKey";
            Integer namespace = 900;
            String emptyValue = "";
            int keyGroup = computeKeyGroup(key);

            stateStore.put(key, namespace, emptyValue, keyGroup);
            assertEquals(emptyValue, stateStore.get(key, namespace, keyGroup));
            assertTrue(stateStore.containsKey(key, namespace, keyGroup));
        }

        @Test
        void testNamespaceIsolation() {
            String key = "sameKey";
            String value1 = "value1";
            String value2 = "value2";
            int keyGroup = computeKeyGroup(key);

            // Put same key in different namespaces
            stateStore.put(key, 1, value1, keyGroup);
            stateStore.put(key, 2, value2, keyGroup);

            assertEquals(2, stateStore.size());
            assertEquals(value1, stateStore.get(key, 1, keyGroup));
            assertEquals(value2, stateStore.get(key, 2, keyGroup));

            // Remove one namespace
            stateStore.remove(key, 1, keyGroup);
            assertEquals(1, stateStore.size());
            assertNull(stateStore.get(key, 1, keyGroup));
            assertEquals(value2, stateStore.get(key, 2, keyGroup));
        }

        @Test
        void testNamespaceCleanupWhenEmpty() {
            // Test that empty namespace maps are properly cleaned up
            // This is critical for windowed operations to avoid memory leaks
            String key1 = "key1";
            String key2 = "key2";
            int keyGroup = computeKeyGroup(key1);
            
            // Put entries in namespace 100
            stateStore.put(key1, 100, "v1", keyGroup);
            stateStore.put(key2, 100, "v2", keyGroup);
            assertEquals(2, stateStore.sizeOfNamespace(100));
            
            // Put entry in namespace 200
            stateStore.put(key1, 200, "v3", keyGroup);
            assertEquals(1, stateStore.sizeOfNamespace(200));
            
            // Remove all entries from namespace 100
            stateStore.remove(key1, 100, keyGroup);
            assertEquals(1, stateStore.sizeOfNamespace(100));
            stateStore.remove(key2, 100, keyGroup);
            assertEquals(0, stateStore.sizeOfNamespace(100));
            
            // Namespace 200 should still work correctly
            assertEquals("v3", stateStore.get(key1, 200, keyGroup));
            assertEquals(1, stateStore.sizeOfNamespace(200));
            
            // Re-add to namespace 100 should work (namespace was cleaned up, not corrupted)
            stateStore.put(key1, 100, "newValue", keyGroup);
            assertEquals(1, stateStore.sizeOfNamespace(100));
            assertEquals("newValue", stateStore.get(key1, 100, keyGroup));
        }

        @Test
        void testMultipleNamespaceCleanupInSameKeyGroup() {
            // Test cleanup of multiple namespaces in same key group
            String key = "testKey";
            int keyGroup = computeKeyGroup(key);
            
            // Create entries in 10 different namespaces
            for (int ns = 0; ns < 10; ns++) {
                stateStore.put(key, ns, "value" + ns, keyGroup);
            }
            assertEquals(10, stateStore.size());
            
            // Remove all namespaces one by one
            for (int ns = 0; ns < 10; ns++) {
                stateStore.remove(key, ns, keyGroup);
                assertEquals(0, stateStore.sizeOfNamespace(ns));
            }
            assertEquals(0, stateStore.size());
            
            // Verify we can still add new entries after cleanup
            stateStore.put(key, 999, "newValue", keyGroup);
            assertEquals(1, stateStore.size());
            assertEquals("newValue", stateStore.get(key, 999, keyGroup));
        }
    }

    @Nested
    class IterationAndNamespaceTests {

        @Test
        void testGetKeysByNamespace() {
            // ns 1
            String keyA = "a";
            String keyB = "b";
            String keyC = "c";
            stateStore.put(keyA, 1, "v1", computeKeyGroup(keyA));
            stateStore.put(keyB, 1, "v2", computeKeyGroup(keyB));
            // ns 2
            stateStore.put(keyC, 2, "v3", computeKeyGroup(keyC));

            Set<String> keysNs1 = stateStore.getKeys(1).collect(Collectors.toSet());

            assertEquals(2, keysNs1.size());
            assertTrue(keysNs1.contains("a"));
            assertTrue(keysNs1.contains("b"));
            assertFalse(keysNs1.contains("c"));
        }

        @Test
        void testSizeOfNamespace() {
            // ns 10
            stateStore.put("k1", 10, "v1", computeKeyGroup("k1"));
            stateStore.put("k2", 10, "v2", computeKeyGroup("k2"));
            stateStore.put("k3", 10, "v3", computeKeyGroup("k3"));
            // ns 11
            stateStore.put("k4", 11, "v4", computeKeyGroup("k4"));

            assertEquals(3, stateStore.sizeOfNamespace(10));
            assertEquals(1, stateStore.sizeOfNamespace(11));
            assertEquals(0, stateStore.sizeOfNamespace(12));
        }

        @Test
        void testEntriesIteration() {
            // Put entries across different key groups
            for (int i = 0; i < 20; i++) {
                String key = "key" + i;
                stateStore.put(key, i, "value" + i, computeKeyGroup(key));
            }

            // Collect all entries via iteration
            Set<String> foundKeys = new HashSet<>();
            for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
                for (StateEntry<String, Integer, String> entry : stateStore.entries(kg)) {
                    foundKeys.add(entry.getKey());
                }
            }

            assertEquals(20, foundKeys.size());
            for (int i = 0; i < 20; i++) {
                assertTrue(foundKeys.contains("key" + i));
            }
        }

        @Test
        void testGetKeysAndNamespaces() {
            stateStore.put("k1", 1, "v1", computeKeyGroup("k1"));
            stateStore.put("k2", 2, "v2", computeKeyGroup("k2"));
            stateStore.put("k3", 3, "v3", computeKeyGroup("k3"));

            List<Tuple2<String, Integer>> tuples = stateStore.getKeysAndNamespaces().collect(Collectors.toList());
            assertEquals(3, tuples.size());

            Set<String> keys = tuples.stream().map(t -> t.f0).collect(Collectors.toSet());
            assertTrue(keys.contains("k1"));
            assertTrue(keys.contains("k2"));
            assertTrue(keys.contains("k3"));
        }
    }

    @Nested
    class KeyGroupTests {

        @Test
        void testDifferentKeyGroups() {
            // Test that entries in different key groups are stored separately
            Set<Integer> usedKeyGroups = new HashSet<>();
            
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                int keyGroup = computeKeyGroup(key);
                usedKeyGroups.add(keyGroup);
                stateStore.put(key, 0, "value" + i, keyGroup);
            }

            assertEquals(100, stateStore.size());
            
            // Verify data distributed across multiple key groups
            assertTrue(usedKeyGroups.size() > 1, "Keys should be distributed across multiple key groups");

            // Verify all entries can be retrieved
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                int keyGroup = computeKeyGroup(key);
                assertEquals("value" + i, stateStore.get(key, 0, keyGroup));
            }
        }

        @Test
        void testKeyGroupIsolation() {
            // Same key string but computed for correct key group
            String key = "isolationKey";
            int correctKeyGroup = computeKeyGroup(key);
            int wrongKeyGroup = (correctKeyGroup + 1) % NUM_KEY_GROUPS;

            stateStore.put(key, 1, "value", correctKeyGroup);

            // Should find in correct key group
            assertEquals("value", stateStore.get(key, 1, correctKeyGroup));
            
            // Should not find in wrong key group
            assertNull(stateStore.get(key, 1, wrongKeyGroup));
        }
    }

    @Nested
    class SnapshotTests {

        @Test
        void testStateSnapshot() {
            stateStore.put("k1", 1, "v1", computeKeyGroup("k1"));
            stateStore.put("k2", 2, "v2", computeKeyGroup("k2"));

            StateSnapshot snapshot = stateStore.stateSnapshot();
            assertNotNull(snapshot);
            assertNotNull(snapshot.getMetaInfoSnapshot());
        }
    }

    @Nested
    class EntryCountAndForEachTests {

        // ========== getEntryCount tests (General Namespace) ==========

        @Test
        void testGetEntryCountEmpty() {
            // Empty key group should return 0
            assertEquals(0, stateStore.getEntryCount(0));
        }

        @Test
        void testGetEntryCountSingleNamespace() {
            String key1 = "a";
            String key2 = "b";
            int kg = computeKeyGroup(key1);
            // Ensure both keys hash to same key group for this test
            stateStore.put(key1, 1, "v1", kg);
            stateStore.put(key2, 1, "v2", kg);

            assertEquals(2, stateStore.getEntryCount(kg));
        }

        @Test
        void testGetEntryCountMultipleNamespaces() {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            
            stateStore.put(key, 1, "v1", kg);
            stateStore.put(key, 2, "v2", kg);
            stateStore.put(key, 3, "v3", kg);

            assertEquals(3, stateStore.getEntryCount(kg));
        }

        @Test
        void testGetEntryCountAfterRemove() {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            
            stateStore.put(key, 1, "v1", kg);
            stateStore.put(key, 2, "v2", kg);
            assertEquals(2, stateStore.getEntryCount(kg));

            stateStore.remove(key, 1, kg);
            assertEquals(1, stateStore.getEntryCount(kg));

            stateStore.remove(key, 2, kg);
            assertEquals(0, stateStore.getEntryCount(kg));
        }

        @Test
        void testGetEntryCountMatchesSize() {
            // Total of getEntryCount across all key groups should equal size()
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                stateStore.put(key, i % 10, "v" + i, computeKeyGroup(key));
            }

            int totalCount = 0;
            for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
                totalCount += stateStore.getEntryCount(kg);
            }
            assertEquals(stateStore.size(), totalCount);
        }

        // ========== forEachInKeyGroup tests (General Namespace) ==========

        @Test
        void testForEachInKeyGroupEmpty() throws IOException {
            int[] count = {0};
            stateStore.forEachInKeyGroup(0, (key, ns, state) -> count[0]++);
            assertEquals(0, count[0]);
        }

        @Test
        void testForEachInKeyGroupSingleEntry() throws IOException {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            stateStore.put(key, 42, "hello", kg);

            List<String> keys = new ArrayList<>();
            List<Integer> namespaces = new ArrayList<>();
            List<String> states = new ArrayList<>();

            stateStore.forEachInKeyGroup(kg, (k, ns, s) -> {
                keys.add(k);
                namespaces.add(ns);
                states.add(s);
            });

            assertEquals(1, keys.size());
            assertEquals(key, keys.get(0));
            assertEquals(42, (int) namespaces.get(0));
            assertEquals("hello", states.get(0));
        }

        @Test
        void testForEachInKeyGroupMultipleNamespaces() throws IOException {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            
            stateStore.put(key, 1, "v1", kg);
            stateStore.put(key, 2, "v2", kg);
            stateStore.put(key, 3, "v3", kg);

            Set<Integer> namespaces = new HashSet<>();
            Set<String> values = new HashSet<>();

            stateStore.forEachInKeyGroup(kg, (k, ns, s) -> {
                assertEquals(key, k);
                namespaces.add(ns);
                values.add(s);
            });

            assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), namespaces);
            assertEquals(new HashSet<>(Arrays.asList("v1", "v2", "v3")), values);
        }

        @Test
        void testForEachInKeyGroupConsistentWithEntries() throws IOException {
            // Fill multiple key groups
            for (int i = 0; i < 50; i++) {
                String key = "key" + i;
                stateStore.put(key, i % 5, "v" + i, computeKeyGroup(key));
            }

            for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
                // Collect via entries() iterator
                Set<String> entriesKeys = new HashSet<>();
                for (StateEntry<String, Integer, String> entry : stateStore.entries(kg)) {
                    entriesKeys.add(entry.getKey());
                }

                // Collect via forEachInKeyGroup
                Set<String> forEachKeys = new HashSet<>();
                stateStore.forEachInKeyGroup(kg, (k, ns, s) -> forEachKeys.add(k));

                assertEquals(entriesKeys, forEachKeys, "KeyGroup " + kg + " mismatch");
            }
        }

        @Test
        void testForEachInKeyGroupExceptionPropagation() {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            stateStore.put(key, 1, "v1", kg);

            assertThrows(IOException.class, () ->
                stateStore.forEachInKeyGroup(kg, (k, ns, s) -> {
                    throw new IOException("test");
                })
            );
        }

        @Test
        void testForEachInKeyGroupCountMatchesGetEntryCount() throws IOException {
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                stateStore.put(key, i % 10, "v" + i, computeKeyGroup(key));
            }

            for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
                int expectedCount = stateStore.getEntryCount(kg);
                int[] actualCount = {0};
                stateStore.forEachInKeyGroup(kg, (k, ns, s) -> actualCount[0]++);
                assertEquals(expectedCount, actualCount[0], "KeyGroup " + kg + " count mismatch");
            }
        }
    }

    @Nested
    class VoidNamespaceTests {

        private ForL0StateStore<String, VoidNamespace, String> voidNsStore;

        @BeforeEach
        void setUp() {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "voidNsState",
                            VoidNamespaceSerializer.INSTANCE,
                            StringSerializer.INSTANCE);
            voidNsStore = new ForL0StateStore<>(KEY_GROUP_RANGE, StringSerializer.INSTANCE, metaInfo);
        }

        @Test
        void testGetEntryCountVoidNamespaceEmpty() {
            assertEquals(0, voidNsStore.getEntryCount(0));
        }

        @Test
        void testGetEntryCountVoidNamespace() {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            voidNsStore.put(key, VoidNamespace.INSTANCE, "v1", kg);
            assertEquals(1, voidNsStore.getEntryCount(kg));
        }

        @Test
        void testGetEntryCountVoidNamespaceMultiple() {
            // Insert multiple keys that hash to same key group
            String fixedKg = "ka"; // pick a key, then add more to same kg
            int kg = computeKeyGroup(fixedKg);
            voidNsStore.put(fixedKg, VoidNamespace.INSTANCE, "v1", kg);

            // Add entries across different key groups
            for (int i = 0; i < 20; i++) {
                String key = "vk" + i;
                voidNsStore.put(key, VoidNamespace.INSTANCE, "v" + i, computeKeyGroup(key));
            }

            // Total should match size
            int total = 0;
            for (int kgi = 0; kgi < NUM_KEY_GROUPS; kgi++) {
                total += voidNsStore.getEntryCount(kgi);
            }
            assertEquals(voidNsStore.size(), total);
        }

        @Test
        void testForEachInKeyGroupVoidNamespaceEmpty() throws IOException {
            int[] count = {0};
            voidNsStore.forEachInKeyGroup(0, (key, ns, state) -> count[0]++);
            assertEquals(0, count[0]);
        }

        @Test
        void testForEachInKeyGroupVoidNamespace() throws IOException {
            String key = "testKey";
            int kg = computeKeyGroup(key);
            voidNsStore.put(key, VoidNamespace.INSTANCE, "hello", kg);

            List<String> keys = new ArrayList<>();
            List<Object> namespaces = new ArrayList<>();
            List<String> states = new ArrayList<>();

            voidNsStore.forEachInKeyGroup(kg, (k, ns, s) -> {
                keys.add(k);
                namespaces.add(ns);
                states.add(s);
            });

            assertEquals(1, keys.size());
            assertEquals(key, keys.get(0));
            assertSame(VoidNamespace.INSTANCE, namespaces.get(0));
            assertEquals("hello", states.get(0));
        }

        @Test
        void testForEachInKeyGroupVoidNamespaceConsistentWithEntries() throws IOException {
            for (int i = 0; i < 50; i++) {
                String key = "key" + i;
                voidNsStore.put(key, VoidNamespace.INSTANCE, "v" + i, computeKeyGroup(key));
            }

            for (int kg = 0; kg < NUM_KEY_GROUPS; kg++) {
                Set<String> entriesKeys = new HashSet<>();
                for (StateEntry<String, VoidNamespace, String> entry : voidNsStore.entries(kg)) {
                    entriesKeys.add(entry.getKey());
                }

                Set<String> forEachKeys = new HashSet<>();
                voidNsStore.forEachInKeyGroup(kg, (k, ns, s) -> {
                    assertSame(VoidNamespace.INSTANCE, ns);
                    forEachKeys.add(k);
                });

                assertEquals(entriesKeys, forEachKeys, "KeyGroup " + kg + " mismatch");
            }
        }

        @Test
        void testForEachInKeyGroupVoidNamespaceAfterRemove() throws IOException {
            String key1 = "a";
            String key2 = "b";
            int kg1 = computeKeyGroup(key1);
            int kg2 = computeKeyGroup(key2);

            voidNsStore.put(key1, VoidNamespace.INSTANCE, "v1", kg1);
            voidNsStore.put(key2, VoidNamespace.INSTANCE, "v2", kg2);

            voidNsStore.remove(key1, VoidNamespace.INSTANCE, kg1);

            int[] count1 = {0};
            voidNsStore.forEachInKeyGroup(kg1, (k, ns, s) -> count1[0]++);
            // If key1 and key2 are in same kg, count is 1; if different, count is 0
            if (kg1 == kg2) {
                assertEquals(1, count1[0]);
            } else {
                assertEquals(0, count1[0]);
            }
        }
    }

    @Nested
    class HighLoadTests {

        @Test
        void testLargeNumberOfEntries() {
            int count = 10000;
            
            for (int i = 0; i < count; i++) {
                String key = "largeKey" + i;
                stateStore.put(key, i % 100, "value" + i, computeKeyGroup(key));
            }

            assertEquals(count, stateStore.size());

            // Verify random samples
            for (int i = 0; i < count; i += 100) {
                String key = "largeKey" + i;
                assertEquals("value" + i, stateStore.get(key, i % 100, computeKeyGroup(key)));
            }
        }

        @Test
        void testManyUpdates() {
            String key = "updateKey";
            int keyGroup = computeKeyGroup(key);
            
            for (int i = 0; i < 1000; i++) {
                stateStore.put(key, 0, "value" + i, keyGroup);
            }

            assertEquals(1, stateStore.size());
            assertEquals("value999", stateStore.get(key, 0, keyGroup));
        }

        @Test
        void testManyNamespaces() {
            String key = "nsKey";
            int keyGroup = computeKeyGroup(key);
            int numNamespaces = 500;

            for (int ns = 0; ns < numNamespaces; ns++) {
                stateStore.put(key, ns, "value" + ns, keyGroup);
            }

            assertEquals(numNamespaces, stateStore.size());

            for (int ns = 0; ns < numNamespaces; ns++) {
                assertEquals("value" + ns, stateStore.get(key, ns, keyGroup));
            }
        }
    }
}
