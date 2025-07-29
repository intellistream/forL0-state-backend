package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class KVNodeTest {

    private static final int DEFAULT_PAGE_SIZE = 32 * 1024; // 32KB
    private static final long DEFAULT_MEMORY_SIZE = 64L * DEFAULT_PAGE_SIZE; // 2MB

    private MemoryManager memoryManager;
    private MemoryManagerAllocator allocator;
    private EntryArena arena;
    private Object owner;

    // Serializers for testing
    private final StringSerializer stringSerializer = StringSerializer.INSTANCE;
    private final IntSerializer intSerializer = IntSerializer.INSTANCE;
    private final LongSerializer longSerializer = LongSerializer.INSTANCE;

    @BeforeEach
    void setUp() {
        memoryManager = MemoryManagerBuilder.newBuilder()
                .setMemorySize(DEFAULT_MEMORY_SIZE)
                .setPageSize(DEFAULT_PAGE_SIZE)
                .build();
        owner = new Object();
        allocator = new MemoryManagerAllocator(memoryManager, owner);
        arena = new EntryArena(allocator);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (arena != null) {
            arena.close();
        }
        if (allocator != null && !allocator.isClosed()) {
            allocator.close();
        }
        if (memoryManager != null) {
            memoryManager.shutdown();
        }
    }

    @Nested
    class BasicFunctionalityTests {

        @Test
        void testCreateKVNode() throws IOException {
            String key = "testKey";
            Integer namespace = 42;
            String state = "testState";

            long address = arena.allocate(1024);
            int totalSize = KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            assertTrue(totalSize > 0);

            KVNode kvNode = new KVNode(address);
            assertEquals(totalSize, kvNode.getTotalSize());
            assertEquals(address, kvNode.getBaseAddress());
        }

        @Test
        void testGetKey() throws IOException {
            String expectedKey = "myTestKey";
            Integer namespace = 123;
            String state = "myState";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    expectedKey, namespace, state);

            KVNode kvNode = new KVNode(address);
            String actualKey = kvNode.getKey(stringSerializer);

            assertEquals(expectedKey, actualKey);
        }

        @Test
        void testGetNamespace() throws IOException {
            String key = "testKey";
            Integer expectedNamespace = 456;
            String state = "testState";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, expectedNamespace, state);

            KVNode kvNode = new KVNode(address);
            Integer actualNamespace = kvNode.getNamespace(intSerializer);

            assertEquals(expectedNamespace, actualNamespace);
        }

        @Test
        void testGetValue() throws IOException {
            String key = "testKey";
            Integer namespace = 789;
            String expectedState = "myTestState";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, expectedState);

            KVNode kvNode = new KVNode(address);
            String actualState = kvNode.getValue(stringSerializer);

            assertEquals(expectedState, actualState);
        }

        @Test
        void testUpdateValue() throws IOException {
            String key = "testKey";
            Integer namespace = 999;
            String originalState = "original";
            String newState = "updated";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);

            // Update should succeed if new value is same size or smaller
            boolean updated = kvNode.updateValue(stringSerializer, newState);
            assertTrue(updated);

            String retrievedState = kvNode.getValue(stringSerializer);
            assertEquals(newState, retrievedState);
        }

        @Test
        void testMatches() throws IOException {
            String key = "matchKey";
            Integer namespace = 111;
            String state = "matchState";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);

            // Should match exact key and namespace
            assertTrue(kvNode.matches(stringSerializer, intSerializer, key, namespace));

            // Should not match different key
            assertFalse(kvNode.matches(stringSerializer, intSerializer, "differentKey", namespace));

            // Should not match different namespace
            assertFalse(kvNode.matches(stringSerializer, intSerializer, key, 222));
        }
    }

    @Nested
    class SerializationTests {

        @Test
        void testDifferentKeyTypes() throws IOException {
            // Test with Long key
            Long longKey = 12345L;
            Integer namespace = 1;
            String state = "longKeyState";

            long address = arena.allocate(1024);
            KVNode.create(address, longSerializer, intSerializer, stringSerializer,
                    longKey, namespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(longKey, kvNode.getKey(longSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(state, kvNode.getValue(stringSerializer));
        }

        @Test
        void testDifferentNamespaceTypes() throws IOException {
            String key = "testKey";
            String stringNamespace = "testNamespace";
            Long state = 999L;

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, stringSerializer, longSerializer,
                    key, stringNamespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(stringNamespace, kvNode.getNamespace(stringSerializer));
            assertEquals(state, kvNode.getValue(longSerializer));
        }

        @Test
        void testDifferentStateTypes() throws IOException {
            String key = "testKey";
            Integer namespace = 42;
            Integer intState = 777;

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, intSerializer,
                    key, namespace, intState);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(intState, kvNode.getValue(intSerializer));
        }

        @Test
        void testNullValues() throws IOException {
            String key = "testKey";
            Integer namespace = null; // Null namespace
            String state = null; // Null state

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertNull(kvNode.getNamespace(intSerializer));
            assertNull(kvNode.getValue(stringSerializer));
        }

        @Test
        void testEmptyStrings() throws IOException {
            String key = "";
            Integer namespace = 1;
            String state = "";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(state, kvNode.getValue(stringSerializer));
        }

        @Test
        void testLargeStrings() throws IOException {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("x");
            }
            String largeString = sb.toString();

            String key = largeString;
            Integer namespace = 1;
            String state = largeString;

            long address = arena.allocate(8192); // Larger allocation for large strings
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(state, kvNode.getValue(stringSerializer));
        }
    }

    @Nested
    class UpdateValueTests {

        @Test
        void testUpdateWithSameSize() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String originalState = "original"; // 8 chars
            String newState = "modified"; // 8 chars

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);
            assertTrue(kvNode.updateValue(stringSerializer, newState));
            assertEquals(newState, kvNode.getValue(stringSerializer));
        }

        @Test
        void testUpdateWithSmallerSize() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String originalState = "verylongoriginalstring";
            String newState = "short";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);
            assertTrue(kvNode.updateValue(stringSerializer, newState));
            assertEquals(newState, kvNode.getValue(stringSerializer));
        }

        @Test
        void testUpdateWithLargerSize() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String originalState = "short";
            String newState = "verylongnewstring";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);
            // Update with larger size should fail
            assertFalse(kvNode.updateValue(stringSerializer, newState));
            // Original value should remain unchanged
            assertEquals(originalState, kvNode.getValue(stringSerializer));
        }

        @Test
        void testUpdateToNull() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String originalState = "original";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);
            assertTrue(kvNode.updateValue(stringSerializer, null));
            assertNull(kvNode.getValue(stringSerializer));
        }

        @Test
        void testUpdateFromNull() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String originalState = null;

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, originalState);

            KVNode kvNode = new KVNode(address);
            assertTrue(kvNode.updateValue(stringSerializer, "newValue"));
            assertEquals("newValue", kvNode.getValue(stringSerializer));
        }

        @Test
        void testMultipleUpdates() throws IOException {
            String key = "key";
            Integer namespace = 1;
            String state1 = "state1";
            String state2 = "state2";
            String state3 = "stat3"; // Same length as state2

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state1);

            KVNode kvNode = new KVNode(address);

            assertTrue(kvNode.updateValue(stringSerializer, state2));
            assertEquals(state2, kvNode.getValue(stringSerializer));

            assertTrue(kvNode.updateValue(stringSerializer, state3));
            assertEquals(state3, kvNode.getValue(stringSerializer));
        }
    }

    @Nested
    class RawDataTests {

        @Test
        void testGetRawKeyBytes() throws IOException {
            String key = "testKey";
            Integer namespace = 1;
            String state = "state";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            byte[] rawKeyBytes = kvNode.getRawKeyBytes();

            assertNotNull(rawKeyBytes);
            assertTrue(rawKeyBytes.length > 0);

            // Verify that raw bytes can reconstruct the key
            // (This is implementation-dependent, but length should be reasonable)
        }

        @Test
        void testGetRawNamespaceBytes() throws IOException {
            String key = "key";
            Integer namespace = 42;
            String state = "state";

            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            byte[] rawNamespaceBytes = kvNode.getRawNamespaceBytes();

            assertNotNull(rawNamespaceBytes);
            assertTrue(rawNamespaceBytes.length > 0);
        }

        @Test
        void testRawBytesConsistency() throws IOException {
            String key = "consistencyKey";
            Integer namespace = 123;
            String state = "state";

            long address1 = arena.allocate(1024);
            long address2 = arena.allocate(1024);

            // Create two identical KVNodes
            KVNode.create(address1, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);
            KVNode.create(address2, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode1 = new KVNode(address1);
            KVNode kvNode2 = new KVNode(address2);

            // Raw bytes should be identical for identical content
            assertArrayEquals(kvNode1.getRawKeyBytes(), kvNode2.getRawKeyBytes());
            assertArrayEquals(kvNode1.getRawNamespaceBytes(), kvNode2.getRawNamespaceBytes());
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testMinimalKVNode() throws IOException {
            String key = "k";
            Integer namespace = 0;
            String state = "s";

            long address = arena.allocate(256);
            int totalSize = KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            assertTrue(totalSize > 12); // At least header size

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(state, kvNode.getValue(stringSerializer));
        }

        @Test
        void testKVNodeWithZeroValues() throws IOException {
            String key = "key";
            Integer namespace = 0;
            Integer state = 0;

            long address = arena.allocate(256);
            KVNode.create(address, stringSerializer, intSerializer, intSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(Integer.valueOf(0), kvNode.getNamespace(intSerializer));
            assertEquals(Integer.valueOf(0), kvNode.getValue(intSerializer));
        }

        @Test
        void testKVNodeLayout() throws IOException {
            String key = "layoutTest";
            Integer namespace = 999;
            String state = "layoutState";

            long address = arena.allocate(1024);
            int totalSize = KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            KVNode kvNode = new KVNode(address);

            // Total size should be consistent
            assertEquals(totalSize, kvNode.getTotalSize());

            // Should be able to create another KVNode from the same address
            KVNode kvNode2 = new KVNode(address);
            assertEquals(kvNode.getTotalSize(), kvNode2.getTotalSize());
            assertEquals(kvNode.getBaseAddress(), kvNode2.getBaseAddress());
        }

        @Test
        void testMatchesWithDifferentTypes() throws IOException {
            String stringKey = "123";
            Integer intKey = 123;
            Integer namespace = 1;
            String state = "state";

            // Create KVNode with string key
            long address = arena.allocate(1024);
            KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    stringKey, namespace, state);

            KVNode kvNode = new KVNode(address);

            // Should match with same string key
            assertTrue(kvNode.matches(stringSerializer, intSerializer, stringKey, namespace));

            // Should not match when trying to use integer serializer for string key
            // (This might throw exception or return false depending on implementation)
            assertThrows(Exception.class, () -> {
                kvNode.matches(intSerializer, intSerializer, intKey, namespace);
            });
        }
    }

    @Nested
    class PerformanceTests {

        @Test
        void testManyKVNodes() throws IOException {
            int numNodes = 100;
            long[] addresses = new long[numNodes];

            // Create many KVNodes
            for (int i = 0; i < numNodes; i++) {
                String key = "key" + i;
                Integer namespace = i;
                String state = "state" + i;

                addresses[i] = arena.allocate(512);
                KVNode.create(addresses[i], stringSerializer, intSerializer, stringSerializer,
                        key, namespace, state);
            }

            // Verify all nodes
            for (int i = 0; i < numNodes; i++) {
                KVNode kvNode = new KVNode(addresses[i]);
                assertEquals("key" + i, kvNode.getKey(stringSerializer));
                assertEquals(Integer.valueOf(i), kvNode.getNamespace(intSerializer));
                assertEquals("state" + i, kvNode.getValue(stringSerializer));
            }
        }

        @Test
        void testLargeKVNode() throws IOException {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 500; i++) {
                sb.append("largeContent");
            }
            String largeContent = sb.toString();

            String key = largeContent;
            Integer namespace = 1;
            String state = largeContent;

            long address = arena.allocate(16384); // 16KB allocation
            int totalSize = KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                    key, namespace, state);

            assertTrue(totalSize > 1000); // Should be substantial size

            KVNode kvNode = new KVNode(address);
            assertEquals(key, kvNode.getKey(stringSerializer));
            assertEquals(namespace, kvNode.getNamespace(intSerializer));
            assertEquals(state, kvNode.getValue(stringSerializer));
        }

        @Test
        void testSerializationPerformance() throws IOException {
            String key = "performanceKey";
            Integer namespace = 42;
            String state = "performanceState";

            long startTime = System.nanoTime();

            for (int i = 0; i < 1000; i++) {
                long address = arena.allocate(512);
                KVNode.create(address, stringSerializer, intSerializer, stringSerializer,
                        key, namespace, state);

                KVNode kvNode = new KVNode(address);
                kvNode.getKey(stringSerializer);
                kvNode.getNamespace(intSerializer);
                kvNode.getValue(stringSerializer);
            }

            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;

            // Should complete in reasonable time
            assertTrue(durationMs < 1000, "Serialization performance test took too long: " + durationMs + "ms");
        }
    }
}
