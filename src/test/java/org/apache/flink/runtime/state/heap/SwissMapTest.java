package org.apache.flink.runtime.state.heap;

import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for SwissMap implementation.
 * Tests CRUD operations, expansion (rehash/grow/split), and edge cases.
 */
class SwissMapTest {

    private SwissMap<String, Integer, String> map;

    @BeforeEach
    void setUp() {
        map = new SwissMap<>();
    }

    /** Helper method to put key-namespace-state triple */
    private void putState(String key, Integer ns, String state) {
        int ptr = map.put(key, ns);
        map.states[ptr - 1] = state;
    }

    @Nested
    class BasicCRUDTests {

        @Test
        void testEmptyMap() {
            assertEquals(0, map.size());
            assertNull(map.get("key", 1));
        }

        @Test
        void testPutAndGet() {
            putState("key1", 1, "value1");
            assertEquals(1, map.size());
            assertEquals("value1", map.get("key1", 1));
        }

        @Test
        void testPutMultiple() {
            for (int i = 0; i < 10; i++) {
                putState("key" + i, i, "value" + i);
            }
            assertEquals(10, map.size());

            for (int i = 0; i < 10; i++) {
                assertEquals("value" + i, map.get("key" + i, i));
            }
        }

        @Test
        void testUpdate() {
            putState("key", 1, "value1");
            assertEquals("value1", map.get("key", 1));
            assertEquals(1, map.size());

            putState("key", 1, "value2");
            assertEquals("value2", map.get("key", 1));
            assertEquals(1, map.size());
        }

        @Test
        void testRemove() {
            putState("key", 1, "value");
            assertEquals(1, map.size());

            String removed = map.remove("key", 1);
            assertEquals("value", removed);
            assertEquals(0, map.size());
            assertNull(map.get("key", 1));
        }

        @Test
        void testRemoveNonExistent() {
            putState("key", 1, "value");
            assertNull(map.remove("nonexistent", 1));
            assertNull(map.remove("key", 2));
            assertEquals(1, map.size());
        }

        @Test
        void testPutAfterRemove() {
            putState("key", 1, "value1");
            map.remove("key", 1);
            putState("key", 1, "value2");
            assertEquals(1, map.size());
            assertEquals("value2", map.get("key", 1));
        }

        @Test
        void testDifferentNamespaces() {
            putState("key", 1, "value1");
            putState("key", 2, "value2");
            putState("key", 3, "value3");

            assertEquals(3, map.size());
            assertEquals("value1", map.get("key", 1));
            assertEquals("value2", map.get("key", 2));
            assertEquals("value3", map.get("key", 3));
        }
    }

    @Nested
    class ExpansionTests {

        @Test
        void testGrowTriggered() {
            // Insert enough entries to trigger grow (initial capacity is 64, maxOcc = 56)
            for (int i = 0; i < 100; i++) {
                putState("key" + i, 0, "value" + i);
            }
            assertEquals(100, map.size());

            // Verify all entries are still accessible
            for (int i = 0; i < 100; i++) {
                assertEquals("value" + i, map.get("key" + i, 0));
            }
        }

        @Test
        void testSplitTriggered() {
            // Insert many entries to trigger multiple splits (max slot count per table is 1024)
            int count = 2000;
            for (int i = 0; i < count; i++) {
                putState("key" + i, 0, "value" + i);
            }
            assertEquals(count, map.size());

            // Verify all entries are still accessible
            for (int i = 0; i < count; i++) {
                assertEquals("value" + i, map.get("key" + i, 0),
                        "Failed for key" + i);
            }

            // Verify multiple tables exist
            assertTrue(map.getTables().size() > 1,
                    "Expected multiple tables after split");
        }

        @Test
        void testRehashAfterManyDeletes() {
            // Fill the table
            for (int i = 0; i < 50; i++) {
                putState("key" + i, 0, "value" + i);
            }

            // Delete half of them
            for (int i = 0; i < 25; i++) {
                map.remove("key" + i, 0);
            }
            assertEquals(25, map.size());

            // Insert more to trigger rehash (tombstones should be cleared)
            for (int i = 50; i < 100; i++) {
                putState("key" + i, 0, "value" + i);
            }

            // Verify all remaining entries
            for (int i = 25; i < 100; i++) {
                assertEquals("value" + i, map.get("key" + i, 0));
            }
        }
    }

    @Nested
    class IterationTests {

        @Test
        void testIteratorEmpty() {
            assertFalse(map.iterator().hasNext());
        }

        @Test
        void testIteratorSingleEntry() {
            putState("key", 1, "value");

            Iterator<SwissMap.SwissMapEntry<String, Integer, String>> iter = map.iterator();
            assertTrue(iter.hasNext());

            SwissMap.SwissMapEntry<String, Integer, String> entry = iter.next();
            assertEquals("key", entry.getKey());
            assertEquals(1, entry.getNamespace());
            assertEquals("value", entry.getState());

            assertFalse(iter.hasNext());
        }

        @Test
        void testIteratorMultipleEntries() {
            Set<String> expected = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                putState("key" + i, i, "value" + i);
                expected.add("key" + i + ":" + i + ":" + "value" + i);
            }

            Set<String> actual = new HashSet<>();
            Iterator<SwissMap.SwissMapEntry<String, Integer, String>> iter = map.iterator();
            while (iter.hasNext()) {
                SwissMap.SwissMapEntry<String, Integer, String> entry = iter.next();
                actual.add(entry.getKey() + ":" + entry.getNamespace() + ":" + entry.getState());
            }

            assertEquals(expected, actual);
        }

        @Test
        void testForEach() {
            Map<String, String> expected = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                putState("key" + i, 0, "value" + i);
                expected.put("key" + i, "value" + i);
            }

            Map<String, String> actual = new HashMap<>();
            map.forEach((k, n, s) -> actual.put(k, s));

            assertEquals(expected, actual);
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testNullValue() {
            putState("key", 1, null);
            assertEquals(1, map.size());
            assertNull(map.get("key", 1));
        }

        @Test
        void testHashCollision() {
            // Test keys that might have hash collisions
            // Using strings with predictable hash behavior
            for (int i = 0; i < 100; i++) {
                String key = String.format("%08d", i);
                putState(key, 0, "value" + i);
            }

            for (int i = 0; i < 100; i++) {
                String key = String.format("%08d", i);
                assertEquals("value" + i, map.get(key, 0));
            }
        }

        @Test
        void testLargeScale() {
            int count = 10000;
            for (int i = 0; i < count; i++) {
                putState("key" + i, i % 10, "value" + i);
            }
            assertEquals(count, map.size());

            // Verify random samples
            Random random = new Random(42);
            for (int i = 0; i < 1000; i++) {
                int idx = random.nextInt(count);
                assertEquals("value" + idx, map.get("key" + idx, idx % 10));
            }
        }

        @Test
        void testRemoveAndReinsertMany() {
            // Fill
            for (int i = 0; i < 100; i++) {
                putState("key" + i, 0, "value" + i);
            }

            // Remove all
            for (int i = 0; i < 100; i++) {
                map.remove("key" + i, 0);
            }
            assertEquals(0, map.size());

            // Reinsert
            for (int i = 0; i < 100; i++) {
                putState("key" + i, 0, "newvalue" + i);
            }
            assertEquals(100, map.size());

            // Verify
            for (int i = 0; i < 100; i++) {
                assertEquals("newvalue" + i, map.get("key" + i, 0));
            }
        }

        @Test
        void testInterleavedOperations() {
            Random random = new Random(123);
            Map<String, String> reference = new HashMap<>();

            for (int i = 0; i < 1000; i++) {
                int op = random.nextInt(3);
                String key = "key" + random.nextInt(100);
                int ns = random.nextInt(5);
                String refKey = key + ":" + ns;

                switch (op) {
                    case 0: // put
                        String value = "value" + i;
                        putState(key, ns, value);
                        reference.put(refKey, value);
                        break;
                    case 1: // get
                        assertEquals(reference.get(refKey), map.get(key, ns));
                        break;
                    case 2: // remove
                        String expectedRemoved = reference.remove(refKey);
                        String actualRemoved = map.remove(key, ns);
                        assertEquals(expectedRemoved, actualRemoved);
                        break;
                }
            }

            assertEquals(reference.size(), map.size());
        }
    }

    @Nested
    class SwissTableTests {

        @Test
        void testMatchH2() {
            // All empty: 0x8080808080808080
            long allEmpty = 0x8080808080808080L;
            assertEquals(0, SwissTable.matchH2(allEmpty, 0));
            assertEquals(0, SwissTable.matchH2(allEmpty, 0x7F));

            // Single match at lane 0
            long ctrlWord = 0x8080808080808000L; // h2=0 at lane 0
            long match = SwissTable.matchH2(ctrlWord, 0);
            assertTrue(match != 0);
            assertEquals(0, SwissTable.laneFromTz(Long.numberOfTrailingZeros(match)));

            // Single match at lane 3
            long ctrlWord2 = 0x80808080_42_808080L; // h2=0x42 at lane 3
            long match2 = SwissTable.matchH2(ctrlWord2, 0x42);
            assertTrue(match2 != 0);
            assertEquals(3, SwissTable.laneFromTz(Long.numberOfTrailingZeros(match2)));
        }

        @Test
        void testMatchEmpty() {
            // All empty
            long allEmpty = 0x8080808080808080L;
            assertEquals(0x8080808080808080L, SwissTable.matchEmpty(allEmpty));

            // Mix of empty and full
            // In little-endian long representation:
            // Lane 0 = byte 0 (LSB), Lane 7 = byte 7 (MSB)
            // 0x80_42_80_00_80_7F_80_01L:
            // Lane 0: 0x01, Lane 1: 0x80 (empty), Lane 2: 0x7F
            // Lane 3: 0x80 (empty), Lane 4: 0x00, Lane 5: 0x80 (empty)
            // Lane 6: 0x42, Lane 7: 0x80 (empty)
            long mixed = 0x80_42_80_00_80_7F_80_01L;
            long emptyMask = SwissTable.matchEmpty(mixed);
            
            // Check that we have some matches (lanes 1, 3, 5, 7 are empty)
            assertTrue(emptyMask != 0, "Should find at least one empty slot");
            
            // Verify specific lanes
            assertTrue((emptyMask & (0x80L << 8)) != 0, "Lane 1 should be empty");  // lane 1
            assertTrue((emptyMask & (0x80L << 24)) != 0, "Lane 3 should be empty"); // lane 3
            assertTrue((emptyMask & (0x80L << 40)) != 0, "Lane 5 should be empty"); // lane 5
            assertTrue((emptyMask & (0x80L << 56)) != 0, "Lane 7 should be empty"); // lane 7
        }

        @Test
        void testMatchDeleted() {
            // All deleted: 0xFEFEFEFEFEFEFEFE
            long allDeleted = 0xFEFEFEFEFEFEFEFEL;
            assertEquals(0x8080808080808080L, SwissTable.matchDeleted(allDeleted));

            // Single deleted at lane 5
            long ctrlWord = 0x8080_FE_8080808080L;
            long delMask = SwissTable.matchDeleted(ctrlWord);
            assertTrue(delMask != 0);
            assertEquals(5, SwissTable.laneFromTz(Long.numberOfTrailingZeros(delMask)));
        }

        @Test
        void testIsFull() {
            assertTrue(SwissTable.isFull((byte) 0x00));
            assertTrue(SwissTable.isFull((byte) 0x42));
            assertTrue(SwissTable.isFull((byte) 0x7F));
            assertFalse(SwissTable.isFull(SwissTable.CTRL_EMPTY));
            assertFalse(SwissTable.isFull(SwissTable.CTRL_DELETED));
        }

        @Test
        void testTableConstruction() {
            SwissTable table = new SwissTable(64);
            assertEquals(64, table.capacity);
            assertEquals(7, table.groupMask); // 64/8 - 1 = 7
            assertEquals(56, table.growthLeft); // 64 * 7/8 = 56
            assertEquals(0, table.used);
            assertEquals(0, table.tomb);

            // All ctrl should be EMPTY
            for (byte b : table.ctrl) {
                assertEquals(SwissTable.CTRL_EMPTY, b);
            }
        }
    }
}
