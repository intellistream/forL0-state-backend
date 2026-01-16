package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that ForL0StateTable correctly creates specialized SwissTable implementations
 * based on key and namespace types.
 */
class ForL0StateTableSpecializationTest {

    /**
     * Helper to create a ForL0StateTable with specified key/namespace serializers.
     */
    private <K, N> ForL0StateTable<K, N, String> createStateTable(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer) {
        
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
        InternalKeyContextImpl<K> keyContext = new InternalKeyContextImpl<>(keyGroupRange, 1);
        
        RegisteredKeyValueStateBackendMetaInfo<N, String> metaInfo = 
            new RegisteredKeyValueStateBackendMetaInfo<>(
                org.apache.flink.api.common.state.StateDescriptor.Type.VALUE,
                "test-state",
                namespaceSerializer,
                StringSerializer.INSTANCE,
                StateSnapshotTransformFactory.noTransform());
        
        return ForL0StateTable.create(keyContext, metaInfo, keySerializer);
    }

    /**
     * Gets the internal SwissTable class name from a ForL0StateMap.
     */
    private String getSwissTableClassName(StateMap<?, ?, ?> stateMap) throws Exception {
        assertTrue(stateMap instanceof ForL0StateMap, "Expected ForL0StateMap");
        ForL0StateMap<?, ?, ?> forL0Map = (ForL0StateMap<?, ?, ?>) stateMap;
        
        // Access the directory field to get the SwissTable
        Field directoryField = ForL0StateMap.class.getDeclaredField("directory");
        directoryField.setAccessible(true);
        AbstractSwissTable<?, ?, ?>[] directory = (AbstractSwissTable<?, ?, ?>[]) directoryField.get(forL0Map);
        
        return directory[0].getClass().getSimpleName();
    }

    @Nested
    class LongKeySpecializations {

        @Test
        void testLongKeyWithVoidNamespace() throws Exception {
            ForL0StateTable<Long, VoidNamespace, String> stateTable = 
                createStateTable(LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
            
            assertNotNull(stateTable);
            assertEquals(1, stateTable.size() == 0 ? 1 : 0, "Table should be empty initially");
            
            // Get the internal StateMap and verify it uses the specialized implementation
            StateMap<Long, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableLongVoid", tableClassName,
                "Long + VoidNamespace should use SwissTableLongVoid");
        }

        @Test
        void testLongKeyWithTimeWindow() throws Exception {
            ForL0StateTable<Long, TimeWindow, String> stateTable = 
                createStateTable(LongSerializer.INSTANCE, new TimeWindow.Serializer());
            
            StateMap<Long, TimeWindow, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableLongTimeWindow", tableClassName,
                "Long + TimeWindow should use SwissTableLongTimeWindow");
        }
    }

    @Nested
    class StringKeySpecializations {

        @Test
        void testStringKeyWithVoidNamespace() throws Exception {
            ForL0StateTable<String, VoidNamespace, String> stateTable = 
                createStateTable(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
            
            StateMap<String, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableStringVoid", tableClassName,
                "String + VoidNamespace should use SwissTableStringVoid");
        }

        @Test
        void testStringKeyWithTimeWindow() throws Exception {
            ForL0StateTable<String, TimeWindow, String> stateTable = 
                createStateTable(StringSerializer.INSTANCE, new TimeWindow.Serializer());
            
            StateMap<String, TimeWindow, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableStringTimeWindow", tableClassName,
                "String + TimeWindow should use SwissTableStringTimeWindow");
        }
    }

    @Nested
    class GenericFallback {

        @Test
        void testIntKeyFallsBackToGeneric() throws Exception {
            ForL0StateTable<Integer, VoidNamespace, String> stateTable = 
                createStateTable(IntSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
            
            StateMap<Integer, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableGeneric", tableClassName,
                "Integer key (unsupported) should fall back to SwissTableGeneric");
        }

        @Test
        void testLongKeyWithCustomNamespaceFallsBackToGeneric() throws Exception {
            // Using StringSerializer as namespace (not VoidNamespace or TimeWindow)
            ForL0StateTable<Long, String, String> stateTable = 
                createStateTable(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
            
            StateMap<Long, String, String> stateMap = stateTable.getMapForKeyGroup(0);
            String tableClassName = getSwissTableClassName(stateMap);
            
            assertEquals("SwissTableGeneric", tableClassName,
                "Long + String namespace should fall back to SwissTableGeneric");
        }
    }

    @Nested
    class FunctionalityVerification {

        @Test
        void testLongVoidSpecializationWorks() throws Exception {
            ForL0StateTable<Long, VoidNamespace, String> stateTable = 
                createStateTable(LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
            
            StateMap<Long, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(0);
            
            // Test basic operations work correctly
            stateMap.put(100L, VoidNamespace.INSTANCE, "value100");
            stateMap.put(200L, VoidNamespace.INSTANCE, "value200");
            stateMap.put(300L, VoidNamespace.INSTANCE, "value300");
            
            assertEquals("value100", stateMap.get(100L, VoidNamespace.INSTANCE));
            assertEquals("value200", stateMap.get(200L, VoidNamespace.INSTANCE));
            assertEquals("value300", stateMap.get(300L, VoidNamespace.INSTANCE));
            assertEquals(3, stateMap.size());
        }

        @Test
        void testLongTimeWindowSpecializationWorks() throws Exception {
            ForL0StateTable<Long, TimeWindow, String> stateTable = 
                createStateTable(LongSerializer.INSTANCE, new TimeWindow.Serializer());
            
            StateMap<Long, TimeWindow, String> stateMap = stateTable.getMapForKeyGroup(0);
            
            TimeWindow w1 = new TimeWindow(0, 1000);
            TimeWindow w2 = new TimeWindow(1000, 2000);
            
            stateMap.put(1L, w1, "window1");
            stateMap.put(1L, w2, "window2");
            stateMap.put(2L, w1, "key2window1");
            
            assertEquals("window1", stateMap.get(1L, w1));
            assertEquals("window2", stateMap.get(1L, w2));
            assertEquals("key2window1", stateMap.get(2L, w1));
            assertEquals(3, stateMap.size());
        }

        @Test
        void testStringVoidSpecializationWorks() throws Exception {
            ForL0StateTable<String, VoidNamespace, String> stateTable = 
                createStateTable(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE);
            
            StateMap<String, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(0);
            
            stateMap.put("key1", VoidNamespace.INSTANCE, "value1");
            stateMap.put("key2", VoidNamespace.INSTANCE, "value2");
            
            assertEquals("value1", stateMap.get("key1", VoidNamespace.INSTANCE));
            assertEquals("value2", stateMap.get("key2", VoidNamespace.INSTANCE));
        }

        @Test
        void testStringTimeWindowSpecializationWorks() throws Exception {
            ForL0StateTable<String, TimeWindow, String> stateTable = 
                createStateTable(StringSerializer.INSTANCE, new TimeWindow.Serializer());
            
            StateMap<String, TimeWindow, String> stateMap = stateTable.getMapForKeyGroup(0);
            
            TimeWindow window = new TimeWindow(0, 60000);
            
            stateMap.put("user1", window, "data1");
            stateMap.put("user2", window, "data2");
            
            assertEquals("data1", stateMap.get("user1", window));
            assertEquals("data2", stateMap.get("user2", window));
        }
    }

    @Nested
    class MultipleKeyGroupsTest {

        @Test
        void testAllKeyGroupsUseSpecialization() throws Exception {
            KeyGroupRange keyGroupRange = new KeyGroupRange(0, 3);  // 4 key groups
            InternalKeyContextImpl<Long> keyContext = new InternalKeyContextImpl<>(keyGroupRange, 4);
            
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> metaInfo = 
                new RegisteredKeyValueStateBackendMetaInfo<>(
                    org.apache.flink.api.common.state.StateDescriptor.Type.VALUE,
                    "test-state",
                    VoidNamespaceSerializer.INSTANCE,
                    StringSerializer.INSTANCE,
                    StateSnapshotTransformFactory.noTransform());
            
            ForL0StateTable<Long, VoidNamespace, String> stateTable = 
                ForL0StateTable.create(keyContext, metaInfo, LongSerializer.INSTANCE);
            
            // Verify all key groups use the specialized implementation
            for (int kg = 0; kg <= 3; kg++) {
                StateMap<Long, VoidNamespace, String> stateMap = stateTable.getMapForKeyGroup(kg);
                String tableClassName = getSwissTableClassName(stateMap);
                assertEquals("SwissTableLongVoid", tableClassName,
                    "KeyGroup " + kg + " should use SwissTableLongVoid");
            }
        }
    }
}
