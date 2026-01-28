package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

public class ListAppendDebugTest2 {

    @Test
    void testListAppendInternalStore() throws Exception {
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        
        ForL0KeyedStateBackendBuilder<Long> builder = new ForL0KeyedStateBackendBuilder<>(
                null,
                LongSerializer.INSTANCE,
                Thread.currentThread().getContextClassLoader(),
                keyGroupRange.getNumberOfKeyGroups(),
                keyGroupRange,
                new org.apache.flink.api.common.ExecutionConfig(),
                org.apache.flink.runtime.state.ttl.TtlTimeProvider.DEFAULT,
                org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig.disabled(),
                java.util.Collections.emptyList(),
                org.apache.flink.runtime.state.AbstractStateBackend.getCompressionDecorator(new org.apache.flink.api.common.ExecutionConfig()),
                new org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory(keyGroupRange, keyGroupRange.getNumberOfKeyGroups(), 128),
                true,
                new org.apache.flink.core.fs.CloseableRegistry());
        
        ForL0KeyedStateBackend<Long> backend = builder.build();
        
        ListStateDescriptor<Long> stateDesc = new ListStateDescriptor<>("listState", Long.class);
        ListState<Long> listState = backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDesc);
        
        // Print state implementation class
        System.out.println("ListState implementation: " + listState.getClass().getName());
        
        // Get the internal store
        Field storeField = ForL0ListState.class.getDeclaredField("store");
        storeField.setAccessible(true);
        ForL0StateStore<Long, ?, ?> store = (ForL0StateStore<Long, ?, ?>) storeField.get(listState);
        System.out.println("Store implementation: " + store.getClass().getName());
        
        // Verify it's ForL0StateStoreLong
        assertTrue(store instanceof ForL0StateStoreLong, 
                "Expected ForL0StateStoreLong but got " + store.getClass().getName());
        
        ForL0StateStoreLong<?, ?> longStore = (ForL0StateStoreLong<?, ?>) store;
        
        // Check isVoidNamespaceLong field
        Field isVoidField = ForL0StateStoreLong.class.getDeclaredField("isVoidNamespaceLong");
        isVoidField.setAccessible(true);
        boolean isVoidNamespace = isVoidField.getBoolean(longStore);
        System.out.println("isVoidNamespaceLong: " + isVoidNamespace);
        assertTrue(isVoidNamespace, "Should be VoidNamespace mode");
        
        // Setup: add initial value for key 1
        backend.setCurrentKey(1L);
        listState.add(100L);
        
        // Check tablesLong array
        Field tablesField = ForL0StateStoreLong.class.getDeclaredField("tablesLong");
        tablesField.setAccessible(true);
        Object[] tables = (Object[]) tablesField.get(longStore);
        System.out.println("tablesLong length: " + tables.length);
        
        // Check keyGroupOffsetLong
        Field offsetField = ForL0StateStoreLong.class.getDeclaredField("keyGroupOffsetLong");
        offsetField.setAccessible(true);
        int offset = offsetField.getInt(longStore);
        System.out.println("keyGroupOffsetLong: " + offset);
        
        // Now check which keyGroup the key 1L is in
        Field keyContextField = backend.getClass().getDeclaredField("forl0KeyContext");
        keyContextField.setAccessible(true);
        ForL0KeyContext<Long> keyContext = (ForL0KeyContext<Long>) keyContextField.get(backend);
        System.out.println("Current keyGroupIndex: " + keyContext.currentKeyGroupIndex);
        System.out.println("Current key: " + keyContext.currentKey);
        
        // Check if table at that keyGroup is not null
        int idx = keyContext.currentKeyGroupIndex - offset;
        System.out.println("Array index: " + idx);
        assertNotNull(tables[idx], "Table at keyGroup should not be null after put");
        System.out.println("Table at idx " + idx + ": " + tables[idx].getClass().getName());
        
        // Now call get directly using reflection
        SwissTableLong<?> table = (SwissTableLong<?>) tables[idx];
        System.out.println("Table size: " + table.size());
        
        // Try get with same hash
        long k = 1L;
        int h = k == 0 ? 0 : (int)(0x1b873593L * Long.rotateLeft(k * 0xcc9e2d51L, 15));
        System.out.println("Hash for key 1L: " + h);
        
        Object value = table.get(h, k);
        System.out.println("Direct table.get() result: " + value);
        assertNotNull(value, "Direct table get should return non-null value");
        
        // Now test through ListState interface
        backend.setCurrentKey(1L);  // Same key
        Iterable<Long> values = listState.get();
        int count = 0;
        for (Long l : values) {
            count++;
        }
        System.out.println("Values count through listState.get(): " + count);
        assertEquals(1, count, "Should have 1 element");
        
        // Add another value - should append, not create new list
        listState.add(200L);
        
        values = listState.get();
        count = 0;
        for (Long l : values) {
            count++;
        }
        System.out.println("Values count after append: " + count);
        assertEquals(2, count, "Should have 2 elements after append");
        
        backend.close();
        System.out.println("Test passed!");
    }
}
