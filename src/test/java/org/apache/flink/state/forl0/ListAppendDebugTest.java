package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ListAppendDebugTest {

    @Test
    void testListAppendDoesNotCreateNewArrayList() throws Exception {
        // Create backend similar to benchmark
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        
        ForL0KeyedStateBackendBuilder<Long> builder = new ForL0KeyedStateBackendBuilder<>(
                null,  // kvStateRegistry
                LongSerializer.INSTANCE,
                Thread.currentThread().getContextClassLoader(),
                keyGroupRange.getNumberOfKeyGroups(),  // numberOfKeyGroups
                keyGroupRange,
                new org.apache.flink.api.common.ExecutionConfig(),
                org.apache.flink.runtime.state.ttl.TtlTimeProvider.DEFAULT,
                org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig.disabled(),
                java.util.Collections.emptyList(),
                org.apache.flink.runtime.state.AbstractStateBackend.getCompressionDecorator(new org.apache.flink.api.common.ExecutionConfig()),
                new org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory(keyGroupRange, keyGroupRange.getNumberOfKeyGroups(), 128),
                true,
                false,  // l0CacheEnabled
                0L,     // l0CacheSize
                0L,     // l0CacheMaxPerAlloc
                new org.apache.flink.core.fs.CloseableRegistry());
        
        ForL0KeyedStateBackend<Long> backend = builder.build();
        
        ListStateDescriptor<Long> stateDesc = new ListStateDescriptor<>("listState", Long.class);
        ListState<Long> listState = backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDesc);
        
        // Setup: add initial value for key 1
        backend.setCurrentKey(1L);
        listState.add(100L);
        
        // Get the list reference
        Iterable<Long> list1 = listState.get();
        assertNotNull(list1);
        
        // Now do "append" - should NOT create new ArrayList
        backend.setCurrentKey(1L);  // Same key
        listState.add(200L);
        
        // Verify we still have the same list
        Iterable<Long> list2 = listState.get();
        
        // Count elements
        int count = 0;
        for (Long l : list2) {
            count++;
        }
        assertEquals(2, count, "List should have 2 elements after append");
        
        // Test with different key group
        backend.setCurrentKey(2L);  // This should go to a different key group
        listState.add(300L);
        
        Iterable<Long> list3 = listState.get();
        count = 0;
        for (Long l : list3) {
            count++;
        }
        assertEquals(1, count, "New key should have 1 element");
        
        // Go back to key 1 - should still have 2 elements
        backend.setCurrentKey(1L);
        Iterable<Long> list4 = listState.get();
        count = 0;
        for (Long l : list4) {
            count++;
        }
        assertEquals(2, count, "Key 1 should still have 2 elements");
        
        backend.close();
    }
}
