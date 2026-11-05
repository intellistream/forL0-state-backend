package org.apache.flink.benchmark.benchset;

import com.google.gson.*;
import org.junit.jupiter.api.Test;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class BenchsetTest {
    public static class HeapMap implements MapState<Long,Long> {
        final Map<Long,Long> values=new HashMap<>();
        public Long get(Long k){return values.get(k);}
        public void put(Long k,Long v){values.put(k,v);}
        public void putAll(Map<Long,Long> m){values.putAll(m);}
        public void remove(Long k){values.remove(k);}
        public boolean contains(Long k){return values.containsKey(k);}
        public Iterable<Map.Entry<Long,Long>> entries(){return values.entrySet();}
        public Iterable<Long> keys(){return values.keySet();}
        public Iterable<Long> values(){return values.values();}
        public Iterator<Map.Entry<Long,Long>> iterator(){return values.entrySet().iterator();}
        public boolean isEmpty(){return values.isEmpty();}
        public void clear(){values.clear();}
    }
    @Test void numericRecordIsAnActualFlinkPojo() {
        assertTrue(org.apache.flink.api.common.typeinfo.TypeInformation.of(BenchsetEvent.class)
                instanceof org.apache.flink.api.java.typeutils.PojoTypeInfo);
    }
    @Test void allSevenTaskGraphsMatchTheIndependentPaperContract() throws Exception {
        JsonObject root=JsonParser.parseString(new String(Files.readAllBytes(Paths.get("reference/topology-contract.json")),java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject();
        for(Map.Entry<String,JsonElement> entry:root.getAsJsonObject("workloads").entrySet()) {
            StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
            env.disableOperatorChaining();env.getConfig().disableGenericTypes();
            Map<String,String> args=new HashMap<>();args.put("outputDirectory","/tmp/benchset-unit");
            BenchsetMain main=new BenchsetMain(env,ParameterTool.fromMap(args));main.build(entry.getKey(),128,1024);
            JsonObject actual=new Gson().toJsonTree(BenchsetMain.topology(env.getStreamGraph(false))).getAsJsonObject();
            Set<String> nodes=new HashSet<>();for(JsonElement n:actual.getAsJsonArray("nodes"))nodes.add(n.getAsJsonObject().get("name").getAsString());
            Set<String> expected=new HashSet<>();for(JsonElement n:entry.getValue().getAsJsonObject().getAsJsonArray("nodes"))expected.add(n.getAsString());
            assertEquals(expected,nodes,entry.getKey());
            assertEquals(edgeCounts(entry.getValue().getAsJsonObject().getAsJsonArray("edges")),edgeCounts(actual.getAsJsonArray("edges")),entry.getKey());
        }
    }
    private Map<String,Integer> edgeCounts(JsonArray edges) {
        Map<String,Integer> m=new TreeMap<>();
        for(JsonElement item:edges){JsonObject e=item.getAsJsonObject();String k=e.get("from").getAsString()+" -> "+e.get("to").getAsString();m.merge(k,1,Integer::sum);}return m;
    }
}
