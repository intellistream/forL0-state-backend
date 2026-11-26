package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Runs unmodified repository SELECTs with optional native test counters. */
class NexmarkHotCacheSqlITCase {
    private static native long[] testCacheStats(boolean reset);
    private static final LocalDateTime START = LocalDateTime.of(2020, 1, 1, 0, 0);

    @ParameterizedTest(name = "Q{0}: Heap, ForL0, and whole-window tables return the same rows")
    @ValueSource(ints = {4, 5, 8, 9, 11, 18, 19, 20})
    void queryOutputsAndCacheTraffic(int query) throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"),
                "Requires the JNI library with SQL test counters");
        NativeEngine.ensureLoaded();
        Map<String, Integer> expected = run(query, "hashmap", false);
        assertFalse(expected.isEmpty(), "Fixture must exercise the query output");
        assertEquals(expected, run(query, "forl0", false), "L0 off Q" + query);
        testCacheStats(true);
        assertEquals(expected, run(query, "forl0", true), "L0 on Q" + query);
        long[] stats = testCacheStats(false);
        assertTrue(stats[0] > 0, "No attached caches in Q" + query);
        assertTrue(stats[1] > 0, "No cache lookups in Q" + query);
        assertTrue(stats[2] > 0, "No cache hits in Q" + query);
        assertTrue(stats[3] > 0, "No resident table writes in Q" + query);
        System.out.printf("NEXMARK Q%d PASS attached=%d lookups=%d hits=%d writes=%d%n",
                query, stats[0], stats[1], stats[2], stats[3]);
    }

    private Map<String, Integer> run(int query, String backend, boolean l0) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend.type", backend.equals("forl0")
                ? ForL0StateBackendFactory.class.getName() : backend);
        if (backend.equals("forl0")) {
            conf.setString("state.backend", ForL0StateBackendFactory.class.getName());
        }
        conf.setBoolean("state.backend.forl0.l0-cache.enabled", l0);
        conf.setString("state.backend.forl0.l0-cache.size", "8mb");
        conf.setInteger("taskmanager.numberOfTaskSlots", 1);
        conf.setString("rest.bind-port", "0");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(env);
        tables.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "1");
        // Both paths must visit backend state instead of being masked by planner caches.
        tables.getConfig().getConfiguration().setString("table.exec.mini-batch.enabled", "false");
        tables.getConfig().getConfiguration().setString("table.exec.rank.topn-cache-size", "1");

        List<Row> people = new ArrayList<>(), auctions = new ArrayList<>(), bids = new ArrayList<>();
        for (long i = 1; i <= 6; ++i) people.add(Row.of(i, "person" + i, "mail", "card", "city", "st", START.plusSeconds(i % 3), "person-extra"));
        for (long i = 0; i < 24; ++i) auctions.add(Row.of(100 + i, "item" + i, "description", 1L, 5L,
                START.plusSeconds(1), START.plusSeconds(60), 1 + i % 6, i % 2 == 0 ? 10L : 20L, "auction-extra"));
        for (long i = 0; i < 720; ++i) bids.add(Row.of(100 + i % 24, 1 + i % 6, 20 + i % 31,
                "channel", "https://example.test/" + i % 4, START.plusNanos((2000 + i * 40) * 1000000), "bid-extra"));
        view(tables, env, "person", people,
                new String[]{"id", "name", "emailAddress", "creditCard", "city", "state", "dateTime", "extra"},
                new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME, Types.STRING});
        view(tables, env, "auction", auctions,
                new String[]{"id", "itemName", "description", "initialBid", "reserve", "dateTime", "expires", "seller", "category", "extra"},
                new TypeInformation<?>[]{Types.LONG, Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE_TIME, Types.LONG, Types.LONG, Types.STRING});
        view(tables, env, "bid", bids,
                new String[]{"auction", "bidder", "price", "channel", "url", "dateTime", "extra"},
                new TypeInformation<?>[]{Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME, Types.STRING});
        String sql = new String(Files.readAllBytes(Paths.get("benchmark/nexmark-src/nexmark-flink/src/main/resources/queries/q" + query + ".sql")), StandardCharsets.UTF_8);
        int select = sql.indexOf("\nSELECT", sql.indexOf("INSERT INTO"));
        assertTrue(select >= 0);
        String querySql = sql.substring(select).trim().replaceFirst(";\\s*$", "");
        TableResult result = tables.executeSql(querySql);
        Map<String, Integer> multiset = new HashMap<>();
        try (CloseableIterator<Row> rows = result.collect()) {
            while (rows.hasNext()) {
                Row row = rows.next();
                int delta = row.getKind() == RowKind.DELETE || row.getKind() == RowKind.UPDATE_BEFORE ? -1 : 1;
                row.setKind(RowKind.INSERT);
                multiset.merge(row.toString(), delta, Integer::sum);
            }
        }
        result.await();
        multiset.values().removeIf(n -> n == 0);
        return multiset;
    }

    private void view(StreamTableEnvironment tables, StreamExecutionEnvironment env, String name,
                      List<Row> rows, String[] names, TypeInformation<?>[] types) {
        Schema.Builder schema = Schema.newBuilder();
        for (int i = 0; i < names.length; ++i) {
            schema.column(names[i], types[i] == Types.LONG ? DataTypes.BIGINT()
                    : types[i] == Types.STRING ? DataTypes.STRING() : DataTypes.TIMESTAMP(3));
        }
        schema.watermark("dateTime", "dateTime - INTERVAL '4' SECOND");
        tables.createTemporaryView(name, tables.fromDataStream(
                env.fromCollection(rows, new RowTypeInfo(types, names)), schema.build()));
    }
}
