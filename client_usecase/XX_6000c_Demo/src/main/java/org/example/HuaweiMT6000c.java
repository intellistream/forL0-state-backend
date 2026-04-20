package org.example;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class HuaweiMT6000c {
    private static final Logger logger = LoggerFactory.getLogger(HuaweiMT6000c.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int parallelism = parameters.getInt("parallelism", env.getParallelism());
        long checkpointInterval = parameters.getLong("checkpointInterval", 0L);
        long leftNumRecords = parameters.getLong("leftNumRecords", 0L);
        long rightNumRecords = parameters.getLong("rightNumRecords", 0L);

        env.setParallelism(parallelism);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }

        DataStream<PVMVLogType> inputStream1 = env.addSource(
            new CsvReplaySource(0L, 0, leftNumRecords));

        DataStream<PVMVLogType> inputStream2 = env.addSource(
            new CsvReplaySource(20L, 2, rightNumRecords));

        SingleOutputStreamOperator<Tuple2<String, String>> coProcess =
                inputStream1.keyBy(PVMVLogType::joinKey)
                        .connect(inputStream2.keyBy(PVMVLogType::joinKey))
                        .process(new HuaweiTestFunction(
                                Time.minutes(20), Time.minutes(20), false, 100))
                        .disableChaining();
        coProcess.addSink(new DiscardingSink<>()).disableChaining();

        logger.info(
            "Starting HuaweiMT6000c benchmark: parallelism={}, checkpointInterval={}, leftNumRecords={}, rightNumRecords={}",
                parallelism,
                checkpointInterval,
            leftNumRecords,
            rightNumRecords);

        env.execute("client-usecase-xx6000c-benchmark");
    }

    private static final class CsvReplaySource extends RichParallelSourceFunction<PVMVLogType> {
        private final long lineOffset;
        private final int perRecordDelayMs;
        private final long maxRecords;
        private volatile boolean running = true;

        private CsvReplaySource(long lineOffset, int perRecordDelayMs, long maxRecords) {
            this.lineOffset = lineOffset;
            this.perRecordDelayMs = perRecordDelayMs;
            this.maxRecords = maxRecords;
        }

        @Override
        public void run(SourceFunction.SourceContext<PVMVLogType> sourceContext) throws Exception {
            List<String> lines = loadCsvLines();
            long emitted = 0L;

            while (running && (maxRecords <= 0 || emitted < maxRecords)) {
                for (String rawLine : lines) {
                    if (!running || (maxRecords > 0 && emitted >= maxRecords)) {
                        return;
                    }
                    long line = Long.parseLong(rawLine.trim()) + lineOffset;
                    PVMVLogType record = createPVMV(line);
                    synchronized (sourceContext.getCheckpointLock()) {
                        sourceContext.collect(record);
                        emitted++;
                    }
                    if (perRecordDelayMs > 0) {
                        Thread.sleep(perRecordDelayMs);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static List<String> loadCsvLines() throws IOException {
        InputStream inputStream = HuaweiMT6000c.class.getClassLoader().getResourceAsStream("data.csv");
        if (inputStream == null) {
            throw new IOException("data.csv not found in classpath");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().filter(line -> !line.trim().isEmpty()).collect(Collectors.toList());
        }
    }

    public static PVMVLogType createPVMV(long line) throws IllegalAccessException {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        long timestampMillis = Instant.from(now).toEpochMilli();
        long uniqueLine = line + timestampMillis;
        PVMVLogType result = new PVMVLogType(timestampMillis, uniqueLine);
        result.setPvmvFlowInfo(Gen7KBData.genDataSimple());
        return result;
    }
}
