package org.example;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class Hxx6000c {
    private static final Logger logger = LoggerFactory.getLogger(Hxx6000c.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<PVMVLogType> inputStream1 = generatePVMVLog(env);

        DataStream<PVMVLogType> inputStream2 = generatePVMVLog1(env);

        // simulate stream1 --> keyby --> keyedCoprocess->printSink
        //          stream2 --> keyby  ./
        SingleOutputStreamOperator<Tuple2<String, String>> coProcess =
                inputStream1.keyBy(PVMVLogType::joinKey)
                        .connect(inputStream2.keyBy(PVMVLogType::joinKey)
                        ).process(
                                new HTestFunction(
                                        Time.minutes(20),Time.minutes(20), false,100)).disableChaining();
        coProcess.addSink(new PrintSinkFunction<>()).disableChaining();

        env.execute();
    }

    public static DataStream<PVMVLogType> generatePVMVLog(StreamExecutionEnvironment env) {
        DataStream<PVMVLogType> inputStream = env.addSource(new ParallelSourceFunction<PVMVLogType>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<PVMVLogType> sourceContext) throws Exception {
                //data1 1,000,000-6,000,000; data2 1-5,000,000; data3 5,000,000-10,000,000
                InputStream is = Hxx6000c.class.getClassLoader().getResourceAsStream("data.csv");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    List<String> lines = reader.lines().collect(Collectors.toList());
                    while (running) {
                        long startTime = System.currentTimeMillis();
                        long t=0;
                        synchronized (sourceContext.getCheckpointLock()) {
                            for (int i = 0; i < lines.size(); i++) {
                                //确保文件行数与i的范围一致
                                long line = Long.valueOf(lines.get(i).trim());
                                PVMVLogType res = createPVMV(line);
                                t=res.getEventTimeStamp();
                                sourceContext.collect(res);
                                if(System.currentTimeMillis()%10==1)sourceContext.collect(res);
                                //logger.info("lefttimetp= {}" ,t);
                            }

                        }
                        long millisToSleep = 1000 - (System.currentTimeMillis() - startTime);
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        return inputStream;
    }

    public static DataStream<PVMVLogType> generatePVMVLog1(StreamExecutionEnvironment env) {
        DataStream<PVMVLogType> inputStream = env.addSource(new ParallelSourceFunction<PVMVLogType>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<PVMVLogType> sourceContext) throws Exception {
                InputStream is = Hxx6000c.class.getClassLoader().getResourceAsStream("data.csv");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    List<String> lines = reader.lines().collect(Collectors.toList());
                    while (running) {
                        long startTime = System.currentTimeMillis();
                        long t=0;
                        synchronized (sourceContext.getCheckpointLock()) {
                            for (int i = 0; i < lines.size(); i++) {
                                //确保文件行数与i的范围一致
                                long line = Long.valueOf(lines.get(i).trim())+20;
                                PVMVLogType res = createPVMV(line);
                                sourceContext.collect(res);
                                t=res.getEventTimeStamp();
                                if(System.currentTimeMillis()%10==1)sourceContext.collect(res);
                                //logger.info("righttimetp= {}" ,t);
                                Thread.sleep(2);
                            }

                        }
                        long millisToSleep = 1000 - (System.currentTimeMillis() - startTime);
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        return inputStream;
    }

    public static PVMVLogType createPVMV(long line) throws IllegalAccessException {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        // 转换为时间戳（毫秒）
        long timestampMillis = Instant.from(now).toEpochMilli();
        line = line + timestampMillis;
        PVMVLogType res = new PVMVLogType(timestampMillis,line);
        res.setPvmvFlowInfo(Gen7KBData.genDataSimple());
        return res;
    }
}
