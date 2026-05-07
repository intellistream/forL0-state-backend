package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import static org.apache.flink.benchmark.benchset.PaperBenchmarkSupport.MODULE_KEY_SELECTOR;

final class PaperBenchmarkJobs {

    private PaperBenchmarkJobs() {
    }

    static void runWc(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        DataStream<PaperBenchmarkSupport.ModuleResult> split = PaperBenchmarkSupport
                .createSource(env, spec, Math.max(1, spec.batchSize), "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long wordId = PaperBenchmarkSupport.route(
                                event.key + event.value + event.sequence,
                                spec.numKeys);
                        return PaperBenchmarkSupport.asModuleResult(event, wordId, 1L, event.value, event.eventType);
                    }
                })
                .name("Split");

        split.keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.CountOperator())
                .name("Count")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DenseValueOperator("WC.DenseValue", 6, true))
                .name("Dense Value")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset WC");
    }

    static void runFd(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        PaperBenchmarkSupport.createSource(env, spec, 4, "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                                                long customerId = event.key;
                                                long transactionId = event.sequence;
                                                long transactionSignal = event.value + (event.auxKey & 63L);
                                                return new PaperBenchmarkSupport.ModuleResult(
                                                                customerId,
                                                                transactionSignal,
                                                                transactionId,
                                                                event.eventType,
                                                                event.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.PredictOperator())
                .name("Predict")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset FD");
    }

    static void runSd(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
                DataStream<PaperBenchmarkSupport.ModuleResult> movingAverageInput = PaperBenchmarkSupport
                .createSource(env, spec, 3, "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        return PaperBenchmarkSupport.asModuleResult(event, event.key, event.value, spec.batchSize, event.eventType);
                    }
                });

        movingAverageInput.keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.MovingAverageOperator())
                .name("Moving Average")
                .map(new MapFunction<PaperBenchmarkSupport.ModuleResult, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.ModuleResult value) {
                        return new PaperBenchmarkSupport.ModuleResult(
                                value.key,
                                value.metric,
                                30L,
                                value.category,
                                value.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.SpikeDetectionOperator())
                .name("Spike Detection")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset SD");
    }

    static void runTm(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        PaperBenchmarkSupport.createSource(env, spec, 4, "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long objectId = event.key;
                        long speed = 20L + (event.value % 80L);
                        long coordinate = event.auxKey * 1024L + (event.sequence % 1024L);
                        return new PaperBenchmarkSupport.ModuleResult(objectId, speed, coordinate, event.eventType, event.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.MapMatchOperator(spec.numKeys))
                .name("Map Match")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DenseMapOperator(
                        "TM.SegmentCache", 1, 2, Math.max(8, spec.batchSize), 4))
                .name("Segment Cache")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.SpeedCalculateOperator())
                .name("Speed Calculate")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DenseReduceMapOperator(
                        "TM.TrafficPressure", 2, Math.max(8, spec.batchSize), 5))
                .name("Traffic Pressure")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset TM");
    }

    static void runLg(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        DataStream<PaperBenchmarkSupport.BenchmarkEvent> source =
                PaperBenchmarkSupport.createSource(env, spec, 8, "Data Source");

        DataStream<PaperBenchmarkSupport.ModuleResult> geoStatus = source
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long ipAddress = event.key;
                        long requestBytes = event.value;
                        long syntheticIp = event.auxKey;
                        return new PaperBenchmarkSupport.ModuleResult(ipAddress, syntheticIp, requestBytes, event.eventType, event.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.GeoFinderOperator())
                .name("Geo Finder")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.GeoStatusOperator())
                .name("Geo Status");

        DataStream<PaperBenchmarkSupport.ModuleResult> statusCounter = source
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long statusCode = 200L + (event.eventType % 5L) * 100L;
                                                long statusKey = PaperBenchmarkSupport.route(event.key * 131L + statusCode + event.sequence, spec.numKeys);
                                                return new PaperBenchmarkSupport.ModuleResult(statusKey, statusCode, event.value, event.eventType, event.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.StatusCounterOperator())
                .name("Status Counter");

        DataStream<PaperBenchmarkSupport.ModuleResult> volumeCounter = source
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long minuteBucket = event.sequence / Math.max(1, spec.batchSize);
                                                long volumeKey = PaperBenchmarkSupport.route(event.key * 17L + minuteBucket, spec.numKeys);
                                                return new PaperBenchmarkSupport.ModuleResult(volumeKey, 1L, minuteBucket, event.eventType, event.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.VolumeCounterOperator())
                .name("Volume Counter");

        geoStatus.union(statusCounter).union(volumeCounter)
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset LG");
    }

    static void runVs(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        DataStream<PaperBenchmarkSupport.ModuleResult> dispatcher = PaperBenchmarkSupport
                .createSource(env, spec, 6, "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long caller = event.key;
                        long callee = event.auxKey;
                        long duration = event.value;
                        return new PaperBenchmarkSupport.ModuleResult(caller, duration, callee, event.eventType, event.sequence);
                    }
                })
                .name("Voice Dispatcher");

        DataStream<PaperBenchmarkSupport.ModuleResult> rcrf = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.RcrfOperator())
                .name("RCRF");

        DataStream<PaperBenchmarkSupport.ModuleResult> ecrf = dispatcher
                .map(new MapFunction<PaperBenchmarkSupport.ModuleResult, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.ModuleResult value) {
                        return new PaperBenchmarkSupport.ModuleResult(
                                value.aux,
                                value.metric,
                                value.key,
                                value.category,
                                value.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.EcrfOperator())
                .name("ECRF");

        DataStream<PaperBenchmarkSupport.ModuleResult> encr = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.EncrOperator())
                .name("ENCR");

        DataStream<PaperBenchmarkSupport.ModuleResult> ct24 = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.Ct24Operator())
                .name("CT24");

        DataStream<PaperBenchmarkSupport.ModuleResult> ecr24 = dispatcher
                .map(new MapFunction<PaperBenchmarkSupport.ModuleResult, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.ModuleResult value) {
                        return new PaperBenchmarkSupport.ModuleResult(
                                value.aux,
                                value.metric,
                                value.key,
                                value.category,
                                value.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.Ecr24Operator())
                .name("ECR24");

        DataStream<PaperBenchmarkSupport.ModuleResult> globalAcd = dispatcher
                .map(new MapFunction<PaperBenchmarkSupport.ModuleResult, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.ModuleResult value) {
                        long shardKey = PaperBenchmarkSupport.route(
                                value.sequence * 17L + value.aux,
                                Math.max(spec.parallelism * 512, 4096));
                        return new PaperBenchmarkSupport.ModuleResult(
                                shardKey,
                                value.metric,
                                value.aux,
                                value.category,
                                value.sequence);
                    }
                })
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.GlobalAcdOperator())
                .name("Global ACD");

        DataStream<PaperBenchmarkSupport.ModuleResult> fofir = rcrf
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.FofirOperator())
                .name("FoFIR");

        DataStream<PaperBenchmarkSupport.ModuleResult> url = ecrf.union(encr)
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.UrlOperator())
                .name("URL");

        DataStream<PaperBenchmarkSupport.ModuleResult> acd = ct24.union(ecr24).union(globalAcd)
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.AcdOperator())
                .name("ACD");

        fofir.union(url).union(acd)
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.ScoreOperator())
                .name("Score")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset VS");
    }

    static void runLr(StreamExecutionEnvironment env, final PaperBenchmarkSupport.BenchmarkSpec spec) throws Exception {
        DataStream<PaperBenchmarkSupport.ModuleResult> dispatcher = PaperBenchmarkSupport
                .createSource(env, spec, 4, "Data Source")
                .map(new MapFunction<PaperBenchmarkSupport.BenchmarkEvent, PaperBenchmarkSupport.ModuleResult>() {
                    @Override
                    public PaperBenchmarkSupport.ModuleResult map(PaperBenchmarkSupport.BenchmarkEvent event) {
                        long segmentId = PaperBenchmarkSupport.route(event.auxKey + event.key, spec.numKeys);
                        long speedOrQuery = 10L + (event.value % 90L);
                        long position = event.auxKey;
                        return new PaperBenchmarkSupport.ModuleResult(segmentId, speedOrQuery, position, event.eventType, event.sequence);
                    }
                })
                .name("Dispatcher");

        DataStream<PaperBenchmarkSupport.ModuleResult> averageSpeed = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.AverageSpeedOperator())
                .name("Average Speed");

        DataStream<PaperBenchmarkSupport.ModuleResult> lastAverageSpeed = averageSpeed
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.LastAverageSpeedOperator())
                .name("Last Average Speed");

        DataStream<PaperBenchmarkSupport.ModuleResult> accidentDetection = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.AccidentDetectionOperator())
                .name("Accident Detection");

        DataStream<PaperBenchmarkSupport.ModuleResult> countVehicles = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.CountVehiclesOperator())
                .name("Count Vehicles");

        DataStream<PaperBenchmarkSupport.ModuleResult> dailyExpenses = dispatcher
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DailyExpensesOperator())
                .name("Daily Expenses");

        DataStream<PaperBenchmarkSupport.ModuleResult> accidentNotification = accidentDetection.union(countVehicles)
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.AccidentNotificationOperator())
                .name("Accident Notification")
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DenseMapOperator(
                        "LR.IncidentIndex", 1, 2, Math.max(8, spec.batchSize), 4))
                .name("Incident Index");

        DataStream<PaperBenchmarkSupport.ModuleResult> tollNotification = lastAverageSpeed.union(accidentNotification).union(dailyExpenses)
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.TollNotificationOperator())
                .name("Toll Notification");

        DataStream<PaperBenchmarkSupport.ModuleResult> accountBalance = tollNotification
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.AccountBalanceOperator())
                .name("Account Balance");

        accountBalance
                .keyBy(MODULE_KEY_SELECTOR)
                .process(new PaperBenchmarkSupport.DenseReduceMapOperator(
                        "LR.AccountPressure", 2, Math.max(8, spec.batchSize), 5))
                .name("Account Pressure")
                .addSink(new DiscardingSink<PaperBenchmarkSupport.ModuleResult>())
                .name("Sink")
                .setParallelism(spec.parallelism);

        env.execute("Paper Benchset LR");
    }
}