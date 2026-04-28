package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

final class PaperBenchmarkSupport {

    private PaperBenchmarkSupport() {
    }

    static final KeySelector<ModuleResult, Long> MODULE_KEY_SELECTOR =
            new KeySelector<ModuleResult, Long>() {
                @Override
                public Long getKey(ModuleResult value) {
                    return value.key;
                }
            };

    static DataStream<BenchmarkEvent> createSource(
            StreamExecutionEnvironment env,
            BenchmarkSpec spec,
            int eventKinds,
            String name) {
        return env.addSource(new SyntheticEventSource(
                        spec.numKeys,
                        spec.numRecords,
                        spec.skewFactor,
                eventKinds,
                spec.batchSize))
                .setParallelism(spec.parallelism)
                .name(name);
    }

    static long route(long candidate, int numKeys) {
        long mod = candidate % numKeys;
        return mod >= 0 ? mod : mod + numKeys;
    }

    static ModuleResult asModuleResult(
            BenchmarkEvent event,
            long key,
            long metric,
            long aux,
            int category) {
        return new ModuleResult(key, metric, aux, category, event.sequence);
    }

    static final class BenchmarkSpec implements Serializable {
        private static final long serialVersionUID = 1L;

        final int numKeys;
        final long numRecords;
        final double skewFactor;
        final int batchSize;
        final int parallelism;
        final String backend;

        BenchmarkSpec(
                int numKeys,
                long numRecords,
                double skewFactor,
                int batchSize,
                int parallelism,
                String backend) {
            this.numKeys = numKeys;
            this.numRecords = numRecords;
            this.skewFactor = skewFactor;
            this.batchSize = batchSize;
            this.parallelism = parallelism;
            this.backend = backend;
        }
    }

    static final class BenchmarkEvent implements Serializable {
        public long key;
        public long auxKey;
        public long value;
        public int eventType;
        public long sequence;

        public BenchmarkEvent() {
        }

        BenchmarkEvent(long key, long auxKey, long value, int eventType, long sequence) {
            this.key = key;
            this.auxKey = auxKey;
            this.value = value;
            this.eventType = eventType;
            this.sequence = sequence;
        }
    }

    static final class ModuleResult implements Serializable {
        public long key;
        public long metric;
        public long aux;
        public int category;
        public long sequence;

        public ModuleResult() {
        }

        ModuleResult(long key, long metric, long aux, int category, long sequence) {
            this.key = key;
            this.metric = metric;
            this.aux = aux;
            this.category = category;
            this.sequence = sequence;
        }
    }

    static final class CategoryFilter implements FilterFunction<ModuleResult> {
        private static final long serialVersionUID = 1L;

        private final int modulo;
        private final int[] accepted;

        CategoryFilter(int modulo, int... accepted) {
            this.modulo = modulo;
            this.accepted = accepted;
        }

        @Override
        public boolean filter(ModuleResult value) {
            int bucket = Math.floorMod(value.category, modulo);
            for (int candidate : accepted) {
                if (bucket == candidate) {
                    return true;
                }
            }
            return false;
        }
    }

    private abstract static class FastPathOperator extends KeyedProcessFunction<Long, ModuleResult, ModuleResult> {
        private static final long serialVersionUID = 1L;

        private enum AuxiliaryMode {
            DENSE_VALUE,
            HOT_MAP
        }

        private transient ValueState<Long> activityState;
        private transient ValueState<Long> recencyState;
        private transient ValueState<Long> variationState;
        private transient MapState<Long, Long> hotMapState;
        private transient ReducingState<Long> hotSumState;

        private transient boolean pressureEnabled;
        private transient int hotMapWidth;
        private transient int hotTouchCount;
        private transient long hotSalt;
        private transient AuxiliaryMode auxiliaryMode;

        protected ValueState<Long> longState(String name) {
            return getRuntimeContext().getState(new ValueStateDescriptor<>(name, Long.class));
        }

        protected ReducingState<Long> reducingState(String name, org.apache.flink.api.common.functions.ReduceFunction<Long> reducer) {
            return getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(name, reducer, Long.class));
        }

        protected void openDensityStates(String operatorName) {
            activityState = longState(operatorName + ".activity");
            recencyState = longState(operatorName + ".recency");
            variationState = longState(operatorName + ".variation");
            pressureEnabled = false;
            auxiliaryMode = AuxiliaryMode.DENSE_VALUE;
        }

        protected void openDensityStates(String operatorName, int mapWidth, int touchCount) {
            openDensityStates(operatorName);
            hotMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".hotMap", Long.class, Long.class));
            hotSumState = reducingState(operatorName + ".hotSum", Long::sum);
            pressureEnabled = true;
            hotMapWidth = Math.max(4, mapWidth);
            hotTouchCount = Math.max(2, touchCount);
            hotSalt = Math.abs((long) operatorName.hashCode());
        }

            protected void openHotMapStates(String operatorName, int mapWidth, int touchCount) {
                hotMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".hotMap", Long.class, Long.class));
                hotSumState = reducingState(operatorName + ".hotSum", Long::sum);
                pressureEnabled = true;
                hotMapWidth = Math.max(4, mapWidth);
                hotTouchCount = Math.max(2, touchCount);
                hotSalt = Math.abs((long) operatorName.hashCode());
                auxiliaryMode = AuxiliaryMode.HOT_MAP;
            }

        protected void touchAuxiliaryStates(ModuleResult value, long primaryMetric, long secondaryMetric) throws Exception {
                if (auxiliaryMode == AuxiliaryMode.HOT_MAP) {
                long sample = primaryMetric
                    + positiveModulo(secondaryMetric + value.metric + value.aux, 4096L)
                    + 1L;
                hotSumState.add(sample);
                long hotSum = valueOrDefault(hotSumState.get(), sample);
                long baseSlot = positiveModulo(
                    hotSalt
                        + (value.category * 5L)
                        + positiveModulo(primaryMetric + secondaryMetric + value.metric, hotTouchCount + 1L),
                    hotMapWidth);
                long hotAccumulator = hotSum;
                for (int touch = 0; touch < hotTouchCount; touch++) {
                    long slot = positiveModulo(baseSlot + touch, hotMapWidth);
                    boolean present = hotMapState.contains(slot);
                    long current = valueOrDefault(
                        hotMapState.get(slot),
                        positiveModulo(hotSalt + slot + value.key + hotSum, 2048L));
                    long next = current
                        + positiveModulo(primaryMetric + secondaryMetric + hotAccumulator + (touch * 17L), 97L)
                        + 1L;
                    hotMapState.put(slot, next);
                    hotAccumulator = mix(hotAccumulator + (present ? current : 0L) + next + slot);
                }
                return;
                }

            long previousActivity = valueOrDefault(
                activityState.value(),
                positiveModulo(primaryMetric + value.metric + value.sequence, 1024L));
            long previousRecency = valueOrDefault(recencyState.value(), value.sequence & 1023L);
            long previousVariation = valueOrDefault(
                variationState.value(),
                positiveModulo(secondaryMetric + value.aux + value.category, 2048L));

            long nextActivity = previousActivity
                + positiveModulo(primaryMetric + secondaryMetric + value.metric, 17L)
                + 1L;
            long nextRecency = (previousRecency * 3L
                + positiveModulo(value.sequence + primaryMetric + value.category, 97L)) / 2L;
            long nextVariation = previousVariation
                + Math.abs(primaryMetric - secondaryMetric)
                + (value.category & 7L);

            activityState.update(nextActivity);
            recencyState.update(nextRecency);
            variationState.update(nextVariation);
        }

        protected static long valueOrDefault(Long value, long fallback) {
            return value == null ? fallback : value;
        }

        protected static long positiveModulo(long value, long modulo) {
            if (modulo <= 0L) {
                return value;
            }
            long result = value % modulo;
            return result >= 0 ? result : result + modulo;
        }
    }

    static final class CountOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = longState("Count.wordCount");
            openDensityStates("Count");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCount = valueOrDefault(count.value(), 0L) + value.metric;
            count.update(nextCount);
            touchAuxiliaryStates(value, nextCount, value.aux);
            out.collect(new ModuleResult(value.key, nextCount, value.aux, value.category, value.sequence));
        }
    }

    static final class PredictOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> lastTxnType;
        private transient ValueState<Long> lastTxnId;
        private transient ValueState<Long> transitionScore;
        private transient ValueState<Long> pathCount;

        @Override
        public void open(Configuration parameters) {
            lastTxnType = longState("Predict.lastTxnType");
            lastTxnId = longState("Predict.lastTxnId");
            transitionScore = longState("Predict.transitionScore");
            pathCount = longState("Predict.pathCount");
            openHotMapStates("Predict", 16, 6);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long previousType = valueOrDefault(lastTxnType.value(), value.category);
            long previousTxn = valueOrDefault(lastTxnId.value(), value.sequence);
            long score = valueOrDefault(transitionScore.value(), 0L);
            long count = valueOrDefault(pathCount.value(), 0L) + 1L;

            long transitionPenalty = Math.abs(value.category - previousType) * 11L;
            long txnGap = Math.abs(value.sequence - previousTxn) & 31L;
            long nextScore = (score * 3L + transitionPenalty + txnGap + value.metric) / 2L;

            lastTxnType.update((long) value.category);
            lastTxnId.update(value.sequence);
            transitionScore.update(nextScore);
            pathCount.update(count);
            touchAuxiliaryStates(value, nextScore, count);
            out.collect(new ModuleResult(value.key, nextScore, count, value.category, value.sequence));
        }
    }

    static final class MovingAverageOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> runningSum;
        private transient ValueState<Long> runningCount;
        private transient ValueState<Long> lastAverage;

        @Override
        public void open(Configuration parameters) {
            runningSum = longState("MovingAverage.runningSum");
            runningCount = longState("MovingAverage.runningCount");
            lastAverage = longState("MovingAverage.lastAverage");
            openHotMapStates("MovingAverage", 12, 4);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long window = Math.max(1L, value.aux);
            long nextSum = valueOrDefault(runningSum.value(), 0L) + value.metric;
            long nextCount = Math.min(window, valueOrDefault(runningCount.value(), 0L) + 1L);
            long average = nextSum / nextCount;

            if (nextCount >= window) {
                nextSum = average;
                nextCount = 1L;
            }

            runningSum.update(nextSum);
            runningCount.update(nextCount);
            lastAverage.update(average);
            touchAuxiliaryStates(value, average, nextCount);
            out.collect(new ModuleResult(value.key, average, value.aux, value.category, value.sequence));
        }
    }

    static final class SpikeDetectionOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> alertCount;
        private transient ValueState<Long> lastAverage;

        @Override
        public void open(Configuration parameters) {
            alertCount = longState("SpikeDetection.alertCount");
            lastAverage = longState("SpikeDetection.lastAverage");
            openHotMapStates("SpikeDetection", 14, 6);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long previousAverage = valueOrDefault(lastAverage.value(), value.metric);
            long alerts = valueOrDefault(alertCount.value(), 0L);
            if (value.metric > value.aux) {
                alerts += 1L;
            }
            lastAverage.update(value.metric);
            alertCount.update(alerts);
            touchAuxiliaryStates(value, alerts, previousAverage);
            out.collect(new ModuleResult(value.key, alerts, previousAverage, value.category, value.sequence));
        }
    }

    static final class MapMatchOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final int numKeys;

        private transient ValueState<Long> lastRoadId;
        private transient ValueState<Long> lastCoordinate;

        MapMatchOperator(int numKeys) {
            this.numKeys = numKeys;
        }

        @Override
        public void open(Configuration parameters) {
            lastRoadId = longState("MapMatch.lastRoadId");
            lastCoordinate = longState("MapMatch.lastCoordinate");
            openDensityStates("MapMatch");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long coordinate = value.metric * 31L + value.aux;
            long roadId = route(coordinate + value.sequence, numKeys);
            lastRoadId.update(roadId);
            lastCoordinate.update(coordinate);
            touchAuxiliaryStates(value, roadId, coordinate);
            out.collect(new ModuleResult(roadId, value.metric, coordinate, value.category, value.sequence));
        }
    }

    static final class SpeedCalculateOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> speedSum;
        private transient ValueState<Long> sampleCount;
        private transient ValueState<Long> lastAverage;

        @Override
        public void open(Configuration parameters) {
            speedSum = longState("SpeedCalculate.speedSum");
            sampleCount = longState("SpeedCalculate.sampleCount");
            lastAverage = longState("SpeedCalculate.lastAverage");
            openDensityStates("SpeedCalculate");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long sum = valueOrDefault(speedSum.value(), 0L) + value.metric;
            long count = valueOrDefault(sampleCount.value(), 0L) + 1L;
            long average = sum / count;
            speedSum.update(sum);
            sampleCount.update(count);
            lastAverage.update(average);
            touchAuxiliaryStates(value, average, count);
            out.collect(new ModuleResult(value.key, average, count, value.category, value.sequence));
        }
    }

    static final class GeoFinderOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> countryState;
        private transient ValueState<Long> cityState;

        @Override
        public void open(Configuration parameters) {
            countryState = longState("GeoFinder.country");
            cityState = longState("GeoFinder.city");
            openHotMapStates("GeoFinder", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long country = positiveModulo(value.metric + value.aux, 251L);
            long city = positiveModulo(value.metric * 13L + value.aux, 4096L);
            countryState.update(country);
            cityState.update(city);
            touchAuxiliaryStates(value, country, city);
            out.collect(new ModuleResult(value.key, country, city, value.category, value.sequence));
        }
    }

    static final class GeoStatusOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> countriesSeen;
        private transient ValueState<Long> citiesSeen;

        @Override
        public void open(Configuration parameters) {
            countriesSeen = longState("GeoStatus.countriesSeen");
            citiesSeen = longState("GeoStatus.citiesSeen");
            openHotMapStates("GeoStatus", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCountries = valueOrDefault(countriesSeen.value(), 0L) + 1L;
            long nextCities = valueOrDefault(citiesSeen.value(), 0L) + (value.aux & 1L);
            countriesSeen.update(nextCountries);
            citiesSeen.update(nextCities);
            touchAuxiliaryStates(value, nextCountries, nextCities);
            out.collect(new ModuleResult(value.key, nextCountries, nextCities, value.category, value.sequence));
        }
    }

    static final class StatusCounterOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> statusCount;
        private transient ValueState<Long> byteCount;

        @Override
        public void open(Configuration parameters) {
            statusCount = longState("StatusCounter.statusCount");
            byteCount = longState("StatusCounter.byteCount");
            openHotMapStates("StatusCounter", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCount = valueOrDefault(statusCount.value(), 0L) + 1L;
            long nextBytes = valueOrDefault(byteCount.value(), 0L) + value.aux;
            statusCount.update(nextCount);
            byteCount.update(nextBytes);
            touchAuxiliaryStates(value, nextCount, nextBytes);
            out.collect(new ModuleResult(value.key, nextCount, nextBytes, value.category, value.sequence));
        }
    }

    static final class VolumeCounterOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> volumeCount;

        @Override
        public void open(Configuration parameters) {
            volumeCount = longState("VolumeCounter.volumeCount");
            openHotMapStates("VolumeCounter", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCount = valueOrDefault(volumeCount.value(), 0L) + 1L;
            volumeCount.update(nextCount);
            touchAuxiliaryStates(value, nextCount, value.aux);
            out.collect(new ModuleResult(value.key, nextCount, value.aux, value.category, value.sequence));
        }
    }

    static final class RcrfOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> callCount;
        private transient ValueState<Long> uniqueTargets;

        @Override
        public void open(Configuration parameters) {
            callCount = longState("RCRF.callCount");
            uniqueTargets = longState("RCRF.uniqueTargets");
            openHotMapStates("RCRF", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCalls = valueOrDefault(callCount.value(), 0L) + 1L;
            long nextTargets = valueOrDefault(uniqueTargets.value(), 0L) + ((value.aux & 3L) == 0L ? 1L : 0L);
            callCount.update(nextCalls);
            uniqueTargets.update(nextTargets);
            touchAuxiliaryStates(value, nextCalls, nextTargets);
            out.collect(new ModuleResult(value.key, nextCalls, nextTargets, value.category, value.sequence));
        }
    }

    static final class EcrfOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> receivedCount;
        private transient ValueState<Long> answerCount;

        @Override
        public void open(Configuration parameters) {
            receivedCount = longState("ECRF.receivedCount");
            answerCount = longState("ECRF.answerCount");
            openHotMapStates("ECRF", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long received = valueOrDefault(receivedCount.value(), 0L) + 1L;
            long answers = valueOrDefault(answerCount.value(), 0L) + ((value.category & 1) == 0 ? 1L : 0L);
            receivedCount.update(received);
            answerCount.update(answers);
            touchAuxiliaryStates(value, received, answers);
            out.collect(new ModuleResult(value.key, received, answers, value.category, value.sequence));
        }
    }

    static final class EncrOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> establishedCount;
        private transient ValueState<Long> neighborhoodScore;

        @Override
        public void open(Configuration parameters) {
            establishedCount = longState("ENCR.establishedCount");
            neighborhoodScore = longState("ENCR.neighborhoodScore");
            openHotMapStates("ENCR", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long established = valueOrDefault(establishedCount.value(), 0L) + ((value.aux & 1L) == 0L ? 1L : 0L);
            long score = valueOrDefault(neighborhoodScore.value(), 0L) + positiveModulo(value.metric + value.aux, 7L);
            establishedCount.update(established);
            neighborhoodScore.update(score);
            touchAuxiliaryStates(value, score, established);
            out.collect(new ModuleResult(value.key, score, established, value.category, value.sequence));
        }
    }

    static final class Ct24Operator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> callCount24h;
        private transient ValueState<Long> duration24h;

        @Override
        public void open(Configuration parameters) {
            callCount24h = longState("CT24.callCount24h");
            duration24h = longState("CT24.duration24h");
            openHotMapStates("CT24", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long calls = valueOrDefault(callCount24h.value(), 0L) + 1L;
            long duration = valueOrDefault(duration24h.value(), 0L) + value.metric;
            callCount24h.update(calls);
            duration24h.update(duration);
            touchAuxiliaryStates(value, calls, duration);
            out.collect(new ModuleResult(value.key, calls, duration, value.category, value.sequence));
        }
    }

    static final class Ecr24Operator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> established24h;
        private transient ValueState<Long> completed24h;

        @Override
        public void open(Configuration parameters) {
            established24h = longState("ECR24.established24h");
            completed24h = longState("ECR24.completed24h");
            openHotMapStates("ECR24", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long established = valueOrDefault(established24h.value(), 0L) + ((value.aux & 1L) == 0L ? 1L : 0L);
            long completed = valueOrDefault(completed24h.value(), 0L) + ((value.metric & 1L) == 0L ? 1L : 0L);
            established24h.update(established);
            completed24h.update(completed);
            touchAuxiliaryStates(value, established, completed);
            out.collect(new ModuleResult(value.key, established, completed, value.category, value.sequence));
        }
    }

    static final class GlobalAcdOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> totalDuration;
        private transient ValueState<Long> totalCalls;

        @Override
        public void open(Configuration parameters) {
            totalDuration = longState("GlobalACD.totalDuration");
            totalCalls = longState("GlobalACD.totalCalls");
            openHotMapStates("GlobalACD", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long duration = valueOrDefault(totalDuration.value(), 0L) + value.metric;
            long calls = valueOrDefault(totalCalls.value(), 0L) + 1L;
            totalDuration.update(duration);
            totalCalls.update(calls);
            long acd = duration / Math.max(1L, calls);
            touchAuxiliaryStates(value, acd, calls);
            out.collect(new ModuleResult(value.key, acd, calls, value.category, value.sequence));
        }
    }

    static final class FofirOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> alertCount;
        private transient ValueState<Long> fraudScore;

        @Override
        public void open(Configuration parameters) {
            alertCount = longState("FoFIR.alertCount");
            fraudScore = longState("FoFIR.fraudScore");
            openHotMapStates("FoFIR", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long alerts = valueOrDefault(alertCount.value(), 0L) + ((value.aux > 4L) ? 1L : 0L);
            long score = valueOrDefault(fraudScore.value(), 0L) + value.metric + value.aux;
            alertCount.update(alerts);
            fraudScore.update(score);
            touchAuxiliaryStates(value, score, alerts);
            out.collect(new ModuleResult(value.key, score, alerts, value.category, value.sequence));
        }
    }

    static final class UrlOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> urlScore;
        private transient ValueState<Long> evidenceCount;

        @Override
        public void open(Configuration parameters) {
            urlScore = longState("URL.urlScore");
            evidenceCount = longState("URL.evidenceCount");
            openHotMapStates("URL", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long evidence = valueOrDefault(evidenceCount.value(), 0L) + 1L;
            long score = valueOrDefault(urlScore.value(), 0L) + value.metric + value.aux;
            urlScore.update(score);
            evidenceCount.update(evidence);
            touchAuxiliaryStates(value, score, evidence);
            out.collect(new ModuleResult(value.key, score, evidence, value.category, value.sequence));
        }
    }

    static final class AcdOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> acdScore;
        private transient ValueState<Long> contributingModules;

        @Override
        public void open(Configuration parameters) {
            acdScore = longState("ACD.acdScore");
            contributingModules = longState("ACD.contributingModules");
            openHotMapStates("ACD", 24, 8);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long contributors = valueOrDefault(contributingModules.value(), 0L) + 1L;
            long score = valueOrDefault(acdScore.value(), 0L) + value.metric;
            acdScore.update(score);
            contributingModules.update(contributors);
            touchAuxiliaryStates(value, score, contributors);
            out.collect(new ModuleResult(value.key, score, contributors, value.category, value.sequence));
        }
    }

    static final class ScoreOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> finalScore;
        private transient ValueState<Long> decisionCount;

        @Override
        public void open(Configuration parameters) {
            finalScore = longState("Score.finalScore");
            decisionCount = longState("Score.decisionCount");
            openHotMapStates("Score", 32, 12);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextScore = valueOrDefault(finalScore.value(), 0L) + value.metric + value.aux;
            long decisions = valueOrDefault(decisionCount.value(), 0L) + 1L;
            finalScore.update(nextScore);
            decisionCount.update(decisions);
            touchAuxiliaryStates(value, nextScore, decisions);
            out.collect(new ModuleResult(value.key, nextScore, decisions, value.category, value.sequence));
        }
    }

    static final class AverageSpeedOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> speedSum;
        private transient ValueState<Long> vehicleCount;

        @Override
        public void open(Configuration parameters) {
            speedSum = longState("AverageSpeed.speedSum");
            vehicleCount = longState("AverageSpeed.vehicleCount");
            openDensityStates("AverageSpeed");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long sum = valueOrDefault(speedSum.value(), 0L) + value.metric;
            long count = valueOrDefault(vehicleCount.value(), 0L) + 1L;
            speedSum.update(sum);
            vehicleCount.update(count);
            touchAuxiliaryStates(value, sum / count, count);
            out.collect(new ModuleResult(value.key, sum / count, count, value.category, value.sequence));
        }
    }

    static final class LastAverageSpeedOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> lastAverage;
        private transient ValueState<Long> trend;

        @Override
        public void open(Configuration parameters) {
            lastAverage = longState("LastAverageSpeed.lastAverage");
            trend = longState("LastAverageSpeed.trend");
            openDensityStates("LastAverageSpeed");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long previous = valueOrDefault(lastAverage.value(), value.metric);
            long delta = value.metric - previous;
            lastAverage.update(value.metric);
            trend.update(delta);
            touchAuxiliaryStates(value, value.metric, delta);
            out.collect(new ModuleResult(value.key, value.metric, delta, value.category, value.sequence));
        }
    }

    static final class AccidentDetectionOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> stalledCount;
        private transient ValueState<Long> lastPosition;

        @Override
        public void open(Configuration parameters) {
            stalledCount = longState("AccidentDetection.stalledCount");
            lastPosition = longState("AccidentDetection.lastPosition");
            openDensityStates("AccidentDetection");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long previous = valueOrDefault(lastPosition.value(), value.aux);
            long stalled = valueOrDefault(stalledCount.value(), 0L);
            if (previous == value.aux) {
                stalled += 1L;
            }
            lastPosition.update(value.aux);
            stalledCount.update(stalled);
            touchAuxiliaryStates(value, stalled, value.aux);
            out.collect(new ModuleResult(value.key, stalled, value.aux, value.category, value.sequence));
        }
    }

    static final class CountVehiclesOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> vehicleCount;

        @Override
        public void open(Configuration parameters) {
            vehicleCount = longState("CountVehicles.vehicleCount");
            openDensityStates("CountVehicles");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long count = valueOrDefault(vehicleCount.value(), 0L) + 1L;
            vehicleCount.update(count);
            touchAuxiliaryStates(value, count, value.aux);
            out.collect(new ModuleResult(value.key, count, value.aux, value.category, value.sequence));
        }
    }

    static final class AccidentNotificationOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> activeAccidents;

        @Override
        public void open(Configuration parameters) {
            activeAccidents = longState("AccidentNotification.activeAccidents");
            openDensityStates("AccidentNotification");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long active = valueOrDefault(activeAccidents.value(), 0L) + ((value.metric > 0L) ? 1L : 0L);
            activeAccidents.update(active);
            touchAuxiliaryStates(value, active, value.aux);
            out.collect(new ModuleResult(value.key, active, value.aux, value.category, value.sequence));
        }
    }

    static final class DailyExpensesOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> tollSum;
        private transient ValueState<Long> distanceSum;

        @Override
        public void open(Configuration parameters) {
            tollSum = longState("DailyExpenses.tollSum");
            distanceSum = longState("DailyExpenses.distanceSum");
            openDensityStates("DailyExpenses");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long toll = valueOrDefault(tollSum.value(), 0L) + positiveModulo(value.metric + value.aux, 50L);
            long distance = valueOrDefault(distanceSum.value(), 0L) + value.metric;
            tollSum.update(toll);
            distanceSum.update(distance);
            touchAuxiliaryStates(value, toll, distance);
            out.collect(new ModuleResult(value.key, toll, distance, value.category, value.sequence));
        }
    }

    static final class TollNotificationOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> currentToll;
        private transient ValueState<Long> congestion;

        @Override
        public void open(Configuration parameters) {
            currentToll = longState("TollNotification.currentToll");
            congestion = longState("TollNotification.congestion");
            openDensityStates("TollNotification");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCongestion = valueOrDefault(congestion.value(), 0L) + ((value.metric > 40L) ? 1L : 0L);
            long toll = valueOrDefault(currentToll.value(), 0L) + positiveModulo(value.metric + nextCongestion, 25L);
            congestion.update(nextCongestion);
            currentToll.update(toll);
            touchAuxiliaryStates(value, toll, nextCongestion);
            out.collect(new ModuleResult(value.key, toll, nextCongestion, value.category, value.sequence));
        }
    }

    static final class AccountBalanceOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> balance;
        private transient ValueState<Long> paymentCount;

        @Override
        public void open(Configuration parameters) {
            balance = longState("AccountBalance.balance");
            paymentCount = longState("AccountBalance.paymentCount");
            openDensityStates("AccountBalance");
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextBalance = valueOrDefault(balance.value(), 0L) + value.metric;
            long nextPayments = valueOrDefault(paymentCount.value(), 0L) + 1L;
            balance.update(nextBalance);
            paymentCount.update(nextPayments);
            touchAuxiliaryStates(value, nextBalance, nextPayments);
            out.collect(new ModuleResult(value.key, nextBalance, nextPayments, value.category, value.sequence));
        }
    }

    static final class StatefulStage extends KeyedProcessFunction<Long, ModuleResult, ModuleResult> {
        private static final long serialVersionUID = 1L;

        private final String stageName;
        private final String[] stateNames;

        private transient List<ValueState<Long>> states;

        StatefulStage(String stageName, String... stateNames) {
            this.stageName = stageName;
            this.stateNames = stateNames;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            states = new ArrayList<>(stateNames.length);
            for (String stateName : stateNames) {
                states.add(getRuntimeContext().getState(
                        new ValueStateDescriptor<>(stageName + "." + stateName, Long.class)));
            }
        }

        @Override
        public void processElement(
                ModuleResult value,
                Context ctx,
                Collector<ModuleResult> out) throws Exception {
            long accumulator = value.metric + value.aux + value.sequence + (value.category & 0xffL);

            for (int index = 0; index < states.size(); index++) {
                ValueState<Long> state = states.get(index);
                Long current = state.value();
                long currentValue = current == null ? seed(value, index) : current;
                long next = currentValue + delta(value, accumulator, index);
                state.update(next);
                accumulator = mix(accumulator + next + stageName.charAt(index % stageName.length()));
            }

            out.collect(new ModuleResult(
                    value.key,
                    accumulator,
                    value.metric + stateNames.length,
                    value.category,
                    value.sequence));
        }

        private static long seed(ModuleResult value, int index) {
            return ((value.key + 1L) * (index + 3L) + value.metric + value.aux) & 1023L;
        }

        private static long delta(ModuleResult value, long accumulator, int index) {
            long shifted = value.key >>> (index & 7);
            return ((accumulator ^ shifted ^ value.metric ^ (index * 17L)) & 31L) + 1L;
        }
    }

    static final class BackendPressureOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> rollingCount;
        private transient ValueState<Long> rollingTotal;
        private transient ValueState<Long> rollingChecksum;
        private transient ValueState<Long> previousBucketState;
        private transient MapState<Long, Long> scratchBuckets;

        private final String operatorName;

        BackendPressureOperator(String operatorName) {
            this.operatorName = operatorName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            rollingCount = longState(operatorName + ".rollingCount");
            rollingTotal = longState(operatorName + ".rollingTotal");
            rollingChecksum = longState(operatorName + ".rollingChecksum");
            previousBucketState = longState(operatorName + ".previousBucket");
            scratchBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".scratchBuckets", Long.class, Long.class));
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long nextCount = valueOrDefault(rollingCount.value(), 0L) + 1L;
            long nextTotal = valueOrDefault(rollingTotal.value(), 0L) + value.metric + value.aux;
            long previousChecksum = valueOrDefault(rollingChecksum.value(), mix(value.metric + value.aux + value.sequence));

            long bucket = positiveModulo(value.key + value.sequence + value.category, 2L);
            long previousBucket = valueOrDefault(previousBucketState.value(), bucket ^ 1L);
            Long previousBucketValue = scratchBuckets.get(bucket);
            long bucketSeed = ((value.key + 1L) * (bucket + 3L) + value.metric + value.aux) & 1023L;
            long nextBucketValue = valueOrDefault(previousBucketValue, bucketSeed)
                    + positiveModulo(nextTotal + previousChecksum + value.sequence, 97L)
                    + 1L;

            scratchBuckets.put(bucket, nextBucketValue);
            if (previousBucket != bucket) {
                scratchBuckets.remove(previousBucket);
            }
            previousBucketState.update(bucket);

            long nextChecksum = mix(previousChecksum + nextTotal + nextBucketValue + nextCount);

            rollingCount.update(nextCount);
            rollingTotal.update(nextTotal);
            rollingChecksum.update(nextChecksum);
            touchAuxiliaryStates(value, nextChecksum, nextBucketValue);

            out.collect(new ModuleResult(
                    value.key,
                    nextChecksum,
                    nextBucketValue + nextCount,
                    value.category,
                    value.sequence));
        }
    }

    static final class DenseValueOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final String operatorName;
        private final int stateCount;
        private final boolean withReducers;

        private transient List<ValueState<Long>> valueStates;
        private transient ReducingState<Long> runningSum;
        private transient ReducingState<Long> runningMax;

        DenseValueOperator(String operatorName, int stateCount, boolean withReducers) {
            this.operatorName = operatorName;
            this.stateCount = stateCount;
            this.withReducers = withReducers;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueStates = new ArrayList<>(stateCount);
            for (int index = 0; index < stateCount; index++) {
                valueStates.add(longState(operatorName + ".value." + index));
            }
            if (withReducers) {
                runningSum = reducingState(operatorName + ".sum", Long::sum);
                runningMax = reducingState(operatorName + ".max", Long::max);
            }
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long accumulator = mix(value.metric + value.aux + value.sequence + operatorName.length());

            if (withReducers) {
                runningSum.add(value.metric);
                runningMax.add(value.metric + (value.category & 31L));
                accumulator = mix(
                        accumulator
                                + valueOrDefault(runningSum.get(), value.metric)
                                + valueOrDefault(runningMax.get(), value.metric));
            }

            for (int index = 0; index < valueStates.size(); index++) {
                ValueState<Long> state = valueStates.get(index);
                long current = valueOrDefault(state.value(), positiveModulo(accumulator + index, 4096L));
                long next = current
                        + positiveModulo(accumulator + value.metric + value.aux + (index * 13L), 127L)
                        + 1L;
                state.update(next);
                accumulator = mix(accumulator + current + next + (index * 17L));
            }

            touchAuxiliaryStates(value, accumulator, value.metric + value.aux);
            out.collect(new ModuleResult(value.key, accumulator, value.aux, value.category, value.sequence));
        }
    }

    static final class DenseAggregateValueOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final String operatorName;
        private final int valueStateCount;

        private transient ReducingState<Long> runningSum;
        private transient ReducingState<Long> runningMin;
        private transient ReducingState<Long> runningMax;
        private transient List<ValueState<Long>> valueStates;

        DenseAggregateValueOperator(String operatorName, int valueStateCount) {
            this.operatorName = operatorName;
            this.valueStateCount = valueStateCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            runningSum = reducingState(operatorName + ".sum", Long::sum);
            runningMin = reducingState(operatorName + ".min", Long::min);
            runningMax = reducingState(operatorName + ".max", Long::max);

            valueStates = new ArrayList<>(valueStateCount);
            for (int index = 0; index < valueStateCount; index++) {
                valueStates.add(longState(operatorName + ".value." + index));
            }
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long sample = value.metric
                    + positiveModulo(value.aux, 4096L)
                    + positiveModulo(value.sequence, 257L)
                    + (value.category & 31L)
                    + 1L;

            runningSum.add(sample);
            runningMin.add(sample);
            runningMax.add(sample + (value.category & 15L));

            long sum = valueOrDefault(runningSum.get(), sample);
            long min = valueOrDefault(runningMin.get(), sample);
            long max = valueOrDefault(runningMax.get(), sample);
            long accumulator = mix(sum + min + max + value.sequence + operatorName.length());

            for (int index = 0; index < valueStates.size(); index++) {
                ValueState<Long> state = valueStates.get(index);
                long current = valueOrDefault(state.value(), positiveModulo(accumulator + index, 4096L));
                long next = current
                        + positiveModulo(sum + max + value.aux + (index * 29L), 127L)
                        + positiveModulo(accumulator + min + (index * 7L), 31L)
                        + 1L;
                state.update(next);
                accumulator = mix(accumulator + current + next + sum + max + index);
            }

            touchAuxiliaryStates(value, accumulator, max - min);
            out.collect(new ModuleResult(value.key, accumulator, sum, value.category, value.sequence));
        }
    }

    static final class HotMapOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final String operatorName;
        private final int valueStateCount;
        private final int mapWidth;
        private final int touchCount;

        private transient List<ValueState<Long>> valueStates;
        private transient MapState<Long, Long> hotMap;
        private transient ReducingState<Long> runningSum;

        HotMapOperator(String operatorName, int valueStateCount, int mapWidth, int touchCount) {
            this.operatorName = operatorName;
            this.valueStateCount = valueStateCount;
            this.mapWidth = Math.max(4, mapWidth);
            this.touchCount = Math.max(2, touchCount);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueStates = new ArrayList<>(valueStateCount);
            for (int index = 0; index < valueStateCount; index++) {
                valueStates.add(longState(operatorName + ".value." + index));
            }
            hotMap = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".hotMap", Long.class, Long.class));
            runningSum = reducingState(operatorName + ".sum", Long::sum);
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long sample = value.metric + positiveModulo(value.aux, 2048L) + 1L;
            runningSum.add(sample);
            long sum = valueOrDefault(runningSum.get(), sample);

            long accumulator = mix(sum + value.metric + value.aux + value.sequence + operatorName.hashCode());
            for (int index = 0; index < valueStates.size(); index++) {
                ValueState<Long> state = valueStates.get(index);
                long current = valueOrDefault(state.value(), positiveModulo(accumulator + index, 4096L));
                long next = current + positiveModulo(sum + accumulator + (index * 13L), 89L) + 1L;
                state.update(next);
                accumulator = mix(accumulator + current + next + index);
            }

            long baseSlot = positiveModulo(
                    (value.category * 5L)
                            + positiveModulo(value.metric, Math.max(2, touchCount))
                            + (operatorName.length() * 3L),
                    mapWidth);
            for (int touch = 0; touch < touchCount; touch++) {
                long slot = positiveModulo(baseSlot + touch, mapWidth);
                boolean present = hotMap.contains(slot);
                long current = valueOrDefault(hotMap.get(slot), positiveModulo(value.key + slot + sum, 2048L));
                long next = current + positiveModulo(accumulator + value.metric + value.aux + (touch * 17L), 97L) + 1L;
                hotMap.put(slot, next);
                accumulator = mix(accumulator + (present ? current : 0L) + next + slot);
            }

            if (hotMap.contains(baseSlot)) {
                accumulator = mix(accumulator + baseSlot + touchCount);
            }

            touchAuxiliaryStates(value, accumulator, sum);
            out.collect(new ModuleResult(value.key, accumulator, sum, value.category, value.sequence));
        }
    }

    static final class DenseMapOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final String operatorName;
        private final int valueStateCount;
        private final int mapStateCount;
        private final int mapWidth;
        private final int removeStride;

        private transient List<ValueState<Long>> valueStates;
        private transient List<MapState<Long, Long>> mapStates;

        DenseMapOperator(String operatorName, int valueStateCount, int mapStateCount, int mapWidth, int removeStride) {
            this.operatorName = operatorName;
            this.valueStateCount = valueStateCount;
            this.mapStateCount = mapStateCount;
            this.mapWidth = Math.max(4, mapWidth);
            this.removeStride = Math.max(2, removeStride);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            valueStates = new ArrayList<>(valueStateCount);
            for (int index = 0; index < valueStateCount; index++) {
                valueStates.add(longState(operatorName + ".value." + index));
            }
            mapStates = new ArrayList<>(mapStateCount);
            for (int index = 0; index < mapStateCount; index++) {
                mapStates.add(getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(operatorName + ".map." + index, Long.class, Long.class)));
            }
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long accumulator = mix(value.metric + value.aux + value.sequence + operatorName.hashCode());

            for (int index = 0; index < valueStates.size(); index++) {
                ValueState<Long> state = valueStates.get(index);
                long current = valueOrDefault(state.value(), positiveModulo(accumulator + index, 2048L));
                long next = current + positiveModulo(accumulator + value.metric + (index * 19L), 71L) + 1L;
                state.update(next);
                accumulator = mix(accumulator + current + next + value.aux);
            }

            for (int index = 0; index < mapStates.size(); index++) {
                MapState<Long, Long> mapState = mapStates.get(index);
                long slot = positiveModulo(value.sequence + value.metric + (index * 7L), mapWidth);
                long neighborSlot = positiveModulo(slot + value.category + index + 1L, mapWidth);
                long retireSlot = positiveModulo(slot + (mapWidth / 2L) + 1L, mapWidth);

                boolean present = mapState.contains(slot);
                long current = valueOrDefault(mapState.get(slot), positiveModulo(accumulator + slot, 1024L));
                long next = current + positiveModulo(accumulator + value.metric + value.aux + (index * 29L), 95L) + 1L;
                mapState.put(slot, next);

                long neighbor = valueOrDefault(mapState.get(neighborSlot), positiveModulo(next + value.key, 2048L));
                mapState.put(neighborSlot, neighbor + 1L);

                if ((value.sequence + index) % removeStride == 0L) {
                    mapState.remove(retireSlot);
                }

                accumulator = mix(accumulator + (present ? current : 0L) + next + neighbor + slot);
            }

            touchAuxiliaryStates(value, accumulator, value.metric + mapWidth);
            out.collect(new ModuleResult(value.key, accumulator, value.metric, value.category, value.sequence));
        }
    }

    static final class DenseReduceMapOperator extends FastPathOperator {
        private static final long serialVersionUID = 1L;

        private final String operatorName;
        private final int valueStateCount;
        private final int mapWidth;
        private final int removeStride;

        private transient ReducingState<Long> runningSum;
        private transient ReducingState<Long> runningMin;
        private transient ReducingState<Long> runningMax;
        private transient List<ValueState<Long>> valueStates;
        private transient List<MapState<Long, Long>> mapStates;

        DenseReduceMapOperator(String operatorName, int valueStateCount, int mapWidth, int removeStride) {
            this.operatorName = operatorName;
            this.valueStateCount = valueStateCount;
            this.mapWidth = Math.max(4, mapWidth);
            this.removeStride = Math.max(2, removeStride);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            runningSum = reducingState(operatorName + ".sum", Long::sum);
            runningMin = reducingState(operatorName + ".min", Long::min);
            runningMax = reducingState(operatorName + ".max", Long::max);

            valueStates = new ArrayList<>(valueStateCount);
            for (int index = 0; index < valueStateCount; index++) {
                valueStates.add(longState(operatorName + ".value." + index));
            }

            mapStates = new ArrayList<>(2);
            mapStates.add(getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".map.primary", Long.class, Long.class)));
            mapStates.add(getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(operatorName + ".map.secondary", Long.class, Long.class)));
            openDensityStates(operatorName);
        }

        @Override
        public void processElement(ModuleResult value, Context ctx, Collector<ModuleResult> out) throws Exception {
            long sample = value.metric + positiveModulo(value.aux, 4096L) + 1L;
            runningSum.add(sample);
            runningMin.add(sample);
            runningMax.add(sample + (value.category & 31L));

            long sum = valueOrDefault(runningSum.get(), sample);
            long min = valueOrDefault(runningMin.get(), sample);
            long max = valueOrDefault(runningMax.get(), sample);
            long accumulator = mix(sum + min + max + value.sequence);

            for (int index = 0; index < valueStates.size(); index++) {
                ValueState<Long> state = valueStates.get(index);
                long current = valueOrDefault(state.value(), positiveModulo(accumulator + index, 2048L));
                long next = current + positiveModulo(sum + max + (index * 23L), 83L) + 1L;
                state.update(next);
                accumulator = mix(accumulator + current + next + min);
            }

            for (int index = 0; index < mapStates.size(); index++) {
                MapState<Long, Long> mapState = mapStates.get(index);
                long slot = positiveModulo(value.key + value.sequence + (index * 11L), mapWidth);
                long retireSlot = positiveModulo(slot + (mapWidth / 2L) + index + 1L, mapWidth);
                long current = valueOrDefault(mapState.get(slot), positiveModulo(accumulator + slot, 1024L));
                long next = current + positiveModulo(sum + max + (index * 31L), 61L) + 1L;
                mapState.put(slot, next);
                if ((value.sequence + index) % removeStride == 0L) {
                    mapState.remove(retireSlot);
                }
                accumulator = mix(accumulator + current + next + slot);
            }

            touchAuxiliaryStates(value, accumulator, max - min);
            out.collect(new ModuleResult(value.key, accumulator, sum, value.category, value.sequence));
        }
    }

    private static long mix(long value) {
        value ^= (value >>> 33);
        value *= 0xff51afd7ed558ccdl;
        value ^= (value >>> 33);
        return value;
    }

    static final class SyntheticEventSource extends RichParallelSourceFunction<BenchmarkEvent> {
        private static final long serialVersionUID = 1L;

        private final int numKeys;
        private final long numRecords;
        private final double skewFactor;
        private final int eventKinds;
        private final int batchSize;

        private volatile boolean running = true;

        SyntheticEventSource(int numKeys, long numRecords, double skewFactor, int eventKinds, int batchSize) {
            this.numKeys = numKeys;
            this.numRecords = numRecords;
            this.skewFactor = skewFactor;
            this.eventKinds = eventKinds;
            this.batchSize = Math.max(1, batchSize);
        }

        @Override
        public void run(SourceContext<BenchmarkEvent> ctx) throws Exception {
            int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            long recordsPerSubtask = numRecords / parallelism;
            long myRecords = subtaskIndex == parallelism - 1
                    ? numRecords - recordsPerSubtask * (parallelism - 1)
                    : recordsPerSubtask;

            SplittableRandom random = new SplittableRandom(17L + 31L * subtaskIndex);
            boolean useZipf = skewFactor > 0;
            double[] cdf = useZipf ? computeZipfCDF(numKeys, skewFactor) : null;
            int localityWindow = Math.max(64, Math.min(4096, Math.max(batchSize * 16, Math.max(64, numKeys / 1024))));
            long[] activeKeys = new long[localityWindow];
            long[] activeAuxKeys = new long[localityWindow];
            for (int index = 0; index < localityWindow; index++) {
                activeKeys[index] = sampleKey(random, cdf, useZipf, numKeys);
                activeAuxKeys[index] = route(activeKeys[index] * 17L + random.nextLong(numKeys), numKeys);
            }
            Object lock = ctx.getCheckpointLock();

            for (long count = 0; running && count < myRecords; count++) {
                int slot = (int) (count % localityWindow);
                if (count % batchSize == 0L) {
                    activeKeys[slot] = sampleKey(random, cdf, useZipf, numKeys);
                    activeAuxKeys[slot] = route(activeKeys[slot] * 17L + random.nextLong(numKeys), numKeys);
                }

                long key;
                long auxKey;
                if (random.nextDouble() < 0.38d) {
                    int hotIndex = random.nextInt(localityWindow);
                    key = activeKeys[hotIndex];
                    auxKey = route(activeAuxKeys[hotIndex] + random.nextLong(Math.max(1, numKeys / Math.max(32, localityWindow / 2))), numKeys);
                } else {
                    key = sampleKey(random, cdf, useZipf, numKeys);
                    auxKey = route(key * 17L + random.nextLong(numKeys), numKeys);
                    activeKeys[slot] = key;
                    activeAuxKeys[slot] = auxKey;
                }
                long value = 1L + random.nextLong(1000L);
                int eventType = eventKinds == 1 ? 0 : random.nextInt(eventKinds);
                long sequence = count + (subtaskIndex * recordsPerSubtask);

                synchronized (lock) {
                    ctx.collect(new BenchmarkEvent(key, auxKey, value, eventType, sequence));
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private static double[] computeZipfCDF(int n, double s) {
            double[] cdf = new double[n];
            double sum = 0.0d;
            for (int i = 1; i <= n; i++) {
                sum += 1.0d / Math.pow(i, s);
            }
            double cumulative = 0.0d;
            for (int i = 0; i < n; i++) {
                cumulative += (1.0d / Math.pow(i + 1, s)) / sum;
                cdf[i] = cumulative;
            }
            return cdf;
        }

        private static long sampleZipf(SplittableRandom random, double[] cdf) {
            double sample = random.nextDouble();
            int low = 0;
            int high = cdf.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (cdf[mid] < sample) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        private static long sampleKey(SplittableRandom random, double[] cdf, boolean useZipf, int numKeys) {
            return useZipf ? sampleZipf(random, cdf) : random.nextLong(numKeys);
        }
    }
}