package org.example;
//import com.sankai.data.flow.increment.PVMVLogType;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class MvIncrementProcessFunction<OUT>
        extends KeyedCoProcessFunction<String, PVMVLogType, PVMVLogType, OUT> {
    //public static long totalcount = 0;
    //public static long matchcout = 0;
    private final static Logger logger = LoggerFactory.getLogger(MvIncrementProcessFunction.class);
    private final long JOIN_DURATION;
    private final long DEDUPLICATE_DURATION;
    private final boolean ENABLE_LIST_DIRECT_EMIT_BORDER;
    private final int LIST_DIRECT_EMIT_BORDER_SIZE;

    // Cache the rows from left stream
    private transient MapState<Long, List<PVMVLogType>> leftCache;
    // Cache the rows from right stream
    private transient MapState<Long, PVMVLogType> rightCache;


    // Cache the duplicate rows from left stream having been emitted
    private transient MapState<String, Long> leftDuplicateRcd;
    // Cache the duplicate rows from right stream having been emitted
    private transient MapState<String, Long> rightDuplicateRcd;
    // Record the earliest right element.
    private transient ValueState<PVMVLogType> earliestRightElement;
    // Record the last time when the expired rows in left cache are cleaned up
    private transient ValueState<Long> lastLeftDuplicateRcdCleanTime;

    // Record the last time when the expired rows in right cache are cleaned up
    private transient ValueState<Long> lastRightDuplicateRcdCleanTime;

    // Record the last time when the expired rows in left cache are cleaned up
    private transient ValueState<Long> lastLeftCacheCleanTime;
    // Record the last time when the expired rows in right cache are cleaned up
    private transient ValueState<Long> lastRightCacheCleanTime;
    // Record the latest size when the expired rows in left cache are cleaned up
    private transient ValueState<Long> lastestLeftDataTotalSize;


    public MvIncrementProcessFunction(Time joinDuration, Time deduplicateDuration,
                                      boolean enableListDirectEmitBorder, int listDirectEmitBorderSize) {
        this.JOIN_DURATION = joinDuration.toMilliseconds();
        this.DEDUPLICATE_DURATION = deduplicateDuration.toMilliseconds();
        this.ENABLE_LIST_DIRECT_EMIT_BORDER = enableListDirectEmitBorder;
        this.LIST_DIRECT_EMIT_BORDER_SIZE = listDirectEmitBorderSize;
    }

    @Override
    public void open(Configuration parameter) {
        logger.info("Check direct limit size enable:{}, listDirectLimitBorder.size()",
                this.ENABLE_LIST_DIRECT_EMIT_BORDER , this.LIST_DIRECT_EMIT_BORDER_SIZE);

        // Initialize the data cache.
        ListTypeInfo<PVMVLogType> leftRowListTypeInfo = new ListTypeInfo<>(PVMVLogType.class);
        MapStateDescriptor<Long, List<PVMVLogType>> leftMapStateDescriptor =
                new MapStateDescriptor<>(
                        "LeftCache", BasicTypeInfo.LONG_TYPE_INFO, leftRowListTypeInfo);
        leftCache = getRuntimeContext().getMapState(leftMapStateDescriptor);

        MapStateDescriptor<Long, PVMVLogType> rightMapStateDescriptor =
                new MapStateDescriptor<>(
                        "RightCache",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        TypeInformation.of(PVMVLogType.class));
        rightCache = getRuntimeContext().getMapState(rightMapStateDescriptor);

        ValueStateDescriptor<PVMVLogType> earliestRightElementDescriptor = new ValueStateDescriptor<>(
                "EarliestElement", PVMVLogType.class);
        earliestRightElement = getRuntimeContext().getState(earliestRightElementDescriptor);

        // Initialize the timer for cleaning expired cache.
        ValueStateDescriptor<Long> leftCacheCleanTimeDescriptor = new ValueStateDescriptor<>(
                "LeftCacheCleanTime", Long.class);
        lastLeftCacheCleanTime = getRuntimeContext().getState(leftCacheCleanTimeDescriptor);

        ValueStateDescriptor<Long> rightCacheCleanTimeDescriptor = new ValueStateDescriptor<>("RightCacheCleanTime", Long.class);
        lastRightCacheCleanTime = getRuntimeContext().getState(rightCacheCleanTimeDescriptor);

        // Initialize the duplicate states.
        MapStateDescriptor<String, Long> leftDuplicateDesc =
                new MapStateDescriptor<>("LeftDuplicateRcd",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class));
        leftDuplicateRcd = getRuntimeContext().getMapState(leftDuplicateDesc);

        MapStateDescriptor<String, Long> rightDuplicateDesc =
                new MapStateDescriptor<>("RightDuplicateRcd",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class));
        rightDuplicateRcd = getRuntimeContext().getMapState(rightDuplicateDesc);

// Initialize the timer for cleaning duplicate states.
        ValueStateDescriptor<Long> lastLeftDuplicateCleanTimeDesc =
                new ValueStateDescriptor<>("LeftDuplicateRcdCleanerTime",
                        Long.class);
        lastLeftDuplicateRcdCleanTime = getRuntimeContext().getState(lastLeftDuplicateCleanTimeDesc);

        ValueStateDescriptor<Long> lastRightDuplicateCleanTimeDesc =
                new ValueStateDescriptor<>("RightDuplicateRcdCleanerTime",
                        Long.class);
        lastRightDuplicateRcdCleanTime = getRuntimeContext().getState(lastRightDuplicateCleanTimeDesc);

// Initialize the size of left cache data.
        ValueStateDescriptor<Long> lastestLeftDataTotalSizeDesc =
                new ValueStateDescriptor<>("LeftDataTotalSize",
                        Long.class);
        lastestLeftDataTotalSize = getRuntimeContext().getState(lastestLeftDataTotalSizeDesc);

    }

    @Override
    public void processElement1(PVMVLogType leftRow, Context ctx, Collector<OUT> out) throws Exception {
        // 开始检查LeftRow大小限制
        if (this.ENABLE_LIST_DIRECT_EMIT_BORDER && lastestLeftDataTotalSize.value() != null
                && lastestLeftDataTotalSize.value() >= this.LIST_DIRECT_EMIT_BORDER_SIZE) {
            actionOnDirectEmit(leftRow, out);
            return;
        }

        boolean isDuplicateRow = checkAndRecordDuplicateRow(ctx, leftRow, leftDuplicateRcd, lastLeftDuplicateRcdCleanTime);
        if (isDuplicateRow) {
            return;
        }

        PVMVLogType rightRow = earliestRightElement.value();
        if (rightRow != null) {
            actionOnMatched(leftRow, rightRow, out);
            return;
        }

        // 没有满足情况下写入状态，更新size值
        long timeForLeftRow = leftRow.getEventTimeStamp();
        List<PVMVLogType> leftRowList = leftCache.get(timeForLeftRow);
        if (leftRowList == null) {
            leftRowList = new ArrayList<>(1);
        }

        leftRowList.add(leftRow);
        leftCache.put(timeForLeftRow, leftRowList);

// 开启 checkpoint list 累计
        if (this.ENABLE_LIST_DIRECT_EMIT_BORDER) {
            long latestLeftDataTotalSizeUpdateTo = 0;
            Long latestLeftDataTotalSizeValue = lastestLeftDataTotalSize.value();
            if (latestLeftDataTotalSizeValue == null) {
                latestLeftDataTotalSizeUpdateTo = 1;
            } else {
                latestLeftDataTotalSizeUpdateTo = latestLeftDataTotalSizeValue + 1;
            }
            lastestLeftDataTotalSize.update(latestLeftDataTotalSizeUpdateTo);
        }

        if (lastLeftCacheCleanTime.value() == null) {
            registerCleanUpTimer(ctx, timeForLeftRow + JOIN_DURATION, lastLeftCacheCleanTime);
        }
    }

    public void processElement2(PVMVLogType rightRow, Context ctx, Collector<OUT> out)
            throws Exception {
        boolean isDuplicateRow = checkAndRecordDuplicateRow(ctx, rightRow, rightDuplicateRcd, lastRightDuplicateRcdCleanTime);
        if (isDuplicateRow) {
            return;
        }

        final long timeForRightRow = rightRow.getEventTimeStamp();
        Iterator<Map.Entry<Long, List<PVMVLogType>>> leftIterator = leftCache.iterator();
        while (leftIterator.hasNext()) {
            Map.Entry<Long, List<PVMVLogType>> leftEntry = leftIterator.next();
            for (PVMVLogType leftRow : leftEntry.getValue()) {
                actionOnMatched(leftRow, rightRow, out);
            }
            leftIterator.remove();
        }

        // Save the earliest right element.
        PVMVLogType earliestRightRow = earliestRightElement.value();
        if (earliestRightRow == null || rightRow.getEventTimeStamp() < earliestRightRow.getEventTimeStamp()) {
            earliestRightElement.update(rightRow);
        }

        rightCache.put(timeForRightRow, rightRow);
        if (lastRightCacheCleanTime.value() == null) {
            registerCleanUpTimer(ctx, timeForRightRow + JOIN_DURATION, lastRightCacheCleanTime);
        }
    }


    private boolean checkAndRecordDuplicateRow(Context ctx, PVMVLogType row,
                                               MapState<String, Long> duplicateRcd, ValueState<Long> lastCleanTime) throws Exception {
        String duplicateKey = row.duplicateKey();
        if (duplicateRcd.contains(duplicateKey)) {
            return true;
        }
        duplicateRcd.put(duplicateKey, row.getEventTimeStamp());
        if (lastCleanTime.value() == null) {
            registerCleanUpTimer(ctx, row.getEventTimeStamp() + DEDUPLICATE_DURATION, lastCleanTime);
        }
        return false;
    }

    private void registerCleanUpTimer(Context ctx, long cleanUpTime, ValueState<Long> lastCleanTime) throws Exception {
        ctx.timerService().registerEventTimeTimer(cleanUpTime);
        lastCleanTime.update(cleanUpTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        Long rightCleanUpTime = lastRightCacheCleanTime.value();
        if (rightCleanUpTime != null && timestamp == rightCleanUpTime) {
            removeExpiredRightRow(ctx, rightCleanUpTime - JOIN_DURATION);
        }

        Long leftCleanUpTime = lastLeftCacheCleanTime.value();
        if (leftCleanUpTime != null && timestamp == leftCleanUpTime) {
            removeExpiredLeftRows(ctx, leftCleanUpTime - JOIN_DURATION, out);
        }

        Long leftDuplicateRcdCleanUpTime = lastLeftDuplicateRcdCleanTime.value();
        if (leftDuplicateRcdCleanUpTime != null && timestamp == leftDuplicateRcdCleanUpTime) {
            removeExpiredDuplicateRcd(ctx, timestamp - DEDUPLICATE_DURATION, leftDuplicateRcd, lastLeftDuplicateRcdCleanTime);
        }

        Long rightDuplicateRcdCleanUpTime = lastRightDuplicateRcdCleanTime.value();
        if (rightDuplicateRcdCleanUpTime != null && timestamp == rightDuplicateRcdCleanUpTime) {
            removeExpiredDuplicateRcd(ctx, rightDuplicateRcdCleanUpTime - DEDUPLICATE_DURATION,
                    rightDuplicateRcd, lastRightDuplicateRcdCleanTime);
        }
    }


    private void removeExpiredDuplicateRcd(OnTimerContext ctx, long expirationTime, MapState<String, Long> record, ValueState<Long> lastCleanTime) throws Exception {
        Iterator<Map.Entry<String, Long>> iterator = record.iterator();
        long earliestTimestamp = -1L;

        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            Long rowTime = entry.getValue();
            if (rowTime <= expirationTime) {
                iterator.remove();
            } else {
                if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
                    earliestTimestamp = rowTime;
                }
            }
        }

        if (earliestTimestamp > 0) {
            registerCleanUpTimer(ctx, earliestTimestamp + DEDUPLICATE_DURATION, lastCleanTime);
        } else {
            record.clear();
            lastCleanTime.clear();
        }
    }
    private void removeExpiredRightRow(OnTimerContext ctx, long expirationTime) throws Exception {
        Iterator<Map.Entry<Long, PVMVLogType>> iterator = rightCache.iterator();
        long earliestTimestamp = -1L;
        PVMVLogType tmpEarliestElement = null;

        while (iterator.hasNext()) {
            Map.Entry<Long, PVMVLogType> entry = iterator.next();
            Long rowTime = entry.getKey();
            if (rowTime < expirationTime) {
                iterator.remove();
            } else {
                // We find the earliest timestamp that is still valid.
                if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
                    earliestTimestamp = rowTime;
                    tmpEarliestElement = entry.getValue();
                }
            }
        }

        if (earliestTimestamp > 0) {
            PVMVLogType earliestRightRow = earliestRightElement.value();
            if (earliestRightRow == null || earliestTimestamp < earliestRightRow.getEventTimeStamp()) {
                earliestRightElement.update(tmpEarliestElement);
            }
            // There are rows left in the cache. Register a timer to expire them later.
            registerCleanUpTimer(ctx, earliestTimestamp + JOIN_DURATION, lastRightCacheCleanTime);
        } else {
            // No rows left in the cache. Clear the states and the timerState will be 0.
            lastRightCacheCleanTime.clear();
            rightCache.clear();
            earliestRightElement.clear();
        }
    }

    private void removeExpiredLeftRows(OnTimerContext ctx, long expirationTime, Collector<OUT> collector) throws Exception {
        Iterator<Map.Entry<Long, List<PVMVLogType>>> iterator = leftCache.iterator();
        long earliestTimestamp = -1L;

        // Here, we do a full pass over the state.
        while (iterator.hasNext()) {
            Map.Entry<Long, List<PVMVLogType>> entry = iterator.next();
            Long rowTime = entry.getKey();

            if (rowTime < expirationTime) {
                List<PVMVLogType> rows = entry.getValue();
                rows.forEach(row -> {
                    actionOnLeftDataExpired(row, collector, ctx.timerService().currentWatermark());
                });
                iterator.remove();
            } else {
                // We find the earliest timestamp that is still valid.
                if (rowTime < earliestTimestamp || earliestTimestamp < 0) {
                    earliestTimestamp = rowTime;
                }
            }
        }

        if (earliestTimestamp > 0) {
            // There are rows left in the cache. Register a timer to expire them later.
            registerCleanUpTimer(ctx, earliestTimestamp + JOIN_DURATION, lastLeftCacheCleanTime);
        } else {
            // No rows left in the cache. Clear the states and the timerState will be 0.
            lastLeftCacheCleanTime.clear();
            leftCache.clear();
            lastestLeftDataTotalSize.clear();
        }
    }



// usages 2 implementations
        protected abstract void actionOnMatched(PVMVLogType leftData, PVMVLogType rightData, Collector<OUT> out);
// usage 2 implementations
        protected abstract void actionOnDirectEmit(PVMVLogType leftData, Collector<OUT> out);
// usage 2 implementations
        protected abstract void actionOnLeftDataExpired(PVMVLogType leftData, Collector<OUT> out, long watermark);


    }
