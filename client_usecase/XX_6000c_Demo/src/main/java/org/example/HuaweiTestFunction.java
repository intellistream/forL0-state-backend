package org.example;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTestFunction extends MvIncrementProcessFunction<Tuple2<String, String>> {


    private final static Logger logger = LoggerFactory.getLogger(HTestFunction.class);

    public HTestFunction(Time joinDuration, Time deduplicateDuration,
                              boolean enableListDirectEmitBorder, int listDirectEmitBorderSize) {
        super(joinDuration, deduplicateDuration, enableListDirectEmitBorder, listDirectEmitBorderSize);
    }

    // usages 2 implementations
    protected void actionOnMatched(PVMVLogType leftData, PVMVLogType rightData, Collector<Tuple2<String, String>> out) {
        out.collect(Tuple2.of(leftData.joinKey(), rightData.joinKey()));
    }

    // usage 2 implementations
    protected void actionOnDirectEmit(PVMVLogType leftData, Collector<Tuple2<String, String>> out) {
        logger.info("direct emit {}", leftData);
    }

    // usage 2 implementations
    protected void actionOnLeftDataExpired(PVMVLogType leftData, Collector<Tuple2<String, String>> out, long watermark) {
        logger.info("left expired {}", leftData);
    }
}
