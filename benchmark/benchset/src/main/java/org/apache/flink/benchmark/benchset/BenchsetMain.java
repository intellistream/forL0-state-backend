package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BenchsetMain {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String benchmark = params.get("benchmark", "wc").toLowerCase();
        int numKeys = params.getInt("numKeys", 1_000_000);
        long numRecords = params.getLong("numRecords", 100_000_000L);
        double skewFactor = params.getDouble("skewFactor", 0.0d);
        int batchSize = params.getInt("batchSize", 8);
        int parallelism = params.getInt("parallelism", 8);
        int checkpointInterval = params.getInt("checkpointInterval", 0);
        String backend = params.get("backend", "unknown");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(checkpointInterval);
        }

        PaperBenchmarkSupport.BenchmarkSpec spec = new PaperBenchmarkSupport.BenchmarkSpec(
                numKeys,
                numRecords,
                skewFactor,
                batchSize,
                parallelism,
                backend);

        System.out.println("=== ForL0 Paper Benchset ===");
        System.out.println("Benchmark: " + benchmark);
        System.out.println("Backend: " + backend);
        System.out.println("numKeys: " + numKeys);
        System.out.println("numRecords: " + numRecords);
        System.out.println("skewFactor: " + skewFactor);
        System.out.println("batchSize: " + batchSize);
        System.out.println("parallelism: " + parallelism);
        System.out.println("checkpointing: " + (checkpointInterval > 0 ? checkpointInterval + "ms" : "disabled"));
        System.out.println("============================");

        if ("wc".equals(benchmark)) {
            PaperBenchmarkJobs.runWc(env, spec);
        } else if ("fd".equals(benchmark)) {
            PaperBenchmarkJobs.runFd(env, spec);
        } else if ("sd".equals(benchmark)) {
            PaperBenchmarkJobs.runSd(env, spec);
        } else if ("tm".equals(benchmark)) {
            PaperBenchmarkJobs.runTm(env, spec);
        } else if ("lg".equals(benchmark)) {
            PaperBenchmarkJobs.runLg(env, spec);
        } else if ("vs".equals(benchmark)) {
            PaperBenchmarkJobs.runVs(env, spec);
        } else if ("lr".equals(benchmark)) {
            PaperBenchmarkJobs.runLr(env, spec);
        } else {
            System.err.println("Unknown benchmark: " + benchmark);
            System.err.println("Available: wc, fd, sd, tm, lg, vs, lr");
            System.exit(1);
        }
    }
}