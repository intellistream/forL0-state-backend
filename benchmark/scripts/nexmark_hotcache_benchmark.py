#!/usr/bin/env python3
"""Isolated, identical-SQL Heap/ForL0 comparison on a 2 x 4-core/32-GiB cluster.

The cache arm explicitly loads the separately built DRAM test allocator. It
measures implementation overhead, never hardware-L0 latency or acceleration.
"""
import argparse
import fcntl
import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time
import urllib.request
import yaml


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--flink", type=Path, default=Path("/root/opt/flink-1.20.3"))
    parser.add_argument("--queries", default="4,5,8,9,11,18,19,20")
    parser.add_argument("--arms", default="heap,off,cache")
    parser.add_argument("--config", type=Path, help="Project benchmark.yaml (defaults to source/benchmark/config/benchmark.yaml)")
    parser.add_argument("--events", type=int, help="Explicitly override the project's per-query event counts")
    parser.add_argument("--warmup-events", type=int, default=0)
    parser.add_argument("--checkpoint-dir", type=Path, help="Dedicated scratch directory for this campaign")
    parser.add_argument("--checkpoint-ms", type=int, help="Override the project checkpoint interval")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--native-offheap", default="16g", help="Native state reservation within each 32-GiB TM budget")
    parser.add_argument("--lock-file", type=Path, default=Path("/tmp/forl0-nexmark-hotcache.lock"),
                        help="Serialize campaigns that use the same TaskManager CPU cores")
    args = parser.parse_args()
    campaign_lock = args.lock_file.open("w")
    fcntl.flock(campaign_lock, fcntl.LOCK_EX)
    root = args.root.resolve()
    source, build = root / "source", root / "build"
    config_path = args.config or source / "benchmark/config/benchmark.yaml"
    config = yaml.safe_load(config_path.read_text())
    workload = config["nexmark"]
    checkpoint_ms = args.checkpoint_ms if args.checkpoint_ms is not None else config["runtime"]["nexmark_checkpoint_interval"]
    checkpoints = (args.checkpoint_dir or root / "checkpoints").resolve()
    checkpoints.mkdir(parents=True, exist_ok=True)
    (root / "protocol.json").write_text(json.dumps({
        "config": str(config_path), "workload": workload, "checkpoint_ms": checkpoint_ms,
        "checkpoint_dir": str(checkpoints), "events_override": args.events,
        "warmup_events": args.warmup_events, "parallelism": 8,
        "queries": args.queries, "arms": args.arms, "repeats": args.repeats,
        "taskmanagers": 2, "cores_per_taskmanager": 4, "process_memory_per_taskmanager": "32g",
        "native_offheap": args.native_offheap,
        "allocator": "DRAM_TEST_ONLY", "flink": str(args.flink),
    }, indent=2))
    queries = source / "benchmark/nexmark-src/nexmark-flink/src/main/resources/queries"
    results = []
    dist = root / "flink"
    dist.mkdir(exist_ok=True)
    for directory in ("bin", "conf"):
        if not (dist / directory).exists():
            shutil.copytree(args.flink / directory, dist / directory)
    for directory in ("lib", "opt", "plugins"):
        (dist / directory).mkdir(exist_ok=True)
        for item in (args.flink / directory).iterdir():
            if "statebackend-forL0" in item.name:
                continue
            dst = dist / directory / item.name
            if not dst.exists(): dst.symlink_to(item)
    backend = source / "target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
    dst = dist / "lib" / backend.name
    if dst.is_symlink(): dst.unlink()
    dst.symlink_to(backend)

    # Compile the original source connector against the same Flink runtime.
    connector = root / "nexmark-connector.jar"
    java = source / "benchmark/nexmark-src/nexmark-flink/src/main/java/com/github/nexmark/flink"
    files = sorted(p for d in ("generator", "model", "source") for p in (java / d).rglob("*.java") if p.name != "SideInputGenerator.java")
    files += [java / "NexmarkConfiguration.java", java / "utils/NexmarkUtils.java"]
    digest = hashlib.sha256(b"".join(p.read_bytes() for p in files)).hexdigest()
    fingerprint = root / "connector-source.sha256"
    if not connector.exists() or not fingerprint.exists() or fingerprint.read_text() != digest:
        classes = root / "connector-classes"
        classes.mkdir(exist_ok=True)
        subprocess.run(["javac", "-source", "8", "-target", "8", "-cp", str(args.flink / "lib/*"), "-d", str(classes), *map(str, files)], check=True)
        service = classes / "META-INF/services/org.apache.flink.table.factories.Factory"
        service.parent.mkdir(parents=True, exist_ok=True)
        service.write_text("com.github.nexmark.flink.source.NexmarkTableSourceFactory\n")
        subprocess.run(["jar", "cf", str(connector), "-C", str(classes), "."], check=True)
        fingerprint.write_text(digest)
    dst = dist / "lib" / connector.name
    if not dst.exists(): dst.symlink_to(connector)

    def request(rest, path):
        with urllib.request.urlopen(rest + path, timeout=10) as response:
            return json.load(response)

    def task_pid(parent):
        pending = [parent]
        while pending:
            pid = pending.pop()
            cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
            if b"org.apache.flink.runtime.taskexecutor.TaskManagerRunner" in cmd: return pid
            pending.extend(map(int, Path(f"/proc/{pid}/task/{pid}/children").read_text().split()))
        raise RuntimeError("Cannot find owned TaskManager JVM")

    def cpu(pids):
        total = 0
        for pid in pids:
            stat = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
            total += int(stat[11]) + int(stat[12])
        return total / os.sysconf("SC_CLK_TCK")

    def gc_cpu(run):
        total = 0.0
        for name in ("tm1", "tm2"):
            log = run / name / "gc.log"
            if log.exists():
                for user, system in re.findall(r"User=([\d.]+)s Sys=([\d.]+)s", log.read_text()):
                    total += float(user) + float(system)
        return total

    def cluster(run, arm):
        rest_port, rpc_port, blob_port = port(), port(), port()
        native = run / "native"
        native.mkdir()
        library = build / ("libforl0_engine_test_l0.so" if arm == "cache" else "libforl0_engine.so")
        (native / "libforl0_engine.so").symlink_to(library)
        rest = f"http://127.0.0.1:{rest_port}"
        configs, processes = [], []
        for name, cores in (("jm", range(32, 36)), ("tm1", range(24, 28)), ("tm2", range(28, 32))):
            home = run / name
            conf = home / "conf"
            shutil.copytree(args.flink / "conf", conf)
            (home / "log").mkdir()
            values = {
                "jobmanager.rpc.address": "127.0.0.1", "jobmanager.rpc.port": rpc_port,
                "rest.address": "127.0.0.1", "rest.bind-address": "127.0.0.1", "rest.port": rest_port,
                "blob.server.port": blob_port, "jobmanager.memory.process.size": "8g",
                "taskmanager.host": "127.0.0.1", "taskmanager.bind-host": "127.0.0.1",
                "taskmanager.memory.process.size": "32g", "taskmanager.numberOfTaskSlots": 4,
                "taskmanager.memory.task.off-heap.size": "0b" if arm == "heap" else args.native_offheap, "taskmanager.memory.managed.size": "256m",
                "taskmanager.memory.network.min": "512m", "taskmanager.memory.network.max": "512m",
                "taskmanager.cpu.cores": 4, "parallelism.default": 8,
                "state.backend.type": "hashmap" if arm == "heap" else "org.apache.flink.state.forl0.ForL0StateBackendFactory",
                "state.backend.forl0.l0-cache.enabled": str(arm == "cache").lower(),
                "state.backend.forl0.l0-cache.size": "32mb",
                "state.backend.forl0.l0-cache.strict-allocation": "true",
                "state.checkpoints.dir": "file://" + str(checkpoints / run.name),
                "execution.checkpointing.checkpoints-after-tasks-finish.enabled": "false",
                "table.exec.mini-batch.enabled": "true", "table.exec.mini-batch.allow-latency": "2s",
                "table.exec.mini-batch.size": 50000, "table.optimizer.distinct-agg.split.enabled": "true",
                "restart-strategy.type": "none", "io.tmp.dirs": str(home / "tmp"),
                "env.java.opts.all": "-XX:ActiveProcessorCount=4 -Djava.library.path=" + str(native)
                    + " -Xlog:gc,gc+cpu=info:file=" + str(home / "gc.log") + ":time,uptime,level,tags",
            }
            if checkpoint_ms > 0: values["execution.checkpointing.interval"] = checkpoint_ms
            (conf / "flink-conf.yaml").write_text("\n".join(f"{k}: {v}" for k, v in values.items()) + "\n")
            configs.append(conf)
            env = dict(os.environ, FLINK_HOME=str(dist), FLINK_CONF_DIR=str(conf), FLINK_LOG_DIR=str(home / "log"), JAVA_HOME="/usr/lib/jvm/java-11-openjdk", LD_LIBRARY_PATH=str(native))
            command = [str(dist / "bin" / ("jobmanager.sh" if name == "jm" else "taskmanager.sh")), "start-foreground"]
            proc = subprocess.Popen(command, env=env, stdout=open(home / "console.log", "w"), stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=lambda cores=cores: os.sched_setaffinity(0, cores))
            processes.append(proc)
        try:
            for _ in range(120):
                if any(p.poll() is not None for p in processes): raise RuntimeError("Cluster process exited")
                try:
                    overview = request(rest, "/overview")
                    if overview["taskmanagers"] == 2 and overview["slots-total"] == 8: break
                except (OSError, KeyError): pass
                time.sleep(1)
            else: raise RuntimeError("Cluster not ready")
        except BaseException:
            stop(processes)
            raise
        return rest, configs[0], processes

    def stop(processes):
        for p in processes:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGTERM)
        for p in processes:
            try: p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()

    def execute(run, rest, conf, processes, query, events, phase):
        replacements = {"TPS": str(workload["tps"]), "EVENTS_NUM": str(events),
                        "PERSON_PROPORTION": str(workload["person_proportion"]),
                        "AUCTION_PROPORTION": str(workload["auction_proportion"]),
                        "BID_PROPORTION": str(workload["bid_proportion"]), "NEXMARK_TABLE": "datagen"}
        sql = "\n".join((queries / name).read_text() for name in ("ddl_gen.sql", "ddl_views.sql", f"q{query}.sql"))
        for key, value in replacements.items(): sql = sql.replace("${" + key + "}", value)
        sql = "SET 'execution.attached' = 'false';\n" + sql
        sql_file = run / (phase + ".sql")
        sql_file.write_text(sql)
        log = run / (phase + "-client.log")
        env = dict(os.environ, FLINK_HOME=str(dist), FLINK_CONF_DIR=str(conf), JAVA_HOME="/usr/lib/jvm/java-11-openjdk", FLINK_LOG_DIR=str(run / "jm/log"))
        before = cpu([task_pid(p.pid) for p in processes[1:]])
        gc_before = gc_cpu(run)
        with log.open("w") as stream:
            submit = subprocess.run([str(dist / "bin/sql-client.sh"), "-f", str(sql_file)], env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=120)
        text = log.read_text()
        ids = re.findall(r"Job ID:\s*([0-9a-f]{32})", text)
        if submit.returncode or not ids: raise RuntimeError("SQL submission failed: " + str(log))
        job_id = ids[-1]
        started = time.monotonic()
        while time.monotonic() - started < args.timeout:
            job = request(rest, "/jobs/" + job_id)
            if job["state"] in ("FINISHED", "FAILED", "CANCELED"): break
            time.sleep(1)
        else: raise TimeoutError(f"Q{query} job {job_id}")
        (run / (phase + "-job.json")).write_text(json.dumps(job, indent=2))
        checkpoint_stats = request(rest, f"/jobs/{job_id}/checkpoints")
        (run / (phase + "-checkpoints.json")).write_text(json.dumps(checkpoint_stats, indent=2))
        if job["state"] != "FINISHED":
            (run / (phase + "-exceptions.json")).write_text(json.dumps(request(rest, f"/jobs/{job_id}/exceptions"), indent=2))
            raise RuntimeError(f"Q{query} state={job['state']}")
        duration = job["duration"] / 1000
        resources = []
        for process in processes[1:]:
            pid = task_pid(process.pid)
            status = Path(f"/proc/{pid}/status").read_text()
            peak_kib = int(re.search(r"VmHWM:\s+(\d+)", status).group(1))
            affinities = set()
            for thread in Path(f"/proc/{pid}/task").iterdir():
                try: affinities.add(tuple(sorted(os.sched_getaffinity(int(thread.name)))))
                except ProcessLookupError: pass  # A completed task thread may exit during inspection.
            resources.append({"pid": pid, "peak_rss_kib": peak_kib, "thread_affinities": sorted(affinities)})
            if peak_kib > 32 * 1024 * 1024: raise RuntimeError("TaskManager exceeded the 32-GiB process budget")
            if any(len(cores) != 4 for cores in affinities): raise RuntimeError("TaskManager thread escaped its four-core affinity")
        (run / (phase + "-resources.json")).write_text(json.dumps(resources, indent=2))
        traffic = {"engines": 0, "states": 0, "lookups": 0, "hits": 0, "writes": 0}
        malformed_records = 0
        console = "\n".join((run / name / "console.log").read_text() for name in ("tm1", "tm2"))
        for line in console.splitlines():
            if "[ForL0-HotCache] engine_summary" in line: traffic["engines"] += 1
            # Manager summaries share the process pool and overlap between
            # engines. Each state summary occurs once and avoids double counting.
            if "[ForL0-HotCache] state_summary" not in line: continue
            record = line.split("[ForL0-HotCache] state_summary", 1)[1]
            counters = {key: re.search(rf"\b{key}=(\d+)", record) for key in ("lookups", "hits", "writes")}
            if not all(counters.values()):
                malformed_records += 1
                continue
            traffic["states"] += 1
            for key in ("lookups", "hits", "writes"):
                traffic[key] += int(counters[key].group(1))
        if "state.backend.forl0.l0-cache.enabled: true" in conf.joinpath("flink-conf.yaml").read_text():
            if not all(traffic[key] > 0 for key in ("engines", "states", "writes")) or "TEST ALLOCATOR: DRAM" not in console:
                raise RuntimeError("Cache arm did not show real owned-payload cache traffic")
        return {"query": query, "events": events, "seconds": duration, "events_per_second": events / duration,
                "tm_cpu_seconds": cpu([task_pid(p.pid) for p in processes[1:]]) - before, "job_id": job_id,
                "tm_gc_cpu_seconds": gc_cpu(run) - gc_before,
                "checkpoint_counts": checkpoint_stats.get("counts", {}), "cache_traffic": traffic,
                "malformed_counter_records": malformed_records, "tm_resources": resources}

    out = root / "runs"
    out.mkdir(exist_ok=True)
    for repeat in range(args.repeats):
        for query in map(int, args.queries.split(",")):
            arms = args.arms.split(",")
            if repeat % 2: arms.reverse()
            for arm in arms:
                run = out / f"q{query}-{arm}-{repeat}"
                run.mkdir()
                progress = {"query": query, "arm": arm, "repeat": repeat, "phase": "starting"}
                (root / "progress.json").write_text(json.dumps(progress))
                print(json.dumps(progress), flush=True)
                rest, conf, processes = cluster(run, arm)
                try:
                    if args.warmup_events:
                        execute(run, rest, conf, processes, query, args.warmup_events, "warmup")
                    progress["phase"] = "measure"
                    (root / "progress.json").write_text(json.dumps(progress))
                    events = args.events if args.events is not None else workload[f"q{query}_events"]
                    result = execute(run, rest, conf, processes, query, events, "measure")
                    result.update(arm=arm, repeat=repeat, allocation="DRAM_TEST_ONLY" if arm == "cache" else "off")
                    results.append(result)
                    (root / "results.json").write_text(json.dumps(results, indent=2))
                    print(json.dumps(result), flush=True)
                finally:
                    stop(processes)
                    shutil.rmtree(checkpoints / run.name, ignore_errors=True)
    (root / "progress.json").write_text(json.dumps({"phase": "complete", "runs": len(results)}))


if __name__ == "__main__": main()
