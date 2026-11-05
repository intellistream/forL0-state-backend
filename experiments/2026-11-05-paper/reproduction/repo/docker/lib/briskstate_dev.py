#!/usr/bin/env python3
"""Single-container Flink 1.20.3 development cluster; standard library only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.request

REPO = Path(__file__).resolve().parents[2]
RUNTIME = Path(os.environ.get("BRISKSTATE_RUNTIME", "/root/briskstate-runtime"))
FLINK = Path(os.environ.get("FLINK_HOME", "/root/opt/flink-1.20.3"))
JAVA = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk")
REST = "http://127.0.0.1:8081"
ROLES = {"jm": ("0-3", "jobmanager.sh"), "tm1": ("4-7", "taskmanager.sh"), "tm2": ("8-11", "taskmanager.sh")}
ENV = dict(os.environ, JAVA_HOME=JAVA, FLINK_HOME=str(FLINK), OMP_NUM_THREADS="4")
ENV["PATH"] = JAVA + "/bin:" + ENV["PATH"]

def run(args, **kwargs):
    print("+", " ".join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), check=True, env=ENV, **kwargs)

def api(path):
    with urllib.request.urlopen(REST + path, timeout=10) as response:
        return json.load(response)

def role_env(role):
    env = ENV.copy()
    for key, dirname in [("FLINK_CONF_DIR", "conf"), ("FLINK_LOG_DIR", "log"), ("FLINK_PID_DIR", "pid")]:
        env[key] = str(RUNTIME / role / dirname)
    env["FLINK_IDENT_STRING"] = "briskstate-" + role
    return env

def configure():
    if not (FLINK / "bin/flink").exists():
        raise RuntimeError("Install the verified Flink 1.20.3 distribution first: " + str(FLINK))
    for role in ROLES:
        root = RUNTIME / role
        for name in ("conf", "log", "pid", "tmp"):
            (root / name).mkdir(parents=True, exist_ok=True)
        for source in (FLINK / "conf").glob("*"):
            if source.is_file() and source.name not in ("config.yaml", "flink-conf.yaml"):
                shutil.copy2(source, root / "conf" / source.name)
        config = {
            "env.java.home": JAVA,
            "env.java.opts.all": "-XX:ActiveProcessorCount=4 -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer -Djava.library.path=" + str(RUNTIME / "native") + " -Xlog:gc*,safepoint:file=" + str(root / "log/gc-%p.log") + ":time,uptime,level,tags:filecount=4,filesize=64M",
            "jobmanager.rpc.address": "127.0.0.1",
            "jobmanager.rpc.port": 6123,
            "jobmanager.bind-host": "127.0.0.1",
            "jobmanager.memory.process.size": "8g",
            "taskmanager.host": "127.0.0.1",
            "taskmanager.bind-host": "127.0.0.1",
            "taskmanager.rpc.port": 0,
            "taskmanager.data.port": 0,
            "taskmanager.resource-id": "briskstate-" + role,
            "taskmanager.cpu.cores": 4.0,
            "taskmanager.numberOfTaskSlots": 4,
            "taskmanager.memory.process.size": os.environ.get("BRISKSTATE_TM_MEMORY", "32g"),
            "taskmanager.memory.task.off-heap.size": os.environ.get("BRISKSTATE_TM_OFFHEAP", "8g"),
            "taskmanager.memory.managed.size": "256m",
            "taskmanager.memory.network.min": "512m",
            "taskmanager.memory.network.max": "512m",
            "parallelism.default": 8,
            "rest.address": "127.0.0.1",
            "rest.bind-address": "127.0.0.1",
            "rest.port": 8081,
            "rest.bind-port": "8081",
            "state.backend.type": "hashmap",
            "state.backend.forl0.l0-cache.enabled": False,
            "state.backend.forl0.native-memory.max-size": "1g",
            "state.checkpoint-storage": "filesystem",
            "state.checkpoints.dir": "file://" + str(RUNTIME / "checkpoints"),
            "state.savepoints.dir": "file://" + str(RUNTIME / "savepoints"),
            "io.tmp.dirs": str(root / "tmp"),
        }
        (root / "conf/config.yaml").write_text("".join(k + ": " + json.dumps(v) + "\n" for k, v in config.items()))
    for name in ("checkpoints", "savepoints", "native", "build"):
        (RUNTIME / name).mkdir(parents=True, exist_ok=True)
    (RUNTIME / "env.sh").write_text(
        "export JAVA_HOME=" + JAVA + "\nexport FLINK_HOME=" + str(FLINK) +
        "\nexport BRISKSTATE_RUNTIME=" + str(RUNTIME) +
        '\nexport PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$PATH"\n' +
        "export FLINK_CONF_DIR=" + str(RUNTIME / "jm/conf") + "\n")

def build():
    for role in ROLES:
        for pidfile in (RUNTIME / role / "pid").glob("*.pid"):
            for line in pidfile.read_text().splitlines():
                if Path("/proc/" + line).exists():
                    raise RuntimeError("Stop cluster before deploying rebuilt backend")
    RUNTIME.mkdir(parents=True, exist_ok=True)
    source = RUNTIME / "build/native-source"
    source.mkdir(parents=True, exist_ok=True)
    shutil.copytree(REPO / "src/main/native", source, dirs_exist_ok=True)
    builddir = RUNTIME / "build/native"
    run(["cmake", "-S", source, "-B", builddir, "-DCMAKE_BUILD_TYPE=Release", "-DFORL0_FORCE_MINI_GTEST=ON"])
    run(["cmake", "--build", builddir, "-j", "8"])
    run(["ctest", "--test-dir", builddir, "--output-on-failure"])
    (RUNTIME / "native").mkdir(exist_ok=True)
    shutil.copy2(builddir / "libforl0_engine.so", RUNTIME / "native")
    run(["mvn", "-B", "-ntp", "-Dforl0.native.library.path=" + str(RUNTIME / "native"),
         "-Dtest=ForL0ConfigurationTest,ForL0StateSemanticsTest", "package"], cwd=REPO)
    backend = REPO / "target/flink-statebackend-forL0-1.0-SNAPSHOT.jar"
    jarstage = RUNTIME / "build/jar/native"
    jarstage.mkdir(parents=True, exist_ok=True)
    shutil.copy2(RUNTIME / "native/libforl0_engine.so", jarstage)
    run(["jar", "uf", backend, "-C", jarstage.parent, "native/libforl0_engine.so"])
    run(["mvn", "-B", "-ntp", "install:install-file", "-Dfile=" + str(backend),
         "-DpomFile=" + str(REPO / "pom.xml")], cwd=REPO)
    run(["mvn", "-B", "-ntp", "-f", REPO / "benchmark/wordcount/pom.xml", "package"], cwd=REPO)
    run(["mvn", "-B", "-ntp", "-f", REPO / "benchmark/benchset/pom.xml", "package"], cwd=REPO)
    shutil.copy2(backend, FLINK / "lib" / backend.name)
    manifest = {}
    for path in (backend, RUNTIME / "native/libforl0_engine.so",
                 REPO / "benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar",
                 REPO / "benchmark/benchset/target/benchset-1.0-SNAPSHOT.jar"):
        manifest[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    (RUNTIME / "build/artifacts.sha256.json").write_text(json.dumps(manifest, indent=2) + "\n")

def start():
    configure()
    if not (FLINK / "lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar").exists():
        raise RuntimeError("Run build before start")
    for role, (cpus, script) in ROLES.items():
        live = [pid for f in (RUNTIME / role / "pid").glob("*.pid")
                for pid in f.read_text().splitlines() if Path("/proc/" + pid).exists()]
        if live:
            print("Already running", role, live, flush=True)
            continue
        print("+ start", role, "CPU", cpus, flush=True)
        subprocess.run(["taskset", "-c", cpus, str(FLINK / "bin" / script), "start"],
                       check=True, env=role_env(role), stdin=subprocess.DEVNULL)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            overview = api("/overview")
            if overview["taskmanagers"] == 2 and overview["slots-total"] == 8:
                print(json.dumps(overview, indent=2))
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Cluster did not reach 2 TM / 8 slots; inspect " + str(RUNTIME))

def stop():
    for role in ("tm2", "tm1", "jm"):
        if (RUNTIME / role / "conf").exists():
            subprocess.run([str(FLINK / "bin" / ROLES[role][1]), "stop"],
                           check=True, env=role_env(role), stdin=subprocess.DEVNULL)

def status():
    print(json.dumps(api("/overview"), indent=2))
    print(json.dumps(api("/taskmanagers"), indent=2))
    for role in ROLES:
        for pidfile in (RUNTIME / role / "pid").glob("*.pid"):
            for pid in pidfile.read_text().splitlines():
                proc = Path("/proc") / pid
                if proc.exists():
                    print(role, "pid=" + pid)
                    print("\n".join(x for x in (proc / "status").read_text().splitlines()
                                    if x.startswith(("Cpus_allowed_list:", "VmRSS:"))))
                    print((proc / "cmdline").read_bytes().replace(b"\0", b" ").decode())

def wordcount(args):
    overview = api("/overview")
    assert overview["taskmanagers"] == 2 and overview["slots-total"] == 8, overview
    pids = {role: int(next((RUNTIME / role / "pid").glob("*.pid")).read_text().strip())
            for role in ("tm1", "tm2")}
    def process_stats(pid):
        fields = Path("/proc", str(pid), "stat").read_text().split()
        rss = int(next(x.split()[1] for x in Path("/proc", str(pid), "status").read_text().splitlines() if x.startswith("VmRSS:"))) * 1024
        return {"cpu_seconds": (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK"), "rss_bytes": rss}
    artifacts = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in
                 (RUNTIME / "native/libforl0_engine.so",
                  FLINK / "lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar",
                  REPO / "benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar")}
    before = {role: process_stats(pid) for role, pid in pids.items()}
    peak = {role: before[role]["rss_bytes"] for role in pids}
    stamp = time.strftime("%Y%m%d_%H%M%S")
    resultdir = REPO / "benchmark/results/runs" / ("briskstate_" + stamp + "_" + args.backend + "_" + args.key_type)
    resultdir.mkdir(parents=True, exist_ok=False)
    (resultdir / "cluster-before.json").write_text(json.dumps(overview, indent=2))
    factory = "hashmap" if args.backend == "heap" else "org.apache.flink.state.forl0.ForL0StateBackendFactory"
    command = [str(FLINK / "bin/flink"), "run", "-d", "-m", "127.0.0.1:8081",
               "-Dstate.backend.type=" + factory,
               "-Dstate.backend.forl0.l0-cache.enabled=false",
               "-Drestart-strategy.type=none",
               "-p", "8", "-c", "org.apache.flink.benchmark.wordcount.WordCountBenchmark",
               str(REPO / "benchmark/wordcount/target/wordcount-benchmark-1.0-SNAPSHOT.jar"),
               "--keyType", args.key_type,
               "--workloadMode", "stateful_counter", "--numKeys", str(args.keys),
               "--numRecords", str(args.records), "--parallelism", "8",
               "--stateOpsPerRecord", str(args.state_ops), "--skewFactor", str(args.skew), "--arrivalRate", str(args.rate),
               "--checkpointInterval", str(args.checkpoint_interval),
               "--verifyCounts", str(args.verify_counts).lower(),
               "--output", str(resultdir / "metrics.json")]
    (resultdir / "command.json").write_text(json.dumps(command, indent=2))
    process = subprocess.run(command, check=True, env=role_env("jm"), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=120)
    (resultdir / "submission.log").write_text(process.stdout)
    print(process.stdout, flush=True)
    jobid = re.search(r"Job has been submitted with JobID ([0-9a-f]{32})", process.stdout)
    if not jobid:
        raise RuntimeError("Missing job ID")
    jobid = jobid[1]
    profiler = Path("/root/opt/async-profiler-4.4-linux-arm64/bin/asprof")
    profile_event = None
    profile_started = False
    profile_at = 0.0
    def finish_profile():
        nonlocal profile_event
        if profile_event:
            for role, pid in pids.items():
                subprocess.run([str(profiler), "dump", "-o", "flamegraph", "-f", str(resultdir / (role + "-" + profile_event + ".html")), str(pid)], check=True, stdout=subprocess.DEVNULL)
                subprocess.run([str(profiler), "stop", "-o", "collapsed", "-f", str(resultdir / (role + "-" + profile_event + ".collapsed")), str(pid)], check=True, stdout=subprocess.DEVNULL)
            profile_event = None
    def start_profile(event):
        nonlocal profile_event, profile_at
        for role, pid in pids.items():
            subprocess.run([str(profiler), "start", "-e", event, "-i", "5ms" if event == "cpu" else "2m", "--cstack", "dwarf", str(pid)], check=True, stdout=subprocess.DEVNULL)
        profile_event, profile_at = event, time.monotonic()
    submitted_at = time.monotonic()
    deadline = submitted_at + 1800
    try:
        while time.monotonic() < deadline:
            job = api("/jobs/" + jobid)
            for role, pid in pids.items():
                peak[role] = max(peak[role], process_stats(pid)["rss_bytes"])
            if args.profile and not profile_started and time.monotonic() - submitted_at > 8 and job["state"] == "RUNNING":
                start_profile("cpu")
                profile_started = True
            elif profile_event == "cpu" and time.monotonic() - profile_at >= 30:
                finish_profile()
                start_profile("alloc")
            elif profile_event == "alloc" and time.monotonic() - profile_at >= 20:
                finish_profile()
            if job["state"] in ("FINISHED", "FAILED", "CANCELED"):
                break
            time.sleep(2)
        else:
            raise RuntimeError("Job timeout; job remains available for inspection: " + jobid)
    finally:
        finish_profile()
    after = {role: process_stats(pid) for role, pid in pids.items()}
    measurement = {"variant": args.variant, "profiled": args.profile, "artifacts_sha256": artifacts,
                   "keys": args.keys, "keys_covered": min(args.keys, args.records), "records": args.records, "state_ops_per_record": args.state_ops,
                   "skew": args.skew, "rate": args.rate,
                   "job_duration_seconds": job["duration"] / 1000,
                   "state_updates_per_second": args.records * args.state_ops / (job["duration"] / 1000),
                   "cpu_seconds": {r: after[r]["cpu_seconds"] - before[r]["cpu_seconds"] for r in pids},
                   "peak_rss_bytes": peak}
    (resultdir / "measurement.json").write_text(json.dumps(measurement, indent=2) + "\n")
    for name, endpoint in [("job", ""), ("checkpoints", "/checkpoints"), ("exceptions", "/exceptions"), ("config", "/config")]:
        (resultdir / (name + ".json")).write_text(json.dumps(api("/jobs/" + jobid + endpoint), indent=2))
    assert job["state"] == "FINISHED", "Job " + jobid + ": " + job["state"] + "; see " + str(resultdir)
    metrics = json.loads((resultdir / "metrics.json").read_text())
    assert metrics["sink_records"] == args.records == metrics["total_records"], metrics
    assert metrics["backend"] == "configured-via-flink" and metrics["parallelism"] == 8, metrics
    if args.verify_counts:
        assert metrics["counts_verified"] and metrics["verified_distinct_keys"] == min(args.keys, args.records), metrics
    if args.checkpoint_interval:
        checkpoints = json.loads((resultdir / "checkpoints.json").read_text())
        assert checkpoints["counts"]["completed"] > 0, checkpoints
    print("PASS", args.backend, args.key_type, "job=" + jobid, "records=" + str(metrics["sink_records"]), str(resultdir), flush=True)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["configure", "build", "start", "stop", "status", "wordcount"])
    parser.add_argument("--backend", choices=["heap", "forl0"], default="heap")
    parser.add_argument("--key-type", choices=["long"], default="long")
    parser.add_argument("--records", type=int, default=128000000)
    parser.add_argument("--keys", type=int, default=32000000)
    parser.add_argument("--rate", type=int, default=0)
    parser.add_argument("--verify-counts", action="store_true", help="Independent per-key count oracle; use small key spaces, not timing runs")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--variant", default="standard-api-v1")
    parser.add_argument("--state-ops", type=int, default=16)
    parser.add_argument("--skew", type=float, default=0.0)
    parser.add_argument("--checkpoint-interval", type=int, default=0)
    args = parser.parse_args()
    if args.action == "wordcount":
        wordcount(args)
    else:
        globals()[args.action]()

if __name__ == "__main__":
    main()
