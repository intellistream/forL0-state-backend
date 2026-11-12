#!/usr/bin/env python3
"""Offline benchmark with six fixed L0 tiers; loopback networking only."""
import argparse
from collections import Counter
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
import traceback
import urllib.request
import uuid

WORKLOADS = ['wc', 'fd', 'sd', 'tm', 'lg', 'vs', 'lr']
ROLES = dict(wc=1, fd=1, sd=2, tm=2, lg=4, vs=9, lr=8)
MULTIPLIERS = dict(wc=1, fd=1, sd=1, tm=1, lg=3, vs=8, lr=12)
L0_SIZES = {'brisk_l0_' + str(mib): mib * 1024 ** 2 for mib in [2, 4, 8, 16, 24, 32]}
ARMS = ['heap', 'brisk_off', *L0_SIZES]
HZ = os.sysconf('SC_CLK_TCK')


def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temp.replace(path)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def signed(value):
    return (int(value) + (1 << 63)) % (1 << 64) - (1 << 63)


def arm_name(arm):
    if arm == 'heap':
        return 'Heap'
    if arm == 'brisk_off':
        return 'BriskState 无L0'
    return 'BriskState L0共享%g MiB' % (L0_SIZES[arm] / 1024 ** 2)


def cpulist(text):
    result = set()
    for chunk in text.strip().split(','):
        if not chunk:
            continue
        lo, _, hi = chunk.partition('-')
        result.update(range(int(lo), int(hi or lo) + 1))
    return result


def cpu_groups():
    allowed = os.sched_getaffinity(0)
    for node in sorted(Path('/sys/devices/system/node').glob('node*/cpulist')):
        candidates = sorted(cpulist(node.read_text()) & allowed)
        if len(candidates) >= 12:
            return dict(zip(['jm', 'tm1', 'tm2'], [candidates[i:i + 4] for i in (0, 4, 8)]))
    raise RuntimeError('需要同一NUMA节点内至少12个允许使用的CPU：JM4核、两个TM各4核，共享同一L0硬件域。')


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def process_stats(pid, expected):
    root = Path('/proc') / str(pid)
    fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
    memory = {}
    smaps = root / 'smaps_rollup'
    if smaps.exists():
        for line in smaps.read_text().splitlines():
            parts = line.split()
            if parts[0] in ('Rss:', 'Pss:', 'Private_Dirty:'):
                memory[parts[0][:-1].lower() + '_bytes'] = int(parts[1]) * 1024
    masks = {}
    for task in (root / 'task').iterdir():
        try:
            mask = next(x.split(':', 1)[1].strip() for x in (task / 'status').read_text().splitlines()
                        if x.startswith('Cpus_allowed_list:'))
            if not cpulist(mask) <= set(expected):
                raise RuntimeError('线程越过4核CPU绑定：pid=%s tid=%s mask=%s' % (pid, task.name, mask))
            masks[mask] = masks.get(mask, 0) + 1
        except FileNotFoundError:
            pass
    return dict(pid=pid, cpu_seconds=(int(fields[11]) + int(fields[12])) / HZ,
                memory=memory, thread_masks=masks)


class Campaign:
    def __init__(self, args):
        self.a = args
        self.runtime = Path(args.runtime).resolve()
        self.oracle = Path(__file__).resolve().parents[1] / 'reproduction/oracle'
        self.result_file = Path(args.results).resolve()
        self.run_dir = Path(args.work_dir).resolve()
        self.output = self.run_dir / 'evidence'
        self.output.mkdir(parents=True, exist_ok=False)
        self.work = self.runtime.parent / ('work-' + uuid.uuid4().hex[:12])
        self.work.mkdir()
        self.cpus = cpu_groups()
        spare = sorted(os.sched_getaffinity(0) - {c for group in self.cpus.values() for c in group})
        self.controller_cpus = spare[:4] if spare else self.cpus['jm']
        os.sched_setaffinity(0, self.controller_cpus)
        self.processes = {}
        self.rows = []
        self.failures = []
        self.port = free_port()
        self.rpc = free_port()
        self.env = dict(os.environ, JAVA_HOME=str(self.runtime / 'jdk'),
                        FLINK_HOME=str(self.runtime / 'flink'),
                        OMP_NUM_THREADS='4', MALLOC_CONF='narenas:4',
                        FORL0_L0_MEMORY_MODE='dram' if args.validate_dram else 'hardware',
                        FORL0_L0_ROW_POLICY='resident', FORL0_L0_SCALAR_CACHE='off')
        self.env['PATH'] = str(self.runtime / 'jdk/bin') + ':' + os.environ.get('PATH', '/usr/bin:/bin')
        self.hashes = {name: sha(self.runtime / name) for name in
                       ['job/paper-steady-benchset.jar', 'native/libforl0_engine.so',
                        'flink/lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar']}
        self.hashes['oracle_class_sha256'] = sha(self.oracle / 'PaperSourceOracle.class')
        self.label = 'DRAM_VALIDATION_ONLY' if args.validate_dram else 'REAL_L0_HARDWARE'
        self.l0_sizes = L0_SIZES.copy()
        self.preflight = {}
        self.plan = []
        self.state = 'preparing'
        self.started_ms = int(time.time() * 1000)
        self.finished_ms = None
        self.environment_info = {}
        self.current = None
        self.progress = {}
        self.last_heartbeat = 0
        self.error = None

    def phase(self, stage, remaining=None):
        self.progress = dict(stage=stage, completed=len(self.rows), total=len(self.plan),
                             current=self.current, updated_ms=int(time.time() * 1000),
                             remaining_seconds=None if remaining is None else max(0, int(remaining)))
        counts = Counter(row['status'] for row in self.rows)
        lines = ['状态：' + stage,
                 '进度：已处理%d / %d；成功%d，失败%d，跳过%d，中止%d。' %
                 (len(self.rows), len(self.plan), counts['passed'], counts['failed'], counts['skipped'], counts['interrupted'])]
        if self.current:
            lines.append('当前：%s / %s / 第%d次。' %
                         (self.current['workload'].upper(), arm_name(self.current['arm']), self.current['repeat']))
        if remaining is not None:
            lines.append('当前阶段剩余：约%d秒。' % max(0, int(remaining)))
        if self.error:
            lines.append('原因：' + self.error['message'])
        lines.append('更新时间：' + time.strftime('%Y-%m-%d %H:%M:%S %z'))
        temp = self.run_dir / 'status.txt.tmp'
        temp.write_text('\n'.join(lines) + '\n')
        temp.replace(self.run_dir / 'status.txt')
        dump(self.run_dir / 'progress.json', self.progress)
        self.last_heartbeat = time.monotonic()
        print('；'.join(lines[:3]), flush=True)

    def heartbeat(self, stage, remaining=None):
        if time.monotonic() - self.last_heartbeat >= 5:
            self.phase(stage, remaining)

    def api(self, path):
        # Explicitly ignore proxy variables. Only this private loopback JM is used.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open('http://127.0.0.1:%d%s' % (self.port, path), timeout=15) as response:
            return json.load(response)

    def environment(self):
        out = self.output / 'environment'
        out.mkdir()
        for name, path in [('os-release', '/etc/os-release'), ('cpuinfo', '/proc/cpuinfo'),
                           ('meminfo', '/proc/meminfo'), ('cgroup', '/proc/self/cgroup'),
                           ('mountinfo', '/proc/self/mountinfo')]:
            (out / (name + '.txt')).write_text(Path(path).read_text())
        for p in Path('/sys/devices/system/node').glob('node*/cpulist'):
            (out / (p.parent.name + '-cpulist.txt')).write_text(p.read_text())
        r = subprocess.run([str(self.runtime / 'jdk/bin/java'), '-version'],
                           env=self.env, capture_output=True, text=True, check=True)
        (out / 'java-version.txt').write_text(r.stdout + r.stderr)
        shutil.copy2(Path(__file__).resolve(), out / 'collector.py')
        shutil.copy2(Path(__file__).resolve().parent / 'l0_detector.sh', out / 'l0_detector.sh')
        info = dict(mode=self.label, uname=list(os.uname()), cpu_groups=self.cpus, controller_cpus=self.controller_cpus,
                    collector_sha256=sha(out / 'collector.py'), detector_sha256=sha(out / 'l0_detector.sh'),
                    command=sys.argv, artifacts=self.hashes, runtime=str(self.runtime),
                    protocol=dict(keys=self.a.keys, feature_width=64, touches=128, window=16,
                                  parallelism=8, tm_cpu_cores=4, tm_memory_gib=32, jm_memory_gib=8,
                                  warmup_seconds=self.a.warmup, measure_seconds=self.a.measure,
                                  repeats=self.a.repeats, distribution='uniform', checkpoint=False,
                                  l0_shared_sizes_mib=[2, 4, 8, 16, 24, 32],
                                  capacity_source='fixed tiers from historical sweep; driver metadata informational only',
                                  l0_row_policy='resident'))
        dump(out / 'environment.json', info)
        info['system'] = dict(os_release=Path('/etc/os-release').read_text(),
                              cpuinfo_first_processor=Path('/proc/cpuinfo').read_text().split('\n\n')[0],
                              meminfo=Path('/proc/meminfo').read_text(),
                              numa_cpu_lists={path.parent.name: path.read_text().strip()
                                              for path in Path('/sys/devices/system/node').glob('node*/cpulist')})
        self.environment_info = info
        mem = {x.split(':')[0]: int(x.split()[1]) * 1024 for x in Path('/proc/meminfo').read_text().splitlines()
               if x.startswith(('MemTotal:', 'MemAvailable:'))}
        if mem['MemAvailable'] < 76 * 1024 ** 3:
            raise RuntimeError('至少需要约76GiB可用内存（JM8GiB＋两个TM各32GiB＋系统余量）。')
        if shutil.disk_usage(self.work).free < 2 * 1024 ** 3:
            raise RuntimeError('运行目录至少需要2GiB空闲磁盘。')

    def preflight_l0(self):
        folder = self.output / 'l0-preflight'
        folder.mkdir()
        selected = [arm for arm in self.l0_sizes if arm in self.a.arms]
        if not selected:
            return
        capacity = self.read_l0_capacity(folder)
        dump(folder / 'capacity.json', dict(mode=self.label, reported_capacity=capacity,
             policy='fixed tiers; each tier independently verified; no maximum-capacity search',
             sizes_total_bytes=self.l0_sizes))
        detection_error = None
        try:
            if not self.a.validate_dram:
                script = Path(__file__).resolve().parent / 'l0_detector.sh'
                command = ['bash', '-c', 'source "$1"; forl0_detect_l0_environment; '
                           'forl0_print_l0_detection; test -n "$FORL0_L0_DEVICE_PATH" && '
                           'test -n "$FORL0_L0_LIBRARY_PATH"; printf "\\nLIBRARY=%s\\n" "$FORL0_L0_LIBRARY_PATH"',
                           'l0-detect', str(script)]
                r = subprocess.run(command, capture_output=True, text=True, env=self.env, timeout=120)
                (folder / 'detection.log').write_text(r.stdout + r.stderr)
                if r.returncode or not re.search(r'^LIBRARY=.+', r.stdout, re.M):
                    raise RuntimeError('找不到已安装的L0设备/SDK；见 l0-preflight/detection.log。')
                library = re.search(r'^LIBRARY=(.+)', r.stdout, re.M).group(1)
                self.env['FORL0_L0_LIBRARY_PATH'] = library
                self.env['BRISKSTATE_L0_LIBRARY_DIRS'] = str(Path(library).parent)
                dump(folder / 'sdk.json', dict(library=library, sha256=sha(library)))
        except Exception:
            detection_error = traceback.format_exc()
            dump(folder / 'detection-failure.json', dict(error=detection_error))
        for arm in selected:
            total = self.l0_sizes[arm]
            trial = folder / arm
            record = dict(mode=self.label, arm=arm, requested_total_bytes=total,
                          requested_per_tm_bytes=total // 2, status='failed')
            self.phase('验证L0共享%g MiB：两个TM各分配%g MiB' % (total / 1024 ** 2, total / 2 / 1024 ** 2))
            try:
                if detection_error:
                    raise RuntimeError(detection_error)
                if self.a.validate_dram and total > self.a.validation_capacity_mib * 1024 ** 2:
                    raise RuntimeError('仅DRAM验收：固定档位超过显式设置的模拟可用容量 %g MiB。'
                                       % self.a.validation_capacity_mib)
                proof = self.probe_pair(trial, total // 2)
                record.update(status='passed', roles=proof)
                print('容量验证通过：' + arm_name(arm), flush=True)
            except Exception:
                record['error'] = traceback.format_exc()
                record['message'] = str(sys.exc_info()[1]).strip().splitlines()[-1]
                dump(trial / 'failure.json', record)
                print('容量验证未通过：%s。原因：%s。其他档位和基线继续。' %
                      (arm_name(arm), record['message']), flush=True)
            self.preflight[arm] = record
            dump(folder / 'tiers.json', self.preflight)

    def read_l0_capacity(self, folder):
        if self.a.validate_dram:
            return dict(total_bytes=int(self.a.validation_capacity_mib * 1024 ** 2),
                        source='explicit DRAM validation fixture', kind='not_hardware')
        evidence = {}
        # Documented driver parameter: maximum MB for each NUMA node. Both TMs
        # are bound within the same NUMA node, so never sum all machine nodes.
        candidates = [Path('/sys/module/hisi_l0_mem_pool/parameters/max_numa_capacity'),
                      Path('/sys/module/hisi_l0/parameters/max_numa_capacity')]
        for p in candidates:
            if p.is_file():
                try:
                    text = p.read_text().strip()
                except OSError as error:
                    evidence[str(p)] = str(error)
                    continue
                evidence[str(p)] = text
                if re.fullmatch(r'[0-9]+', text) and int(text) > 0:
                    dump(folder / 'driver-capacity-evidence.json', evidence)
                    return dict(total_bytes=int(text) * 1024 ** 2, source=str(p),
                                kind='driver configured per-NUMA maximum; not physical chip capacity')
        # Newer drivers may expose byte counts directly under their pool tree.
        candidates = [Path('/sys/kernel/l0_pool/total_capacity_bytes'),
                      Path('/sys/kernel/l0_pool/capacity_bytes')]
        for p in candidates:
            if p.is_file():
                try:
                    text = p.read_text().strip()
                except OSError as error:
                    evidence[str(p)] = str(error)
                    continue
                evidence[str(p)] = text
                if re.fullmatch(r'[0-9]+', text) and int(text) > 0:
                    dump(folder / 'driver-capacity-evidence.json', evidence)
                    return dict(total_bytes=int(text), source=str(p), kind='driver reported shared capacity')
        for base in ['/sys/kernel/l0_pool', '/sys/module/hisi_l0_mem_pool/parameters', '/sys/module/hisi_l0/parameters']:
            p = Path(base)
            if p.is_dir():
                try:
                    evidence[base + '/entries'] = [x.name for x in p.iterdir()]
                except OSError as error:
                    evidence[base + '/entries'] = str(error)
        dump(folder / 'driver-capacity-evidence.json', evidence)
        return dict(total_bytes=None, source=None, kind='metadata unavailable; fixed tiers still verified')

    def probe_pair(self, folder, quota):
        folder.mkdir(parents=True, exist_ok=True)
        probes = []
        env = dict(self.env, FORL0_L0_PROBE_BYTES=str(quota))
        try:
            for role in ['tm1', 'tm2']:
                log = (folder / (role + '.log')).open('w')
                proc = subprocess.Popen([str(self.runtime / 'bin/l0-probe'), str(folder), role],
                                        env=env, stdout=log, stderr=subprocess.STDOUT,
                                        start_new_session=True,
                                        preexec_fn=lambda r=role: os.sched_setaffinity(0, self.cpus[r]))
                probes.append((role, proc, log))
            deadline = time.monotonic() + 100
            while time.monotonic() < deadline:
                if all((folder / ('ready-' + role)).exists() for role, _, _ in probes):
                    break
                if any(proc.poll() is not None for _, proc, _ in probes):
                    raise RuntimeError('L0双进程全容量读写探测失败；见 l0-preflight/*m*.log。')
                time.sleep(.2)
            else:
                raise TimeoutError('L0双进程全容量探测超时。')
            (folder / 'release').write_text('release\n')
            expected = 'dram_emulation' if self.a.validate_dram else 'hardware'
            proof = {}
            for role, proc, log in probes:
                if proc.wait(timeout=30):
                    raise RuntimeError('L0 probe failed: ' + role)
                log.close()
                text = (folder / (role + '.log')).read_text()
                record = json.loads(next(x for x in text.splitlines() if x.startswith('{')))
                if record['memory_mode'] != expected or record['touched_bytes'] < quota - 4096:
                    raise RuntimeError('L0内存类型/容量不符合预期：' + repr(record))
                proof[role] = record
            dump(folder / 'proof.json', dict(mode=self.label, concurrent_allocations=True, roles=proof))
            return proof
        finally:
            for _, proc, log in probes:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=10)
                log.close()

    def role_env(self, role):
        env = self.env.copy()
        root = self.work / role
        env.update(FLINK_CONF_DIR=str(root / 'conf'), FLINK_LOG_DIR=str(root / 'log'),
                   FLINK_PID_DIR=str(root / 'pid'), FLINK_IDENT_STRING='offline-' + role)
        return env

    def configure(self, workload, arm, folder):
        engines = 4 * ROLES[workload] + (1 if workload == 'vs' else 0)
        l0_total = self.l0_sizes.get(arm, 0)
        self.env['FORL0_L0_ROW_ENGINE_BYTES'] = str(l0_total // 2 // engines)
        (folder / 'cache-telemetry').mkdir(exist_ok=True)
        self.env['FORL0_L0_STATS_DIRECTORY'] = str(folder / 'cache-telemetry')
        for role in ['jm', 'tm1', 'tm2']:
            root = self.work / role
            if root.exists():
                shutil.rmtree(root)
            for name in ['conf', 'log', 'pid', 'tmp']:
                (root / name).mkdir(parents=True)
            for p in (self.runtime / 'flink/conf').iterdir():
                if p.is_file() and p.name not in ['config.yaml', 'flink-conf.yaml']:
                    shutil.copy2(p, root / 'conf')
            config = {
                'env.java.home': str(self.runtime / 'jdk'),
                'env.java.opts.all': '-XX:ActiveProcessorCount=4 -XX:+UseG1GC '
                    '-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer '
                    '-Djava.library.path=' + str(self.runtime / 'native') + ' '
                    '-Xlog:gc*,safepoint:file=' + str(root / 'log/gc-%p.log') + ':time,uptime,level,tags',
                'jobmanager.rpc.address': '127.0.0.1', 'jobmanager.bind-host': '127.0.0.1',
                'jobmanager.rpc.port': self.rpc, 'jobmanager.memory.process.size': '8g',
                'taskmanager.host': '127.0.0.1', 'taskmanager.bind-host': '127.0.0.1',
                'taskmanager.rpc.port': 0, 'taskmanager.data.port': 0,
                'taskmanager.resource-id': 'offline-' + role,
                'taskmanager.cpu.cores': 4.0, 'taskmanager.numberOfTaskSlots': 4,
                'taskmanager.memory.process.size': '32g',
                'taskmanager.memory.task.off-heap.size': '0g' if arm == 'heap' else '8g',
                'taskmanager.memory.managed.size': '0m',
                'taskmanager.memory.network.min': '512m', 'taskmanager.memory.network.max': '512m',
                'parallelism.default': 8, 'rest.address': '127.0.0.1',
                'rest.bind-address': '127.0.0.1', 'rest.port': self.port,
                'rest.bind-port': str(self.port), 'state.backend.type': 'hashmap',
                'state.backend.forl0.l0-cache.enabled': arm in self.l0_sizes,
                'state.backend.forl0.l0-cache.total-size': str(l0_total or 2 * 1024 ** 2) + 'b',
                'state.backend.forl0.l0-cache.expected-engines': 2,
                'state.backend.forl0.l0-cache.strict-allocation': True,
                'state.backend.forl0.l0-cache.state-size': '1mb',
                'state.backend.forl0.native-memory.max-size': '1g',
                'restart-strategy.type': 'none', 'state.checkpoint-storage': 'filesystem',
                'state.checkpoints.dir': (self.work / 'checkpoints').as_uri(),
                'io.tmp.dirs': str(root / 'tmp')}
            (root / 'conf/config.yaml').write_text(''.join(k + ': ' + json.dumps(v) + '\n' for k, v in config.items()))
            shutil.copy2(root / 'conf/config.yaml', folder / (role + '-config.yaml'))

    def start(self):
        for role in ['jm', 'tm1', 'tm2']:
            script = 'jobmanager.sh' if role == 'jm' else 'taskmanager.sh'
            log = (self.work / role / 'log/console.log').open('w')
            native_log = (self.work / role / 'log/stderr.log').open('w')
            proc = subprocess.Popen([str(self.runtime / 'flink/bin' / script), 'start-foreground'],
                                    env=self.role_env(role), stdin=subprocess.DEVNULL,
                                    stdout=log, stderr=native_log, start_new_session=True,
                                    preexec_fn=lambda r=role: os.sched_setaffinity(0, self.cpus[r]))
            log.close()
            native_log.close()
            self.processes[role] = proc
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p in self.processes.values()):
                raise RuntimeError('Flink进程启动失败；见该运行目录log。')
            try:
                overview = self.api('/overview')
                if overview['taskmanagers'] == 2 and overview['slots-total'] == 8:
                    return
            except (OSError, ValueError):
                pass
            time.sleep(1)
        raise TimeoutError('Flink集群未达到两台TM、8slots。')

    def stop(self, folder=None):
        for role in ['tm2', 'tm1', 'jm']:
            proc = self.processes.get(role)
            if proc and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
        for role, proc in self.processes.items():
            try:
                proc.wait(timeout=25)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=10)
        self.processes.clear()
        if folder:
            for role in ['jm', 'tm1', 'tm2']:
                source = self.work / role / 'log'
                if source.exists():
                    shutil.copytree(source, folder / (role + '-log'), dirs_exist_ok=True)

    def snapshot(self):
        return dict(time_ms=int(time.time() * 1000),
                    roles={r: process_stats(p.pid, self.cpus[r]) for r, p in self.processes.items()})

    def check_job(self, jid, allowed=('RUNNING',)):
        job = self.api('/jobs/' + jid)
        if job['state'] not in allowed:
            raise RuntimeError('Job状态异常：' + job['state'])
        return job

    def l0_proof(self, arm, folder):
        expected = 'dram_emulation' if self.a.validate_dram else 'hardware'
        proof = {}
        for role in ['tm1', 'tm2']:
            proc = self.processes[role]
            maps = (Path('/proc') / str(proc.pid) / 'maps').read_text()
            paths = sorted({x.split()[-1] for x in maps.splitlines() if 'libforl0_engine.so' in x})
            text = '\n'.join(p.read_text(errors='replace') for p in (self.work / role / 'log').iterdir() if p.is_file())
            starts = [x for x in text.splitlines() if '[ForL0-HotCache]' in x and 'active=1' in x]
            telemetry = None
            if arm == 'heap' and paths:
                raise RuntimeError('Heap组意外加载ForL0 native库。')
            if arm != 'heap':
                if paths != [str(self.runtime / 'native/libforl0_engine.so')]:
                    raise RuntimeError('加载了非离线包的native库：' + repr(paths))
                if arm in self.l0_sizes:
                    records = []
                    stats = folder / 'cache-telemetry' / ('l0-%d.jsonl' % proc.pid)
                    if stats.exists():
                        for line in stats.read_text().splitlines():
                            if line.endswith('}'):
                                records.append(json.loads(line))
                    quota = self.l0_sizes[arm] // 2
                    telemetry = records[-1] if records else None
                    if not telemetry or telemetry['mode'] != expected or not quota - 256 <= telemetry['capacity_bytes'] <= quota:
                        raise RuntimeError('L0模式或实际容量未通过结构化统计验证：' + role)
                    if telemetry['row_caches'] <= 0 or telemetry['row_cache_bytes'] <= 0:
                        raise RuntimeError('L0虽分配成功，但未建立状态行驻留池：' + role)
                elif starts or 'L0 cache disabled by config.' not in text:
                    raise RuntimeError('L0off组配置证明不符合预期。')
            proof[role] = dict(native_mappings=paths, startup_lines=starts,
                               manager_telemetry=telemetry,
                               jvm_command=(Path('/proc') / str(proc.pid) / 'cmdline').read_bytes().replace(b'\0', b' ').decode(),
                               cpu_affinity=self.cpus[role], snapshot=process_stats(proc.pid, self.cpus[role]))
        dump(folder / 'l0-proof.json', dict(mode=self.label, arm=arm, roles=proof))

    def run_one(self, workload, arm, repeat):
        folder = self.output / 'raw' / ('%s-%s-r%d' % (workload, arm, repeat))
        folder.mkdir(parents=True)
        (folder / 'control').mkdir()
        self.phase('开始本轮测试')
        row = dict(workload=workload, arm=arm, repeat=repeat, status='failed', mode=self.label,
                   l0_requested_total_bytes=self.l0_sizes.get(arm, 0))
        if arm in self.preflight and self.preflight[arm]['status'] != 'passed':
            row.update(status='skipped', error=self.preflight[arm]['error'],
                       message=self.preflight[arm]['message'],
                       reason='L0 preflight failed; no benchmark launched for this tier')
            dump(folder / 'failure.json', row)
            self.failures.append(row)
            self.rows.append(row)
            self.write_results()
            print('已跳过：%s / %s。原因：%s。' % (workload.upper(), arm_name(arm), row['message']), flush=True)
            return
        try:
            self.configure(workload, arm, folder)
            self.phase('正在启动JM和两个TM')
            self.start()
            backend = 'hashmap' if arm == 'heap' else 'org.apache.flink.state.forl0.ForL0StateBackendFactory'
            cmd = [str(self.runtime / 'flink/bin/flink'), 'run', '-d', '-m', '127.0.0.1:%d' % self.port,
                   '-Dstate.backend.type=' + backend,
                   '-Dstate.backend.forl0.l0-cache.enabled=' + str(arm in self.l0_sizes).lower(),
                   '-Drestart-strategy.type=none', '-p', '8', '-c',
                   'org.apache.flink.benchmark.benchset.BenchsetMain',
                   str(self.runtime / 'job/paper-steady-benchset.jar'), '--benchmark', workload,
                   '--numKeys', str(self.a.keys), '--numRecords', str(self.a.keys),
                   '--featureWidth', '64', '--touches', '128', '--window', '16',
                   '--parallelism', '8', '--checkpointInterval', '0',
                   '--outputDirectory', str(folder), '--controlDirectory', str(folder / 'control')]
            dump(folder / 'command.json', cmd)
            begin = time.monotonic()
            self.phase('正在提交作业')
            r = subprocess.run(cmd, env=self.role_env('jm'), capture_output=True, text=True, timeout=180)
            (folder / 'submission.log').write_text(r.stdout + r.stderr)
            match = re.search(r'Job has been submitted with JobID ([0-9a-f]{32})', r.stdout)
            if r.returncode or not match:
                raise RuntimeError('作业提交失败；见submission.log。')
            jid = match.group(1)
            self.phase('初始化状态：覆盖全部实体key')
            samples = []
            with (folder / 'process-samples.jsonl').open('w') as sample_file:
                def sample():
                    item = self.snapshot()
                    samples.append(item)
                    sample_file.write(json.dumps(item) + '\n')
                    sample_file.flush()
                deadline = time.monotonic() + 3600
                while time.monotonic() < deadline:
                    job = self.check_job(jid)
                    received = sum(v.get('metrics', {}).get('read-records', 0) for v in job['vertices']
                                   if v['name'].startswith('Sink:'))
                    ready = len(list((folder / 'control').glob('ready-*'))) == 8
                    if ready and received == self.a.keys * MULTIPLIERS[workload]:
                        break
                    sample()
                    self.heartbeat('初始化状态：覆盖全部实体key')
                    time.sleep(1)
                else:
                    raise TimeoutError('全key覆盖初始化超时。')
                coverage_seconds = time.monotonic() - begin
                dump(folder / 'coverage-job.json', job)
                self.l0_proof(arm, folder)
                (folder / 'control/release').write_text('release\n')
                deadline = time.monotonic() + self.a.warmup
                self.phase('预热', self.a.warmup)
                while time.monotonic() < deadline:
                    self.check_job(jid)
                    sample()
                    self.heartbeat('预热', deadline - time.monotonic())
                    time.sleep(min(1, max(0, deadline - time.monotonic())))
                start = int(time.time() * 1000) + 3000
                end = start + self.a.measure * 1000
                dump(folder / 'control/window.json', [start, end])
                dump(folder / 'measurement-window.json', dict(start_ms=start, end_ms=end))
                self.phase('测量吞吐', self.a.measure)
                while time.time() * 1000 < end:
                    self.check_job(jid)
                    sample()
                    self.heartbeat('测量吞吐', end / 1000 - time.time())
                    time.sleep(min(1, max(0, end / 1000 - time.time())))
                (folder / 'control/stop').write_text('stop\n')
                self.phase('测量结束，正在排空输入')
                deadline = time.monotonic() + 600
                while time.monotonic() < deadline:
                    job = self.check_job(jid, ('RUNNING', 'FINISHED'))
                    if job['state'] == 'FINISHED':
                        break
                    sample()
                    self.heartbeat('测量结束，正在排空输入')
                    time.sleep(1)
                else:
                    raise TimeoutError('测量后排空超时。')
            self.phase('正在核验记录数和最终状态')
            for name, suffix in [('job', ''), ('checkpoints', '/checkpoints'),
                                 ('exceptions', '/exceptions'), ('execution-config', '/config')]:
                dump(folder / (name + '.json'), self.api('/jobs/' + jid + suffix))
            if self.api('/jobs/' + jid + '/checkpoints')['counts']['total'] != 0:
                raise RuntimeError('checkpoint应当关闭。')
            actual = json.loads((folder / 'topology.json').read_text())
            expected = json.loads((self.runtime / 'job/topology-contract.json').read_text())['workloads'][workload]
            edges = lambda es: Counter((e['from'], e['to']) for e in es)
            if {n['name'] for n in actual['nodes']} != set(expected['nodes']) or edges(actual['edges']) != edges(expected['edges']):
                raise RuntimeError('拓扑节点/边不符合主实验契约。')
            if any(n['parallelism'] != (1 if n['name'] == 'Global ACD' else 8) for n in actual['nodes']):
                raise RuntimeError('拓扑并行度不符合主实验契约。')
            source = [json.loads(p.read_text()) for p in (folder / 'control').glob('source-*.json')]
            sinks = [json.loads(p.read_text()) for p in (folder / 'sinks').glob('*.json')]
            if len(source) != 8 or not sinks:
                raise RuntimeError('缺少source/sink结果。')
            emitted = sum(x['records'] for x in source)
            received = sum(x['records'] for x in sinks)
            if received != emitted * MULTIPLIERS[workload]:
                raise RuntimeError('源和终端记录数不一致。')
            expected_signature = signed(sum(x['signature'] for x in source) * MULTIPLIERS[workload])
            if signed(sum(x['source_signature'] for x in sinks)) != expected_signature:
                raise RuntimeError('源和终端签名不一致。')
            cmd = [str(self.runtime / 'jdk/bin/java'), '-Xmx256m', '-XX:ActiveProcessorCount=4',
                   '-cp', str(self.oracle) + ':' + str(self.runtime / 'job/paper-steady-benchset.jar'),
                   'PaperSourceOracle', str(folder / 'control'), str(self.a.keys), workload]
            oracle = json.loads(subprocess.check_output(cmd, env=self.env, text=True, timeout=300))
            projection = signed(sum(x['feature_projection'] for x in sinks))
            if projection != oracle['expected_feature_projection']:
                raise RuntimeError('最终feature状态投影未通过独立oracle。')
            oracle['actual_feature_projection'] = projection
            dump(folder / 'source-oracle.json', oracle)
            if any(x['measurement_start_ms'] != start or x['measurement_end_ms'] != end for x in sinks):
                raise RuntimeError('Sink计时窗口不一致。')
            counts = [sum(x['second_counts'][i] for x in sinks) for i in range(self.a.measure)]
            measured = sum(x['measured_records'] for x in sinks)
            if measured != sum(counts):
                raise RuntimeError('逐秒计数与窗口计数不一致。')
            window_samples = [x for x in samples if start <= x['time_ms'] <= end]
            rss = [sum(x['roles'][r]['memory'].get('rss_bytes', 0) for r in ['tm1', 'tm2']) for x in window_samples]
            pss = [sum(x['roles'][r]['memory'].get('pss_bytes', 0) for r in ['tm1', 'tm2']) for x in window_samples]
            row.update(status='passed', throughput_input_records_s=measured / MULTIPLIERS[workload] / self.a.measure,
                       measured_sink_records=measured, input_equivalent_records=measured / MULTIPLIERS[workload],
                       sink_multiplier=MULTIPLIERS[workload], measurement_seconds=self.a.measure,
                       measurement_start_ms=start, measurement_end_ms=end, per_second_sink_records=counts,
                       warmup_seconds=self.a.warmup, coverage_seconds=coverage_seconds,
                       source_records=emitted, sink_records=received, oracle_matches=True,
                       keys=self.a.keys, feature_width=64, touches=128, checkpoint_count=0,
                       tm_pss_mean_bytes=statistics.mean(pss) if pss else None,
                       tm_rss_peak_bytes=max(rss) if rss else None, artifacts=self.hashes,
                       l0_enabled=arm in self.l0_sizes, l0_requested_total_bytes=self.l0_sizes.get(arm, 0), job_id=jid,
                       l0_proof=json.loads((folder / 'l0-proof.json').read_text()), oracle=oracle,
                       topology=actual,
                       flink_configurations={role: {line.split(': ', 1)[0]: json.loads(line.split(': ', 1)[1])
                                                   for line in (folder / (role + '-config.yaml')).read_text().splitlines()}
                                             for role in ['jm', 'tm1', 'tm2']})
            dump(folder / 'measurement.json', row)
            print('本轮成功：%s / %s / 第%d次；吞吐 %.2f 条输入/秒。' %
                  (workload.upper(), arm_name(arm), repeat, row['throughput_input_records_s']), flush=True)
        except KeyboardInterrupt:
            row.update(status='interrupted', message='用户停止本轮测试')
            self.rows.append(row)
            self.failures.append(row)
            raise
        except Exception:
            row['error'] = traceback.format_exc()
            row['message'] = str(sys.exc_info()[1])
            row['diagnostics'] = {}
            for role in ['jm', 'tm1', 'tm2']:
                log_dir = self.work / role / 'log'
                row['diagnostics'][role] = {}
                if log_dir.is_dir():
                    for log in log_dir.iterdir():
                        if log.is_file():
                            with log.open('rb') as stream:
                                stream.seek(max(0, log.stat().st_size - 8192))
                                row['diagnostics'][role][log.name] = stream.read().decode(errors='replace')
            dump(folder / 'failure.json', row)
            self.failures.append(row)
            print('本轮失败：%s / %s。原因：%s。其他条件继续。' %
                  (workload.upper(), arm_name(arm), row['message']), flush=True)
        finally:
            self.stop(folder)
        self.rows.append(row)
        self.write_results()

    def write_results(self):
        summary = []
        for workload in self.a.workloads:
            baseline = [row['throughput_input_records_s'] for row in self.rows
                        if row['workload'] == workload and row['arm'] == 'brisk_off' and row['status'] == 'passed']
            for arm in self.a.arms:
                rows = [row for row in self.rows if row['workload'] == workload and row['arm'] == arm]
                passed = [row for row in rows if row['status'] == 'passed']
                throughput = statistics.mean(row['throughput_input_records_s'] for row in passed) if passed else None
                summary.append(dict(workload=workload, arm=arm,
                                    l0_requested_total_bytes=self.l0_sizes.get(arm, 0),
                                    valid_repeats=len(passed),
                                    failed_repeats=sum(row['status'] == 'failed' for row in rows),
                                    skipped_repeats=sum(row['status'] == 'skipped' for row in rows),
                                    interrupted_repeats=sum(row['status'] == 'interrupted' for row in rows),
                                    throughput_input_records_s_mean=throughput,
                                    speedup_vs_brisk_off=(throughput / statistics.mean(baseline))
                                    if throughput is not None and baseline and statistics.mean(baseline) > 0 else None))
        counts = Counter(row['status'] for row in self.rows)
        sdk = self.output / 'l0-preflight/sdk.json'
        capacity = self.output / 'l0-preflight/capacity.json'
        dump(self.result_file, dict(schema_version=2, status=self.state, mode=self.label,
             started_ms=self.started_ms, finished_ms=self.finished_ms, progress=self.progress,
             counts=dict(planned=len(self.plan), completed=len(self.rows), passed=counts['passed'],
                         failed=counts['failed'], skipped=counts['skipped'], interrupted=counts['interrupted']),
             environment=self.environment_info, artifacts=self.hashes, plan=self.plan,
             l0_sdk=json.loads(sdk.read_text()) if sdk.exists() else None,
             l0_capacity=json.loads(capacity.read_text()) if capacity.exists() else None,
             l0_preflight=self.preflight, summary=summary, runs=self.rows, error=self.error))

    def run(self):
        try:
            for repeat in range(1, self.a.repeats + 1):
                workloads = self.a.workloads.copy()
                random.Random(20261111 + repeat).shuffle(workloads)
                for index, workload in enumerate(workloads):
                    selected = self.a.arms
                    offset = (repeat + index) % len(selected)
                    arms = selected[offset:] + selected[:offset]
                    self.plan.extend(dict(workload=workload, arm=arm, repeat=repeat) for arm in arms)
            self.state = 'running'
            self.phase('检查实验环境和运行资源')
            self.write_results()
            self.environment()
            self.preflight_l0()
            self.write_results()
            for condition in self.plan:
                self.current = condition
                self.run_one(**condition)
            self.current = None
            counts = Counter(row['status'] for row in self.rows)
            self.state = ('partial_results' if counts['passed'] else 'failed') if self.failures else 'complete'
            self.finished_ms = int(time.time() * 1000)
            self.phase('测试全部成功，结果已保存' if not self.failures else '测试已结束，成功数据和失败原因均已保存')
            self.write_results()
            return 1 if self.failures else 0
        except BaseException as error:
            self.state = 'stopped' if isinstance(error, KeyboardInterrupt) else 'failed'
            self.finished_ms = int(time.time() * 1000)
            self.error = dict(message='用户停止测试' if isinstance(error, KeyboardInterrupt) else str(error),
                              traceback=traceback.format_exc())
            self.phase('测试已停止，保留已完成数据' if self.state == 'stopped' else '测试无法继续，错误已保存')
            self.write_results()
            return 130 if self.state == 'stopped' else 1
        finally:
            self.stop()
            shutil.rmtree(self.work, ignore_errors=True)
            print('结果JSON：', self.result_file, flush=True)


class Arguments(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError('参数错误：' + message)


def main():
    campaign = None
    try:
        parser = Arguments(description=__doc__)
        parser.add_argument('--runtime', required=True)
        parser.add_argument('--work-dir', required=True)
        parser.add_argument('--results', required=True)
        parser.add_argument('--workloads', default=','.join(WORKLOADS))
        parser.add_argument('--arms', default=','.join(ARMS), help='默认运行Heap、无L0及六档L0，共八组')
        parser.add_argument('--keys', type=int, default=524288)
        parser.add_argument('--warmup', type=int, default=60)
        parser.add_argument('--measure', type=int, default=120)
        parser.add_argument('--repeats', type=int, default=3)
        parser.add_argument('--validation-capacity-mib', type=float, default=32,
                            help='仅DRAM验收的模拟可用共享容量')
        parser.add_argument('--validate-dram', action='store_true')
        parser.add_argument('--smoke', action='store_true')
        args = parser.parse_args()
        args.workloads = args.workloads.split(',')
        args.arms = args.arms.split(',')
        if not args.workloads or any(workload not in WORKLOADS for workload in args.workloads):
            parser.error('未知workload。')
        if not args.arms or any(arm not in ARMS for arm in args.arms):
            parser.error('未知组别。')
        if args.smoke:
            args.keys, args.warmup, args.measure, args.repeats = 4096, 2, 5, 1
        if args.keys <= 0 or args.keys % 8 or args.measure <= 0 or args.warmup < 0 or args.repeats <= 0:
            parser.error('keys必须是8的正倍数，measure/repeats必须为正。')
        if not 2 <= args.validation_capacity_mib <= 256:
            parser.error('普通内存验收容量须在2–256MiB之间。')
        lock = (Path(args.runtime).parent / 'campaign.lock').open('w')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        campaign = Campaign(args)
        return campaign.run()
    except BaseException as error:
        if campaign is not None:
            raise
        message = str(error) or type(error).__name__
        run_dir = Path(os.environ['BRISKSTATE_RUN_DIR'])
        dump(Path(os.environ['BRISKSTATE_RESULT_FILE']),
             dict(schema_version=2, status='failed', error=dict(stage='setup', message=message,
                  traceback=traceback.format_exc()), runs=[]))
        (run_dir / 'status.txt').write_text('状态：启动失败。\n原因：' + message + '\n')
        print('无法开始测试：' + message, flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
