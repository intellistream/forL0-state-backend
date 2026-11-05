# Kunpeng Top-down measurements

2026-11-02. Microarchitecture measurements are required for future campaigns.
They must use actual PMU counts, independently of CPU/GC profiling and formal
wall-clock timings. A CPU flame graph cannot determine pipeline-slot fractions.

## Current blocker and installed tool

Official DevKit 26.2.RC1 ARM64 CLI is unpacked at
`/root/opt/DevKit-CLI-26.2.RC1-Linux-Kunpeng/devkit`. Its help detects Kunpeng-920
and supports `tuner top-down -L 0`, including the backend memory hierarchy.
Download: https://mirrors.huaweicloud.com/kunpeng/archive/DevKit/Packages/Kunpeng_DevKit/

The current Coder workspace is a Kubernetes deployment:

- Namespace: `coder-workspaces`
- Deployment: `coder-user-yjy-flink-32`
- Pod at investigation: `coder-user-yjy-flink-32-78898f698f-m5dfg`
- Node: `host-192-168-0-2`; host IP: `192.168.0.2`
- Host kernel: `5.15.0-25-generic`; container userland: openEuler 24.03
- `kernel.perf_event_paranoid=4`; container has no `CAP_PERFMON`/`CAP_SYS_ADMIN`
- `/proc/sys` is read-only; no sudo or container-manager socket in the container
- `perf_event_open` for user-only cycles fails; async-profiler cycles also fails
- Workspace service account can read its Pod, but Kubernetes reports
  `allowed:false` for patching this deployment and reading the node
- Supplied `train8` logs into `train05`, kernel 5.10; `huawei-jump` logs into
  `controller`, kernel 6.6. Their kernels differ from this workload host.
  SSH from that jump host to `user@192.168.0.2` times out. `flink-host` fails
  its Cloudflare handshake. None supplied a usable admin session on this node.

**Access instructions corrected on 2026-11-03:** current Seccomp is 0,
so the observed EACCES is not caused by a seccomp filter. The old Ubuntu
5.15.0-25 kernel has an additional paranoid=4 compatibility issue: granting
CAP_PERFMON alone is not a sufficient universal prescription. Ubuntu documented
and patched the CAP_SYS_ADMIN-only level-4 check in November 2025.

Preferred immediate route: an administrator runs perf/DevKit on the actual host
against both TaskManagers' host PIDs. This requires no workspace recreation and
no global sysctl relaxation. For durable in-container collection, an administrator
can set host perf_event_paranoid=2 for this old kernel and add PERFMON to the
workspace container in the Coder template, preserving existing settings; then
recreate the Pod and validate real event access. If the runtime applies seccomp,
its profile must allow perf_event_open. The current container has no such filter.
A kernel with the Ubuntu fix may instead permit retaining paranoid=4, but must
be verified. See [the concrete administrator guide](BriskState_PMU_Access.md).

References: [Ubuntu fix](https://lists.ubuntu.com/archives/kernel-team/2025-November/164447.html),
[Linux perf security](https://cdn.kernel.org/doc/html/latest/admin-guide/perf-security.html).

No real Top-down charts have been produced while event access is denied.

## Collection and two requested charts

1. Run one workload/backend at a time, L0 disabled, checkpoints disabled, same
   entity keys / feature width / touches for both backends. Keep graphs identical.
2. Use sufficiently many records to keep **both** variants actively processing
   throughout the same steady-state window (at least 30 seconds after warm-up).
   Current 8M-record WC finishes too quickly; increase records rather than
   including idle JVM time in its measurement. Keep all raw runs.
3. During that window, run inside the container:

   ```bash
   bash benchmark/scripts/collect_topdown.sh /root/briskstate-setup/pmu/wc/heap 30
   ```

   The script checks hardware permission first, collects both TM PIDs in one
   command, and retains the vendor archive, raw event counts and text report.
   It does not submit/stop the Flink job or automatically prove window coverage:
   retain job start/end times and collection start/end times before accepting it.
4. Repeat for all 7 workloads and both backends. Construct a manifest:

   ```json
   {"runs":[
     {"workload":"wc","backend":"heap","report":"wc/heap/report.txt"},
     {"workload":"wc","backend":"forl0","report":"wc/forl0/report.txt"}
   ]}
   ```

   Add FD/SD/TM/LG/VS/LR entries, then run:

   ```bash
   python3 benchmark/scripts/plot_topdown.py /root/briskstate-setup/pmu/manifest.json \
     --output /root/briskstate-setup/pmu/charts
   ```

The plotter requires all 14 reports, a unique vendor ALL aggregate, nonzero cycles
and instructions, raw hardware event counts, and a consistent metric hierarchy.
It fails on missing data rather than replacing unavailable counters with zero.
It emits PNG/PDF/SVG plus the exact plotted data and raw-report SHA-256.

- **Chart 1:** paired Heap/ForL0 bars per workload, stacked Retiring, Bad
  Speculation, Frontend Bound, Backend Bound; percentage of total pipeline slots.
- **Chart 2:** paired bars normalized to Backend Bound: Core Bound, L1 Bound,
  L2 Bound, L3 or DRAM Bound, Store Bound. The label above each bar gives the
  parent Memory Bound share. Memory Bound is not stacked again with its children.

Kunpeng-920's reported **L3 or DRAM Bound** must retain that combined name if
that is what the tool measures. L1 Bound includes L1-access hazards and is not
synonymous with L1 cache misses. Cache miss rate/MPKI is a different denominator;
record it as an extra table/annotation, not a segment of a pipeline-slot stack.
Resource Bound is nested under Core Bound in this tool and is not counted twice.

Use vendor aggregate counters for both TMs. Do not take an unweighted average of
per-core/per-process percentages. Do not force formulas for Intel or Arm Neoverse
onto the Kunpeng-920 event model. Multi-pass/multiplexed events require stable
work and retained enabled/running-time evidence before causal interpretation.

Primary references:

- https://www.hikunpeng.com/doc_center/source/en/kunpengdevps/profiler/profiler/KunpengDevKitCli_0033.html
- https://github.com/openeuler-mirror/libkperf
- https://raw.githubusercontent.com/openeuler-mirror/libkperf/master/docs/Details_Usage.md

libkperf is an alternate PMU API, not a workaround for denied hardware access.
Its direct user-register counting mode requires kernel 6.6+ and an enabled
`perf_user_access` setting, which this host does not satisfy.
