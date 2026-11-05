# 当前 Coder 工作区启用 PMU

2026-11-03 复核：内核 `5.15.0-25-generic`，`perf_event_paranoid=4`，缺少 CAP_PERFMON/CAP_SYS_ADMIN；`Seccomp=0`。`armv8_pmuv3_0` 和 HiSilicon L3/DDR PMU 入口存在，但 user-only cycles 的 perf_event_open 返回 EACCES。这是访问权限阻塞，不能据此认定 PMU 硬件不存在。

此前定位工作节点为 `host-192-168-0-2` / `192.168.0.2`，namespace 为 `coder-workspaces`，Deployment 为 `coder-user-yjy-flink-32`。管理员应通过当前 Pod 的 nodeName 再确认节点，不应使用另一台可 SSH 的机器代替。

## 方案一：宿主机管理员采集

这是当前最快的办法，不要求重新创建工作区容器，也不要求放宽全机的 perf_event_paranoid。

1. 管理员登录实际工作节点，在宿主机运行匹配内核的 perf 和鲲鹏 DevKit。
2. 用容器运行时/cgroup 找到此工作区两个 TaskManager 的宿主机 PID。容器 PID 不应直接当宿主机 PID。
3. 工作负载运行时先验证：

```bash
sudo perf stat -e cycles:u,instructions:u -p <TM1_HOST_PID>,<TM2_HOST_PID> -- sleep 5
```

4. 再采集鲲鹏完整 Top-down：

```bash
sudo /path/to/devkit tuner top-down -L 0 -d 30 -p <TM1_HOST_PID>,<TM2_HOST_PID>
```

采集范围包括两台 TM 内所有相关线程，保留原始事件和 ALL 聚合。按进程采集可避免把同 CPU 上其他工作负载混入。

## 方案二：长期允许工作区内采集

由 Coder 模板/Kubernetes 管理员操作：

1. 对当前旧 Ubuntu 内核，可先在实际工作节点将 perf_event_paranoid 从 4 调到 2；这是宿主机设置，不能靠容器内 sudo 或更换镜像解决。

```bash
sudo sysctl -w kernel.perf_event_paranoid=2
```

2. 在 Coder 模板的 workspace 容器 securityContext 中保留现有配置，并增加：

```yaml
securityContext:
  capabilities:
    add: ["PERFMON"]
```

3. 若使用 seccomp 过滤，必须允许 perf_event_open。当前实测 Seccomp=0，无需为了这一项先关闭过滤。若 Pod 重建后启用 RuntimeDefault，需要重新检查运行时规则；可使用允许该 syscall 的 Localhost profile。
4. 应用模板并重新创建工作区 Pod，新 capability 才会进入进程的 bounding/permitted/effective sets。先保存工作并停止任务；Pod 重建会结束进程。
5. 验证 capability 及真实事件调用：

```bash
python3 /root/forL0-state-backend/benchmark/scripts/probe_pmu.py
```

要求 user_cycles_open.success 为 true；随后还须验证附加两个 TM 的硬件计数和 DevKit 所需原始事件能实际计数。单看 CAP 位或工具列出事件不等于采集成功。

主线 Linux 通常由 CAP_PERFMON 绕过 perf 范围权限检查。但旧 Ubuntu 的 paranoid=4 还要求 CAP_SYS_ADMIN；2025 年的 Ubuntu 修复补丁专门将这项检查改为 perfmon_capable()。因此本机不能把“仅加 PERFMON”当成必然成功。替代方式是升级至包含修复的宿主机内核并验证，或采用方案一。无需把 CAP_SYS_ADMIN 或 privileged 当作首选。

## 权限通过后的现有流程

DevKit 已安装在容器：
`/root/opt/DevKit-CLI-26.2.RC1-Linux-Kunpeng/devkit`

运行足够长的任务，进入稳定处理阶段后：

```bash
cd /root/forL0-state-backend
bash benchmark/scripts/collect_topdown.sh /root/briskstate-setup/pmu/wc/heap 30
```

先跑单个 workload 证明全套原始事件可用，再跑 7 个 workload × 2 backend。两张图分别使用一级 Top-down 与 Backend 的互斥子项。Kunpeng 920 的 L3 or DRAM Bound 应保留工具的合并命名；L1 Bound 与 L1 cache miss rate/MPKI 不是同一个指标。PMU 计数器不足时可能多轮/复用采集，必须确保采集区间有稳定负载并保留 enabled/running 证据。

参考：

- Linux perf 安全模型：https://cdn.kernel.org/doc/html/latest/admin-guide/perf-security.html
- Ubuntu paranoid=4 修复：https://lists.ubuntu.com/archives/kernel-team/2025-November/164447.html
- Kubernetes capabilities/seccomp：https://kubernetes.io/docs/tasks/configure-pod-container/security-context/
- 鲲鹏 DevKit Top-down：https://www.hikunpeng.com/doc_center/source/en/kunpengdevps/profiler/profiler/KunpengDevKitCli_0033.html
