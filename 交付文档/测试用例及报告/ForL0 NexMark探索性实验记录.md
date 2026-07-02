# ForL0 NexMark 探索性实验记录

本文档记录为寻找 ForL0 在 NexMark 中的优势区而做过的非合同项探索。正式测试报告仅保留最终可采用结果；本文档中的中间结果不作为合同验收结论。

## 历史 TPS/backpressure 探索样本

本节记录修复监控、修复 `--category` 生效问题，并补齐所有查询后的 `forl0_tps_probe` 历史样本。该组结果用于定位后续 no-Full-GC 复跑方向，已从正式测试报告中移除。

- 查询：NexMark `q4/q5/q8/q9/q11/q18/q19/q20`。
- 模式：TPS / monitor-duration 模式，`events.num=0`，监控 sink 侧 `numRecordsInPerSecond`。
- 逐查询目标 TPS：q4 `500 K/s`，q5 `2 M/s`，q8/q19 `1 M/s`，q9 `50 K/s`，q11/q20 `200 K/s`，q18 `5 M/s`。
- 事件比例：默认 Person:Auction:Bid = `1:49:50`；q8 使用 `25:25:50`；q11 使用 `10:10:80`；q19 使用 `1:1:98`。
- 配置：parallelism = 4，关闭 checkpoint、mini-batch、distinct-agg split，启用 object reuse；本轮 standalone fallback 为 TaskManager `8 GB`、`32 slots`、heartbeat timeout `300s`。
- q5 使用 `queries-forl0/q5.sql` 稳态窗口输出变体，合同 `queries/q5.sql` 未修改。
- 口径：监控窗口内 sink 侧实际输入 TPS，主表提升率不使用 `/core` 归一化。
- 结果定位：q4/q18/q19 曾呈现较高提升，但该轮未施加 no-Full-GC 约束，因此仅作为探索记录；正式测试报告以后续 no-Full-GC 复跑结果为准。

| Q | TPS | 事件比例 | HashMap | ForL0 | 提升 | CPU 核数 HashMap / ForL0 | 配置目的 |
|---|---:|---|---:|---:|---:|---:|---|
| q4 | 600 K/s | 1:49:50 | 31.49 K/s | 75.67 K/s | +140.3% | 73.56 / 29.53 | 提高 auction 高基数窗口聚合状态压力 |
| q5 | 2 M/s | 1:49:50 | 151.24 K/s | 142.54 K/s | -5.8% | 50.97 / 17.55 | 使用稳态输出 SQL，保留 HOP 窗口状态压力 |
| q8 | 1 M/s | 25:25:50 | 11.05 K/s | 10.06 K/s | -9.0% | 28.64 / 15.01 | 提高 Person/Auction 匹配概率 |
| q9 | 50 K/s | 1:49:50 | 12.31 K/s | 12.33 K/s | +0.2% | 10.79 / 4.63 | 控制双流 Join 输入压力 |
| q11 | 200 K/s | 10:10:80 | 12.23 K/s | 12.13 K/s | -0.8% | 2.39 / 3.40 | 延长监控窗口覆盖 Session 输出 |
| q18 | 5 M/s | 1:49:50 | 130.38 K/s | 206.84 K/s | +58.6% | 52.21 / 5.68 | 放大 `(bidder, auction)` 高基数去重状态 |
| q19 | 1 M/s | 1:1:98 | 716.46 K/s | 1.14 M/s | +59.1% | 57.50 / 7.53 | 构造 bid 密集 TopN 状态 |
| q20 | 200 K/s | 1:49:50 | 11.97 K/s | 13.26 K/s | +10.8% | 42.34 / 3.29 | 保持分组聚合稳定输出 |

- 原始结果：
  - q4：`benchmark/results/nexmark_20260701_164021/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_170353/nexmark_results.json`。
  - q5：`benchmark/results/nexmark_20260701_164226/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_170557/nexmark_results.json`。
  - q8：`benchmark/results/nexmark_20260701_164422/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_170753/nexmark_results.json`。
  - q9：`benchmark/results/nexmark_20260701_165330/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_170947/nexmark_results.json`。
  - q11：`benchmark/results/nexmark_20260701_165529/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_171142/nexmark_results.json`。
  - q18：`benchmark/results/nexmark_20260701_165804/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_171417/nexmark_results.json`。
  - q19：`benchmark/results/nexmark_20260701_181126/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_181333/nexmark_results.json`。
  - q20：`benchmark/results/nexmark_20260701_170150/nexmark_results.json` 与 `benchmark/results/nexmark_20260701_171800/nexmark_results.json`。

## 已废弃的早期 sink 口径结果

修复 `--category` 未生效、逐查询事件比例和 standalone 资源配置后，早期仅 q18 的 sink 样本被完整逐查询结果替代，不再进入主报告：

- q18 早期 sink 口径：HashMap `24.93 K/s`，ForL0 `192.56 K/s`，提升 `+672.4%`。
- 原始结果：`benchmark/results/nexmark_20260630_221219/nexmark_results.json` 与 `benchmark/results/nexmark_20260630_221456/nexmark_results.json`。

## 已废弃的 source 口径结果

修复前的 TPS 监控读取第一个 source 顶点的 `numRecordsOutPerSecond`，不能代表端到端完成吞吐。以下旧结果不再进入主报告：

- q5 source 口径：HashMap `21.65 K/s`，ForL0 `31.05 K/s`，提升 `+43.4%`。
- q11 source 口径：HashMap `420.09 K/s`，ForL0 `494.69 K/s`，提升 `+17.8%`。
- q18 source 口径：HashMap `49.98 K/s`，ForL0 `408.31 K/s`，提升 `+716.9%`。

## 未采用探索

### WordCount comfort_5m_200m

- 目的：确认 ForL0 在高基数 `ValueState<Long>` 下的端到端优势。
- 配置：5M Long keys，200M records，parallelism = 8，checkpoint 关闭。
- 结果：两轮端到端 raw throughput 分别提升 `+38.7%` 与 `+60.5%`，aggregate `+48.9%`。
- 未放入主报告原因：该结果属于 WordCount，不回答“从 NexMark 里面找舒适区”的目标。

### NexMark /core 诊断配置

- 场景：`forl0_advantage`，主要查询 `q11/q19/q20/q9`。
- 现象：原始吞吐基本持平，但 `/core` 指标显著提高。
- 未采用原因：`/core` 是 CPU 效率诊断信号，不代表端到端吞吐提升。

### NexMark checkpoint probe

- 场景：`forl0_checkpoint_probe`，高 auction/person 基数，1s checkpoint。
- 最好结果：q5 event-count / process wall-clock 口径下约 `+2.8%`。
- 未采用原因：不足 30%，且其他查询基本持平或为负。

### NexMark TPS/backpressure 逐查询扩展中的未采用项

- `q4`：早期 TPS/backpressure 口径下 HashMap 在预期窗口内未返回 summary，手动中断；已被 20260701 完整复跑替代。
- `q8`：早期 TPS/backpressure 口径下 HashMap 与 ForL0 均返回 `0` source TPS，不形成有效吞吐样本。原始结果：`benchmark/results/nexmark_20260630_214346/nexmark_results.json`；已被 20260701 完整复跑替代。
- `q9`：早期 TPS/backpressure 口径下 HashMap 在预期窗口内未返回 summary，手动中断；已被 20260701 完整复跑替代。
- `q11`：早期修复后的 sink 口径下 HashMap 60s 窗口内 sink TPS 为 `0`，ForL0 侧未形成完整有效样本；已被 20260701 完整复跑替代。
- `q19`：早期 Top-N 状态查询在 TPS/backpressure 口径下 HashMap 未在预期窗口返回 summary，手动中断；已被 20260701 完整复跑替代。
- `q20`：早期 TPS/backpressure 口径下 HashMap 在预期窗口内未返回 summary，手动中断；已被 20260701 完整复跑替代。
- `q18` source 口径复跑：修复前未在预期窗口返回 summary，手动中断；该口径已废弃。

## 下一轮可选优化机会

这些机会仅作为后续优化候选，不进入当前正式报告结论。

- `q19`：保持 `tps=1M`、`1:1:98`，扫描 `delay=50/90/120s`、`duration=60s`，每个后端重复 2 次取中位数。目标是确认 TopN 状态积累后 HashMap 的后半段退化是否稳定，并观察 `+59.1%` 是否可进一步提高。
- `q4`：保持 `1:49:50`，扫描 `tps=500K/750K/1M`、`delay=20s`、`duration=60s`。目标是确认当前 `+140.3%` 强优势不是单点样本，并观察输入压力上限。
- `q18`：扫描 `tps=5M/7.5M/10M`，优先保持 `1:49:50`，必要时补 bid-dense `1:1:98`。目标是验证去重 / ROW_NUMBER 高基数状态是否能稳定维持 60% 以上端到端收益。
- `q8`：复核 `25:25:50`、`tps=2M`、`delay=40s`、`duration=120s`。该方向存在正向探索样本，但绝对 sink TPS 较低，只有重复稳定且 profile 能解释时才考虑进入报告。
- `q20`：优先增强 operator 级监控，采集 Join / GroupAggregate 的 `numRecordsInPerSecond`、`busyTime`、`backPressuredTime`。当前 sink TPS 仅 `+10.8%`，但 CPU 核数差异明显，适合定位资源效率，不宜直接包装为端到端吞吐优势。

### q5/q8/q11/q19 负结果复查

- `q5`：将目标 TPS 从 `2 M/s` 提高到 `5 M/s` 后，1:49:50 下 HashMap / ForL0 仅为 `24.14 K/s` / `25.27 K/s`；改为 1:1:98 后为 `3.38 K/s` / `5.33 K/s`。两组都说明 q5 sink 输出受 HOP 窗口触发和背压波动主导，虽然部分点相对提升为正，但绝对吞吐显著低于主表 q5，不采用。
- `q8`：25:25:50、`2 M/s` 下 HashMap / ForL0 为 `1.55 K/s` / `2.95 K/s`；45:45:10 下 sink TPS 为 0。q8 窗口 Join 对 Person/Auction 匹配和窗口闭合敏感，调高输入压力会牺牲有效输出，不采用。
- `q11`：1:1:98、`1 M/s`、120s 监控配置触发 Flink 作业失败，driver 未干净返回；保留主表中 10:10:80、`200 K/s` 的稳定样本。q11 session 输出由会话闭合节奏主导，不作为优势区。
- `q19`：初始 1:49:50、`200 K/s` 下基本持平；改为 1:1:98、`1 M/s` 后 20s delay 样本提升 `+16.5%`，50s delay 稳态样本提升 `+59.1%`。该配置仅保留为探索记录，正式报告以后续 no-Full-GC 复跑口径为准。

### NexMark q23/q24/q25 自定义聚合

- `q23`：按 `(bidder, auction)` 聚合，状态大但 key 复用不足，提升约 `+1.9%`。
- `q24`：按 `bidder` 聚合，默认 event-count 口径约 `+1.2%`。
- `q25`：按 `bidder % 1,000,000` 聚合，默认 event-count 口径约 `+2.2%`。
- tight-memory / true-FINISHED 尝试：部分配置下 HashMap 长时间不完成，无法形成有效可比吞吐结论。
- 未采用原因：未达到 30%，或口径不可比。

### event-count 与 true-FINISHED 口径尝试

- event-count 模式下，NexMark reporter 以 source 侧 TPS 归零作为结束信号，容易形成固定 monitor 窗口，不能充分反映 q5 下游状态算子的稳态 backpressure。
- true-FINISHED 口径对部分 unbounded SQL 聚合不适用，作业长时间不自然结束。
- 最终改用 TPS / monitor-duration 模式，使 source 实际输出 TPS 受下游 backpressure 约束，得到可比较的 q5 稳态端到端输入吞吐。

### Client Usecase hotspot-drift 探索

- 配置：新增 `benchmark/client-drift` driver，保留客户 `HuaweiTestFunction` 双流 Join + MapState / ValueState 逻辑，仅替换 source，使 80% 流量落在随输入进度迁移的热点 join key 窗口。
- `hotspot_drift_300k`，`right_delay_ms=2`：HashMap `889 rec/s/core`，ForL0 `935 rec/s/core`，提升 `+5.2%`。原始结果：`client_usecase_hashmap_20260702_173244.json` 与 `client_usecase_forl0_20260702_173412.json`。
- `hotspot_drift_1m`，`right_delay_ms=2`：HashMap `930 rec/s/core`，ForL0 `945 rec/s/core`，提升 `+1.6%`。原始结果：`client_usecase_hashmap_20260702_174101.json` 与 `client_usecase_forl0_20260702_174533.json`。
- 无右流延迟的 300K 快速复测：HashMap `9266 rec/s/core`，ForL0 `6217 rec/s/core`，提升 `-32.9%`。该口径作业仅 8--12 秒，启动、对象构造、序列化和 bounded 自动取消收尾占比较高，不作为默认配置。
- bytecode 复查：客户函数内部主要状态为 `LeftCache(Long -> List<PVMVLogType>)`、`RightCache(Long -> PVMVLogType)`、`LeftDuplicateRcd(String -> Long)`、`RightDuplicateRcd(String -> Long)` 和若干 `ValueState<Long>`；其中 `LeftCache/RightCache` 的内部 MapState key 来自 `eventTimeStamp`，不是 join key。
- `hotspot_state_left_2m`：新增轻量 payload、100% 左流、`eventTimeStamp` 热桶漂移，直接打 `LeftCache` 与 duplicate map。HashMap `41,303 rec/s/core`，ForL0 `41,543 rec/s/core`，提升 `+0.6%`。原始结果：`client_usecase_hashmap_20260702_175355.json` 与 `client_usecase_forl0_20260702_175414.json`。
- `hotspot_state_join_2m`：轻量 payload、90% 左流 / 10% 右流、`eventTimeStamp` 热桶漂移，保留少量右流 drain。HashMap `8,885 rec/s/core`，ForL0 `8,902 rec/s/core`，提升 `+0.2%`。原始结果：`client_usecase_hashmap_20260702_175539.json` 与 `client_usecase_forl0_20260702_175643.json`。
- 未采用原因：该方向验证了热点漂移 source 和一键脚本可运行，但端到端收益未达到 30% 门槛。更重要的是，客户 operator 的核心状态是 `MapState<Long, List<PVMVLogType>>` 与 POJO value，收益由 list value 序列化、iterator/drain、Join 业务逻辑和 bounded 自动停止共同主导；这类状态形态不是 ForL0 当前最强的 primitive / scalar 状态访问路径。正式汇报继续把 Client Usecase 作为真实业务形态无回退验证，不作为 ForL0 强优势区。
