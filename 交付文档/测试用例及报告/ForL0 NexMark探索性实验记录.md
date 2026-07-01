# ForL0 NexMark 探索性实验记录

本文档记录为寻找 ForL0 在 NexMark 中的优势区而做过的非合同项探索。正式测试报告仅保留最终可采用结果；本文档中的中间结果不作为合同验收结论。

## 最终采用结果

最终保留到主报告的是修复监控、修复 `--category` 生效问题，并补齐所有查询后的 `forl0_tps_probe`：

- 查询：NexMark `q4/q5/q8/q9/q11/q18/q19/q20`。
- 模式：TPS / monitor-duration 模式，`events.num=0`，监控 sink 侧 `numRecordsInPerSecond`。
- 逐查询目标 TPS：q4 `500 K/s`，q5 `2 M/s`，q8/q19 `1 M/s`，q9 `50 K/s`，q11/q20 `200 K/s`，q18 `5 M/s`。
- 事件比例：默认 Person:Auction:Bid = `1:49:50`；q8 使用 `25:25:50`；q11 使用 `10:10:80`；q19 使用 `1:1:98`。
- 配置：parallelism = 4，关闭 checkpoint、mini-batch、distinct-agg split，启用 object reuse；本轮 standalone fallback 为 TaskManager `8 GB`、`32 slots`、heartbeat timeout `300s`。
- q5 使用 `queries-forl0/q5.sql` 稳态窗口输出变体，合同 `queries/q5.sql` 未修改。
- 口径：监控窗口内 sink 侧实际输入 TPS，主表提升率不使用 `/core` 归一化。
- 最终结果：q4 HashMap `31.49 K/s`、ForL0 `75.67 K/s`，提升 `+140.3%`；q18 HashMap `130.38 K/s`、ForL0 `206.84 K/s`，提升 `+58.6%`；q19 HashMap `716.46 K/s`、ForL0 `1.14 M/s`，提升 `+59.1%`。q5/q8/q11 在当前 SQL 形态下不作为优势区，完整逐 Q 表见主报告表 3。
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
- `q19`：初始 1:49:50、`200 K/s` 下基本持平；改为 1:1:98、`1 M/s` 后 20s delay 样本提升 `+16.5%`，50s delay 稳态样本提升 `+59.1%`。该配置已合并进主报告。

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
