# ForL0内存随预算增长的归因（WC，三次独立重复）

状态域固定。默认G1允许年轻代最大为Java堆的60%；本组ForL0的Eden在计时窗口内接近该上限。Flink预算增大使Xms=Xmx增大，Eden随之扩张。页在处理过程中被触碰后成为实际驻留，GC释放对象后仍可能由JVM保留。

|每TM预算GiB|每TM Xmx GiB|两TM Eden目标GiB|两TM运行时PSS GiB|GC后Java heap-used GiB|
|---|---|---|---|---|
|32|22.125|26.53|30.03|0.071|
|40|30.125|36.12|40.16|0.107|
|48|38.125|45.72|50.25|0.189|
|56|46.125|55.31|60.42|0.158|
|64|54.125|64.91|70.56|0.163|

每TM预算增加8GiB时，两TM堆总容量增加16GiB；其60%约为9.6GiB，与Eden每档增加约9.59GiB吻合。PSS每档约增加10.1GiB，剩余增长还包含GC元数据、运行时等。

独立的计时后诊断也支持这一点：暂停/排空、Full GC及histogram后，两TM RSS从30.15GiB增至70.81GiB，增加40.66GiB。利用Java堆地址和NUMA驻留页得到的保守界限表明，至少38.17GiB增量位于Java堆，约占该RSS增量的93.9%。

64GiB预算下两TM Java堆驻留RSS界限为[64.79, 67.62]GiB，而GC后Java heap-used约0.163GiB。这说明大量Java堆页已无存活对象但仍驻留；不能将高PSS全算为ForL0原生状态。

Eden/PSS来自主计时窗口；驻留界限与heap-used来自计时后的不同诊断快照，不能精确逐字节相减。NUMA映射与堆边界合并时给出保守界限，未强行精确归属。

[逐条件诊断CSV](analysis/memory-mechanism.csv)、[全部逐次数据和算法说明](analysis/memory-mechanism.json)、[Java11 G1参数定义](https://docs.oracle.com/en/java/javase/11/tools/java.html)。

本说明依据原主实验已保存的GC日志、诊断快照与地址映射。Java堆RSS界限支持上述观察性归因；尚未逐字节归属所有原生内存。
