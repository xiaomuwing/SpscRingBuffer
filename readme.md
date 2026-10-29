# SpscRingBuffer

.NET 10 进程内固定容量、单生产者单消费者（SPSC）环形缓冲区。
用于采集线程与处理线程之间的短时削峰和数据交接。

命名空间：`EdgeCompute.Infrastructure.Buffers`。类库无第三方依赖；正常读写使用数组和 Span，
不由缓冲区自身分配内存。支持任意正容量、单点/批量读写、整批写入、精确读取、Peek、主动丢弃、
停止后的 Clear/ResetStatistics 维护，以及统计采样。

## 项目结构

```text
SpscRingBuffer/
├─ SpscRingBuffer.slnx
├─ SpscRingBuffer/                        .NET 10 类库
│  ├─ SpscRingBuffer.cs
│  ├─ RingBufferStatistics.cs
│  └─ SpscRingBuffer.csproj
├─ SpscRingBuffer.Tests/                  .NET 10 控制台测试程序
│  ├─ Program.cs
│  ├─ TestRunner.cs
│  ├─ BufferTests.cs
│  ├─ ConcurrencyTests.cs
│  ├─ RegressionTests.cs
│  ├─ MaintenanceTests.cs
│  └─ SpscRingBuffer.Tests.csproj
├─ docs/
│  └─ ROADMAP.MD                         开发路线、设计演进与验证状态
├─ .gitattributes                        文本属性与统一行尾
├─ .gitignore                            忽略 bin、obj、.vs 等生成物
├─ AGENTS.md                             本目录的代码与验证约定
└─ readme.md
```

两个项目均面向 `net10.0`，使用稳定 C# 14（`LangVersion` 14.0），启用可空引用检查和隐式 using；类库生成 XML API 文档。
仓库中只有 `SpscRingBuffer.slnx` 一个解决方案，包含上述两个项目。安装 .NET 10 SDK 后，可在支持 .NET 10 的 Windows/Linux 环境构建；
目前完成验证的是 Windows X64/X86，Linux 和 ARM64 尚未验证，详见 [ROADMAP 的验证状态](docs/ROADMAP.MD#validation)。

## 构建和测试

在此解决方案所在目录运行：

```powershell
dotnet build SpscRingBuffer.slnx -c Release
dotnet run --project SpscRingBuffer.Tests -c Release --no-build
```

默认执行 31 个用例，每个常规并发用例传输 100 万个元素；容量为 1 的并发用例最多传输 5 万个元素。
测试由本项目的控制台程序执行，不使用 `dotnet test`，也不需要第三方测试框架。

```powershell
# 显示参数
dotnet run --project SpscRingBuffer.Tests -c Release --no-build -- --help

# 提高传输数量并重复完整测试
dotnet run --project SpscRingBuffer.Tests -c Release --no-build -- --items 5000000 --repeat 10

# 完整测试后在同一个队列上继续混合并发运行至少一小时
dotnet run --project SpscRingBuffer.Tests -c Release --no-build -- --stress-seconds 3600
```

退出码：`0` 全部通过，`1` 测试失败，`2` 参数错误。
每次双线程传输有 60 秒超时；长测由多轮传输组成，每轮结束后同步交接线程所有权。
长测时长按整轮结束，因此实际时间可能稍长于指定值。输出中的耗时用于诊断，不是实时延迟或吞吐验收指标。

## 引用类库

在使用方项目添加对 `SpscRingBuffer/SpscRingBuffer.csproj` 的项目引用，或引用生成的 `SpscRingBuffer.dll`。
类库还会生成 XML API 文档。

```csharp
using EdgeCompute.Infrastructure.Buffers;

var ring = new SpscRingBuffer<AcquisitionSample>(65_536);

// 由一个采集线程/串行驱动回调调用。
bool Publish(in AcquisitionSample sample) => ring.TryWrite(in sample);

// 由一个处理线程调用；读出后下游是否成功处理，由调用方负责。
bool TryTake(out AcquisitionSample sample) => ring.TryRead(out sample);

readonly record struct AcquisitionSample(
    long Sequence,
    long Timestamp,
    int ChannelId,
    double Value,
    uint Quality);
```

`Publish` 返回 false 时必须按项目策略处理。样本 `Sequence` 应在设备或采集适配层生成，
不能只对成功入队的数据编号，否则消费者无法通过序号发现未入队的样本。
时间戳单位和时钟来源同样由项目定义。

批量接口应明确传入 Span；部分写入后，调用方仍拥有未提交的后缀：

```csharp
int written = ring.TryWriteBatch(source.AsSpan());
// source[0..written] 已发布，source[written..] 未发布。
// 重试时只提交未成功的后缀，避免重复入队。

int read = ring.TryRead(destination.AsSpan());
// 仅 destination[0..read] 是本次读出的数据；其他位置保持原值。
```

`TryWrite` 只表示写入一个 T，`TryWriteBatch` 明确表示写入批量。因此 `objectRing.TryWrite(null)`
写入一个 null，`intRing.TryWrite(default)` 写入一个 0，`objectRing.TryWrite(array)` 写入数组对象本身。
要将数组里的元素逐个入队，应调用 `TryWriteBatch(array.AsSpan())` 或 `TryWriteAll(array.AsSpan())`。

## API 语义

| API | 所属端 | 行为 |
| --- | --- | --- |
| `TryWrite(in T)` | 生产者 | 满时返回 false，保留已有数据 |
| `TryWriteBatch(ReadOnlySpan<T>)` | 生产者 | 提交能容纳的前缀，返回元素数；允许源长于容量 |
| `TryWriteAll(ReadOnlySpan<T>)` | 生产者 | 长度超过容量抛参数异常；其余空间不足则整批拒绝 |
| `TryRead(out T)` | 消费者 | 移除队首，空时返回 false 和 default |
| `TryRead(Span<T>)` | 消费者 | 读取可用前缀，返回元素数 |
| `TryReadExact(Span<T>)` | 消费者 | 长度超过容量抛参数异常；其余数据不足时队列和目标均不改动 |
| `TryPeek(out T)` | 消费者 | 查看队首，不移除，不增加读取量 |
| `Discard(int)` | 消费者 | 主动丢弃可用前缀，计入 TotalDiscarded |
| `Count/FreeCount/IsEmpty/IsFull` | 监控或任一端 | 近似状态，不能代替 Try 方法返回值 |
| `TryGetStatistics(out RingBufferStatistics)` | 监控或任一端 | 维护中或采样跨维护时返回 false；成功仍为近似统计 |
| `GetStatistics()` | 监控或任一端 | 同上；无法取得未跨维护样本时抛 InvalidOperationException |
| `Clear()` | 停止并同步两端后 | 校验停止状态；未读元素计入 TotalDiscarded，保留其他统计 |
| `ResetStatistics()` | 停止并同步两端后 | 校验停止状态；归零统计，保留队列数据，高水位设为当前占用 |

空批量写入/读取返回 0；空整批写入/精确读取返回 true；非正数 Discard 不操作。
满载判断可能基于滞后的消费者序号，所以 false 也可能表示本次未观察到刚释放的空间。

`RejectedWrites` 是拒绝的**元素尝试数**，不是拒绝调用数，也不是唯一丢样数。
对同一后缀不断重试会重复增加此计数。唯一缺样应结合设备序号、适配层统计和业务确认判断。
`TotalRead` 仅表示交付给消费者，不证明下游计算或存储成功。
`HighWatermark` 是保守估计，并发时可能高估占用。
`RingBufferStatistics` 的 `Usage` 和 `PeakUsage` 分别由 `Count/Capacity` 与 `HighWatermark/Capacity` 派生，范围 0 到 1，同样是近似值。

各 long 统计字段在 x86/x64 均原子读写，不会拼出半旧半新的单字段数值；多字段仍非一致快照。
监控建议一次调用 `TryGetStatistics`，仅在成功时从返回值的 `FreeCount/IsEmpty/IsFull/Usage/PeakUsage` 派生显示。
在计数未溢出、没有并发维护时，`TotalRead + TotalDiscarded <= InitialCount + TotalWritten`；
不能据此把 `Count` 当作同一时刻的精确积压。计数器采用 unchecked 累计，长期监控须处理回绕。

从空队列、零统计开始，在停止并同步两端且没有发生计数器溢出时：

```text
TotalWritten = TotalRead + TotalDiscarded + Count
```

非空 `ResetStatistics` 不把历史数据算入新的 TotalWritten，而是保存为 `InitialCount`。
停止并同步两端、且计数未溢出时，可以使用完整等式：

```text
InitialCount + TotalWritten = TotalRead + TotalDiscarded + Count
```

`Generation` 在每次成功的 `Clear` 或 `ResetStatistics` 后递增，监控跨代时应重新建立基线。
两端必须在维护前停止并同步，维护之间也应串行安排；已有维护时新的维护调用会报错。
监控可以继续调用 `TryGetStatistics`，维护中或跨维护的样本返回 false，输出 default，调用方应跳过该样本。
原 `GetStatistics` 在同样场景抛 `InvalidOperationException`，不会无限重试，也不会返回混合代次的数据。
这个协议只保护采样边界，不使数据读写与 Clear/ResetStatistics 并发安全，不把正常运行统计变成原子快照。
如需新会话，先 Clear，再 ResetStatistics；这是两次维护，监控可以观察到它们之间的合法中间代次。

```csharp
if (ring.TryGetStatistics(out var statistics))
{
    // 按 Generation 判断是否重建基线，然后使用 statistics 中的监控值。
}
// false：丢弃这次样本，等待下一次正常监控周期；不要拿 default 当成零计数。
```

Clear 和 ResetStatistics 在修改数据、累计统计与 Generation 前检查完整 long 差值、索引范围和对应关系；
无效时抛 `InvalidOperationException`，保留原数据及账目。它们不负责修复任意损坏状态。
所有非空读写、Peek、正数 Discard 每次读取对端已发布序号，再验证本次观察到的差值；
已移除跳过读取的对端序号缓存。读端遇到非法差值不交付数据，写端按拒绝处理。
空请求仍无操作；这些检查不能覆盖同端重入或并发维护的所有交错，调用者仍须遵守所有权契约。

## 整批等待与结束排空

整批长度合法也不代表重试一定取得进展。容量 4、已有 2 个元素时，整批写 3 和精确读 3 都会失败；
双方都只重试原请求就会一直相互等待。可让一端回退到部分写入/部分读取，或者配置容量至少
`最大写批次 + 最大精确读批次 - 1` 来排除这类占用条件。容量计算建议使用 long，避免加法溢出。
重试还须检查取消/结束状态；生产结束后不足一个精确读取批次的尾部，应使用 `TryRead(Span<T>)` 排空。
不要用 Count 预判后再当作读写许可。相关回退示例由 `RegressionTests.BatchProgressAndExpiry` 验证。

## 相对原实现的 API 变更

- 原 `TryWrite(ReadOnlySpan<T>)` 已移除，批量调用统一改为 `TryWriteBatch`；移除重载才可消除 null/default 的歧义。
  **尤其检查 T 为 object 或其他能接收数组的类型：旧的 `TryWrite(array)` 现在是单点写数组对象，可能仍能编译。**
- 超容量的 TryWriteAll/TryReadExact 现在抛 `ArgumentOutOfRangeException`，且不修改目标、队列或统计。
  这是无效配置，不计 RejectedWrites；容量以内因空间不足而拒绝仍按元素尝试数计数。
- TryRead/TryPeek 新增 `[MaybeNullWhen(false)]`，调用方在失败分支使用结果时可能新增正确的可空告警。
- RingBufferStatistics 保留原七个构造/解构参数，新增 init 属性 Generation、InitialCount；record 相等性包含这两项。
  同一组累计值在不同维护代次可能不再相等。调用方与序列化消费者需重新构建并检查新增字段。
- 监控可能跨维护时，改用 `TryGetStatistics` 并处理 false；原 GetStatistics 新增上述维护冲突异常。
  Clear/ResetStatistics 新增停止状态校验异常；失败时 Generation 不增加。

## 采集场景边界

每个实例必须只有一个生产者和一个消费者，禁止并发或重入。设备驱动有多个回调线程时，
先串行化回调或使用符合并发模型的其他队列；不能仅凭“一台设备”认定满足 SPSC。
同一队列不能同时供计算和存储两个线程读取；需要在外层分发到各自队列。

队列的划分按采集流决定：独立通道可以各自使用一个队列，同步多通道可用一个队列承载完整采样帧。
本库不强制“一设备一个”或“一物理通道一个”。同步、路由、唤醒、断线、完成通知、
持久化及溢出后的业务行为均在外层实现。

元素复制是浅复制。队列里的数组、对象、租用内存仍受所有权约束：在消费者完成使用前，
生产者不能修改或归还其背后的内存；清空槽位不会自动归还池化对象或调用 Dispose。
对于小型采样值，优先使用不含引用的不可变值类型。

建议容量至少覆盖峰值元素率、最大消费者停顿、驱动批量突发和余量；同步帧队列的元素率应按帧率计算。
总内存预算必须汇总所有队列。持续生产快于消费时，任何有限容量最终都会满。
固定内存、无锁及零主动分配支持软实时，但不保证操作系统调度或 GC 暂停的最坏延迟。

小容量会增加两端交接频率，批量操作有固定开销；容量和批次应按实际载荷、延迟需求及目标硬件测量，
不规定“至少 64 个元素”或“批量至少 8”的通用性能门槛。状态对象有实际前后填充，但不保证缓存行对齐。

单个实例不广播；外层可将同一不可变载荷引用分发给多个 SPSC 队列，并按设备序号分别审计拒绝。
完成、唤醒和停机排空可由外层同步协议提供；本库不自带这些协议。同步停用不要求销毁线程。
按非递减时间戳丢弃过期前缀时，消费者可循环 Peek 检查后 Discard(1)，遇到不过期元素即停止；
不支持在队列内部任意删除乱序时间戳元素。写入结果可直接驱动外层溢出告警，避免在队列内部回调导致重入。

项目建立、审查修复和设计演进见 [开发路线](docs/ROADMAP.MD)。当前的维护与监控设计见
[维护采样完善过程](docs/ROADMAP.MD#maintenance-hardening)，后续接入与平台验证见 [下一阶段](docs/ROADMAP.MD#next-steps)。
