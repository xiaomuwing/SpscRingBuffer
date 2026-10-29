# 此基础设施项目的工作约定

- 该解决方案位于边缘计算项目的基础设施目录；相关代码和说明保存在此目录内。
- 使用 .NET 10 / 稳定 C# 14，公共命名空间为 EdgeCompute.Infrastructure.Buffers。
- 保持类库无第三方依赖；测试项目是控制台程序，不是 xUnit/NUnit 项目。
- 所有写方法单生产者独占；所有读、Peek、Discard 方法单消费者独占，禁止同端重入。
- 不允许以未经验证的覆盖旧数据、并发 Clear 或并发统计重置改变线程所有权规则。
- Count、FreeCount、统计为近似监控值，业务操作以 Try 方法返回值为准。
- RejectedWrites 统计拒绝元素尝试数，TotalRead 统计交付量，TotalDiscarded 统计主动丢弃量。
- 序号差值和递增必须支持 unchecked 模运算；引用槽位应在读端序号发布前清理。
- 修改公共 API 或统计含义时同步更新 readme.md、设计说明及相应测试。
- 验证顺序：Release 构建 → 控制台全套测试 → 对并发改动补充重复/长测。
- 命令：dotnet build SpscRingBuffer.slnx -c Release
- 命令：dotnet run --project SpscRingBuffer.Tests -c Release --no-build
- 不将控制台测试耗时声明为硬实时、工业现场或跨平台性能保证。
