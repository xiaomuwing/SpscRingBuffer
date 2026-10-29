namespace EdgeCompute.Infrastructure.Buffers;

/// <summary>环形缓冲区的近似运行统计；并发时各字段不构成一致性快照。</summary>
/// <param name="Capacity">元素容量。</param>
/// <param name="Count">近似未读元素数量。</param>
/// <param name="TotalWritten">成功发布的元素数量。</param>
/// <param name="TotalRead">通过读取接口交付的元素数量，不代表下游已处理成功。</param>
/// <param name="RejectedWrites">被拒绝的元素尝试数量；重复重试可能重复计数，不等于唯一丢样数。</param>
/// <param name="HighWatermark">基于生产者观察的保守占用高水位，并发时可能高估。</param>
/// <param name="TotalDiscarded">通过 Discard 或停止后 Clear 丢弃的元素数量。</param>
public readonly record struct RingBufferStatistics(
    int Capacity,
    int Count,
    long TotalWritten,
    long TotalRead,
    long RejectedWrites,
    int HighWatermark,
    long TotalDiscarded = 0)
{
    /// <summary>成功维护代次；Clear 和 ResetStatistics 各递增一次（unchecked 环回）。不授权维护与数据操作并发。</summary>
    public long Generation { get; init; }

    /// <summary>本统计周期开始时的未读数量；ResetStatistics 保留的数据计入此值。</summary>
    public int InitialCount { get; init; }

    /// <summary>由本次 Count 派生的近似剩余容量。</summary>
    public int FreeCount => Capacity - Count;

    /// <summary>由本次 Count 派生的近似空状态。</summary>
    public bool IsEmpty => Count == 0;

    /// <summary>由本次 Count 派生的近似满状态。</summary>
    public bool IsFull => Count == Capacity;

    /// <summary>近似占用比例，范围为 0 到 1。</summary>
    public double Usage => Capacity == 0 ? 0 : (double)Count / Capacity;

    /// <summary>保守峰值占用比例，范围为 0 到 1。</summary>
    public double PeakUsage => Capacity == 0 ? 0 : (double)HighWatermark / Capacity;
}
