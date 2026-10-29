using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace EdgeCompute.Infrastructure.Buffers;

/// <summary>固定容量、无锁、单生产者单消费者（SPSC）的进程内环形缓冲区。</summary>
/// <typeparam name="T">元素类型；无引用值类型更适合高频采集。</typeparam>
/// <remarks>
/// <para>所有写入方法只能由一个生产者调用，所有读取、Peek 和 Discard 方法只能由一个消费者调用。
/// 不允许同一端并发或重入；更换所属线程前必须完成同步交接。</para>
/// <para>满时拒绝新数据，不覆盖旧数据、不等待。批量写入只提交成功的前缀。
/// 不提供广播、消费者唤醒、完成通知、持久化或硬实时保证。</para>
/// <para>构造后正常读写不由本类分配内存，但引用对象的创建及进程其他代码仍可能触发 GC。
/// 元素复制是浅复制，入队后生产者不得修改消费者仍在使用的共享对象。</para>
/// </remarks>
public sealed class SpscRingBuffer<T>
{
    private static readonly bool NeedClear = RuntimeHelpers.IsReferenceOrContainsReferences<T>();

    private readonly T[] buffer;
    private readonly SpscProducerState writer = new();
    private readonly SpscConsumerState reader = new();
    private long generation;
    private int initialCount;
    // 偶数表示未维护，奇数表示维护中；只协调维护与统计，不授权维护和数据操作并发。
    private long _maintenanceVersion;

    /// <summary>创建一次性分配、不扩容的缓冲区。</summary>
    /// <param name="capacity">可存储的元素数量，必须大于零；不要求为 2 的幂。</param>
    /// <exception cref="ArgumentOutOfRangeException">容量小于或等于零。</exception>
    public SpscRingBuffer(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        buffer = new T[capacity];
    }

    /// <summary>可存储的最大元素数量。</summary>
    public int Capacity => buffer.Length;

    /// <summary>范围为 0 到 Capacity 的近似数量；并发读取不是一致性快照。</summary>
    /// <remarks>用于监控，不能作为读写许可或等待条件；实际操作以 Try 方法返回值为准。</remarks>
    public int Count
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            long read = Volatile.Read(ref reader.Sequence);
            long write = Volatile.Read(ref writer.Sequence);
            long count = unchecked(write - read);
            return (int)Math.Clamp(count, 0L, Capacity);
        }
    }

    /// <summary>近似可写数量。</summary>
    public int FreeCount => Capacity - Count;

    /// <summary>根据近似数量判断是否为空。</summary>
    public bool IsEmpty => Count == 0;

    /// <summary>根据近似数量判断是否已满。</summary>
    public bool IsFull => Count == Capacity;

    /// <summary>生产者尝试写入一个元素；满时增加拒绝元素计数并返回 false。</summary>
    /// <param name="item">待写入元素。</param>
    /// <returns>是否成功发布。</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryWrite(in T item)
    {
        long write = writer.Sequence;
        int used = GetUsed(write);
        if (used >= Capacity)
        {
            Volatile.Write(ref writer.Rejected, unchecked(writer.Rejected + 1));
            return false;
        }

        buffer[writer.Index] = item;
        writer.Index = AdvanceIndex(writer.Index, 1);

        // 数据与索引更新先完成，再通过 release 写发布给消费者。
        Volatile.Write(ref writer.Written, unchecked(writer.Written + 1));
        UpdateHighWatermark(used + 1);
        Volatile.Write(ref writer.Sequence, unchecked(write + 1));
        return true;
    }

    /// <summary>生产者批量写入可用空间容纳的前缀，余下元素计入拒绝数量。</summary>
    /// <param name="source">源数据；剩余后缀仍由调用方持有，可自行重试。</param>
    /// <returns>成功发布的前缀长度。</returns>
    public int TryWriteBatch(ReadOnlySpan<T> source)
    {
        if (source.IsEmpty)
            return 0;

        long write = writer.Sequence;
        int used = GetUsed(write);
        int count = Math.Min(source.Length, Capacity - used);
        if (count > 0)
        {
            CopyIn(source[..count]);
            Volatile.Write(ref writer.Written, unchecked(writer.Written + count));
            UpdateHighWatermark(used + count);
            Volatile.Write(ref writer.Sequence, unchecked(write + count));
        }

        Volatile.Write(ref writer.Rejected, unchecked(writer.Rejected + source.Length - count));
        return count;
    }

    /// <summary>生产者尝试提交整个批次；空间不足则全部拒绝且不修改队列。</summary>
    /// <param name="source">整个批次；空批次成功。</param>
    /// <returns>整个批次是否成功发布。</returns>
    /// <exception cref="ArgumentOutOfRangeException">批次长度超过 Capacity；此时不修改数据或统计。</exception>
    /// <remarks>与精确读取配合重试时，仍须由调用方保证进展和取消，不能仅检查批次不超过容量。</remarks>
    public bool TryWriteAll(ReadOnlySpan<T> source)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(source.Length, Capacity, nameof(source));
        if (source.IsEmpty)
            return true;

        long write = writer.Sequence;
        int used = GetUsed(write);
        if (source.Length > Capacity - used)
        {
            Volatile.Write(ref writer.Rejected, unchecked(writer.Rejected + source.Length));
            return false;
        }

        CopyIn(source);
        Volatile.Write(ref writer.Written, unchecked(writer.Written + source.Length));
        UpdateHighWatermark(used + source.Length);
        Volatile.Write(ref writer.Sequence, unchecked(write + source.Length));
        return true;
    }

    /// <summary>消费者尝试读取并移除一个元素。</summary>
    /// <param name="item">读取结果；为空时为 default。</param>
    /// <returns>是否成功读取。</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRead([MaybeNullWhen(false)] out T item)
    {
        long read = reader.Sequence;
        if (GetAvailable(read) == 0)
        {
            item = default;
            return false;
        }

        item = buffer[reader.Index];
        if (NeedClear)
            buffer[reader.Index] = default!;

        reader.Index = AdvanceIndex(reader.Index, 1);
        // 读取和引用清理先完成，再允许生产者复用槽位。
        Volatile.Write(ref reader.Sequence, unchecked(read + 1));
        Volatile.Write(ref reader.Read, unchecked(reader.Read + 1));
        return true;
    }

    /// <summary>消费者读取最多 destination.Length 个元素。</summary>
    /// <param name="destination">目标空间；仅写入返回数量对应的前缀。</param>
    /// <returns>实际读取数量。</returns>
    public int TryRead(Span<T> destination)
    {
        if (destination.IsEmpty)
            return 0;

        long read = reader.Sequence;
        int available = GetAvailable(read);
        int count = Math.Min(destination.Length, available);
        if (count == 0)
            return 0;

        CopyOut(destination[..count]);
        Volatile.Write(ref reader.Sequence, unchecked(read + count));
        Volatile.Write(ref reader.Read, unchecked(reader.Read + count));
        return count;
    }

    /// <summary>消费者精确读取整个目标长度；数据不足则目标和队列均不修改。</summary>
    /// <param name="destination">目标空间；空目标成功。</param>
    /// <returns>是否精确读取成功。</returns>
    /// <exception cref="ArgumentOutOfRangeException">目标长度超过 Capacity；此时目标、队列和统计均不修改。</exception>
    /// <remarks>生产结束时应使用部分读取排空不足目标长度的尾部，不能无限重试精确读取。</remarks>
    public bool TryReadExact(Span<T> destination)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(destination.Length, Capacity, nameof(destination));
        if (destination.IsEmpty)
            return true;

        long read = reader.Sequence;
        int available = GetAvailable(read);
        if (destination.Length > available)
            return false;

        CopyOut(destination);
        Volatile.Write(ref reader.Sequence, unchecked(read + destination.Length));
        Volatile.Write(ref reader.Read, unchecked(reader.Read + destination.Length));
        return true;
    }

    /// <summary>消费者查看队首元素而不移除，不增加读取数量。</summary>
    /// <param name="item">队首元素；为空时为 default。</param>
    /// <returns>是否存在队首元素。</returns>
    public bool TryPeek([MaybeNullWhen(false)] out T item)
    {
        long read = reader.Sequence;
        if (GetAvailable(read) == 0)
        {
            item = default;
            return false;
        }

        item = buffer[reader.Index];
        return true;
    }

    /// <summary>消费者主动丢弃最多 count 个元素，只增加 TotalDiscarded。</summary>
    /// <param name="count">请求丢弃的数量；小于或等于零时不操作。</param>
    /// <returns>实际丢弃数量。</returns>
    public int Discard(int count)
    {
        if (count <= 0)
            return 0;

        long read = reader.Sequence;
        int available = GetAvailable(read);
        int actual = Math.Min(count, available);
        if (actual == 0)
            return 0;

        if (NeedClear)
            ClearRange(reader.Index, actual);

        reader.Index = AdvanceIndex(reader.Index, actual);
        Volatile.Write(ref reader.Sequence, unchecked(read + actual));
        Volatile.Write(ref reader.Discarded, unchecked(reader.Discarded + actual));
        return actual;
    }

    /// <summary>获取近似运行统计；并发时各字段并非同一时刻的原子快照。</summary>
    /// <returns>可供监控使用的统计值。</returns>
    /// <exception cref="InvalidOperationException">采样遇到维护；可改用 TryGetStatistics 并跳过本次采样。</exception>
    public RingBufferStatistics GetStatistics()
    {
        if (!TryGetStatistics(out var statistics))
            throw new InvalidOperationException("统计采样与维护重叠，请使用 TryGetStatistics 跳过该样本。");
        return statistics;
    }

    /// <summary>尝试取得未跨维护的近似统计；遇到维护返回 false，不自旋重试。</summary>
    /// <param name="statistics">成功时为统计值，失败时为 default，不应使用失败结果。</param>
    /// <returns>是否取得未跨维护的样本；正常生产/消费期间各字段仍不是一致快照。</returns>
    public bool TryGetStatistics(out RingBufferStatistics statistics)
    {
        statistics = default;
        long version = Volatile.Read(ref _maintenanceVersion);
        if ((version & 1) != 0)
            return false;

        // 先读取交付/丢弃，再读取在发布序号之前提交的写入量。
        // 无溢出且没有并发维护时，消费量不会领先写入量与期初积压之和；仍非一致快照。
        long read = Volatile.Read(ref reader.Read);
        long discarded = Volatile.Read(ref reader.Discarded);
        long written = Volatile.Read(ref writer.Written);
        var sample = new RingBufferStatistics(Capacity, Count, written, read,
            Volatile.Read(ref writer.Rejected), Volatile.Read(ref writer.HighWatermark), discarded)
        {
            Generation = Volatile.Read(ref generation),
            InitialCount = Volatile.Read(ref initialCount)
        };

        // 防止字段读取越过末次版本检查；仅末尾 acquire 读取不足以约束之前的读取。
        Thread.MemoryBarrier();
        if (version != Volatile.Read(ref _maintenanceVersion))
            return false;

        statistics = sample;
        return true;
    }

    /// <summary>归零计数，并以当前 Count 初始化高水位；必须停止并同步生产者和消费者。</summary>
    /// <remarks>不清空数据。若队列非空，归零前写入的数据仍会进入之后的读取/丢弃统计。
    /// InitialCount 保存当前积压，Generation 增加一次。
    /// 监控可通过 TryGetStatistics 跳过与维护重叠的样本。不得与数据操作并发。</remarks>
    /// <exception cref="InvalidOperationException">已有维护，或停止后的序号差值/索引不合法；不修改数据、累计统计和 Generation。</exception>
    public void ResetStatistics()
    {
        long version = BeginMaintenance();
        try
        {
            int count = GetStoppedCount();
            initialCount = count;
            Volatile.Write(ref writer.Written, 0);
            Volatile.Write(ref writer.Rejected, 0);
            writer.HighWatermark = count;
            Volatile.Write(ref reader.Read, 0);
            Volatile.Write(ref reader.Discarded, 0);
            Volatile.Write(ref generation, unchecked(generation + 1));
        }
        finally
        {
            EndMaintenance(version);
        }
    }

    /// <summary>清空数据并重置内部序号和索引；必须停止并同步生产者和消费者。</summary>
    /// <remarks>被清除的未读元素计入 TotalDiscarded；其余统计和高水位保留。
    /// Generation 增加一次，InitialCount 保留。清空后如需开始新的统计周期，再调用 ResetStatistics。
    /// 监控可通过 TryGetStatistics 跳过与维护重叠的样本。不得与数据操作并发。</remarks>
    /// <exception cref="InvalidOperationException">已有维护，或停止后的序号差值/索引不合法；不修改数据、累计统计和 Generation。</exception>
    public void Clear()
    {
        long version = BeginMaintenance();
        try
        {
            int count = GetStoppedCount();
            if (NeedClear && count > 0)
                ClearRange(reader.Index, count);

            Volatile.Write(ref reader.Discarded, unchecked(reader.Discarded + count));
            writer.Index = 0;
            reader.Index = 0;
            Volatile.Write(ref writer.Sequence, 0);
            Volatile.Write(ref reader.Sequence, 0);
            Volatile.Write(ref generation, unchecked(generation + 1));
        }
        finally
        {
            EndMaintenance(version);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetUsed(long write)
    {
        long used = unchecked(write - Volatile.Read(ref reader.Sequence));
        // 每次检查实际观察到的已发布序号，不能据此支持并发 Clear 或同端重入。
        return used < 0 || used > Capacity ? Capacity : (int)used;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetAvailable(long read)
    {
        long available = unchecked(Volatile.Read(ref writer.Sequence) - read);
        return available < 0 || available > Capacity ? 0 : (int)available;
    }

    private int GetStoppedCount()
    {
        long count = unchecked(writer.Sequence - reader.Sequence);
        // 先校验 long，避免窄化转换把损坏的超大差值伪装成合法长度。
        if (count < 0 || count > Capacity ||
            writer.Index < 0 || writer.Index >= Capacity ||
            reader.Index < 0 || reader.Index >= Capacity ||
            AdvanceIndex(reader.Index, (int)count) != writer.Index)
            throw new InvalidOperationException("停止后的缓冲区序号或索引不一致，维护已取消。");
        return (int)count;
    }

    private long BeginMaintenance()
    {
        long version = Volatile.Read(ref _maintenanceVersion);
        // CAS 只用于拒绝重叠维护；完整屏障先公布奇数，再允许修改字段。
        if ((version & 1) != 0 ||
            Interlocked.CompareExchange(ref _maintenanceVersion, unchecked(version + 1), version) != version)
            throw new InvalidOperationException("缓冲区已有维护操作。");
        return version;
    }

    private void EndMaintenance(long version) =>
        Interlocked.Exchange(ref _maintenanceVersion, unchecked(version + 2));

    // 在减去尾部剩余长度后再相加，避免超大容量时 index + count 的 int 溢出。
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AdvanceIndex(int index, int count)
    {
        int remaining = Capacity - index;
        return count >= remaining ? count - remaining : index + count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateHighWatermark(int count)
    {
        // read 的 acquire 快照可能滞后，所以这是保守的占用高水位，可能高估。
        if (count > writer.HighWatermark)
            writer.HighWatermark = count;
    }

    private void CopyIn(ReadOnlySpan<T> source)
    {
        int index = writer.Index;
        int firstLength = Math.Min(source.Length, Capacity - index);
        source[..firstLength].CopyTo(buffer.AsSpan(index, firstLength));
        if (firstLength < source.Length)
            source[firstLength..].CopyTo(buffer.AsSpan(0, source.Length - firstLength));
        writer.Index = AdvanceIndex(index, source.Length);
    }

    private void CopyOut(Span<T> destination)
    {
        int index = reader.Index;
        int firstLength = Math.Min(destination.Length, Capacity - index);
        buffer.AsSpan(index, firstLength).CopyTo(destination[..firstLength]);
        if (firstLength < destination.Length)
            buffer.AsSpan(0, destination.Length - firstLength).CopyTo(destination[firstLength..]);
        if (NeedClear)
            ClearRange(index, destination.Length);
        reader.Index = AdvanceIndex(index, destination.Length);
    }

    private void ClearRange(int index, int count)
    {
        int firstLength = Math.Min(count, Capacity - index);
        Array.Clear(buffer, index, firstLength);
        if (firstLength < count)
            Array.Clear(buffer, 0, count - firstLength);
    }
}

// 两端各自的索引、序号和计数器放在独立对象内，前后留出空间，降低常见 64-byte
// cache line 下的伪共享风险；不是缓存行对齐、线程调度或延迟保证。
// 显式布局类必须在泛型类外，避免 CLR 不支持泛型类型的 explicit layout。
[StructLayout(LayoutKind.Explicit, Size = 192)]
internal sealed class SpscProducerState
{
    [FieldOffset(64)] public long Sequence;
    [FieldOffset(72)] public long Written;
    [FieldOffset(80)] public long Rejected;
    [FieldOffset(88)] public int Index;
    [FieldOffset(92)] public int HighWatermark;
    // 使用真实字段保留尾部，不能仅依赖类上的 StructLayout.Size。
    [FieldOffset(184)] private readonly long _tailPadding;
}

[StructLayout(LayoutKind.Explicit, Size = 192)]
internal sealed class SpscConsumerState
{
    [FieldOffset(64)] public long Sequence;
    [FieldOffset(72)] public long Read;
    [FieldOffset(80)] public long Discarded;
    [FieldOffset(88)] public int Index;
    [FieldOffset(184)] private readonly long _tailPadding;
}

