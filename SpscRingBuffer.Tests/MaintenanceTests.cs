using System.Reflection;

namespace EdgeCompute.Infrastructure.Buffers.Tests;

internal static class MaintenanceTests
{
    private const BindingFlags PrivateInstance = BindingFlags.NonPublic | BindingFlags.Instance;

    public static void InvalidStoppedState()
    {
        // 包含窄化后可能伪装成合法 4 的差值；要求在 long 层面检查。
        foreach (long difference in new[] { -96L, 17L, (1L << 32) + 4, long.MinValue, long.MaxValue })
        {
            var ring = new SpscRingBuffer<object>(16);
            object[] items = [new(), new(), new(), new()];
            Check.True(ring.TryWriteAll(items));
            SetState(ring, "reader", "Sequence", unchecked(4L - difference));
            AssertUnchangedOnFailure(ring);
            // 撤销测试注入，而不是声称 Clear 能修复未知的坏账。
            SetState(ring, "reader", "Sequence", 0L);
            ring.Clear();
            Check.True(Storage(ring).All(value => value is null));
            Check.Equal(4L, ring.GetStatistics().TotalDiscarded);
            Check.Equal(1L, ring.GetStatistics().Generation);
        }

        foreach (int index in new[] { -1, 16, 1 })
        {
            var ring = new SpscRingBuffer<object>(16);
            Check.True(ring.TryWrite(new object()));
            SetState(ring, "reader", "Index", index);
            AssertUnchangedOnFailure(ring);
        }
    }

    private static void AssertUnchangedOnFailure(SpscRingBuffer<object> ring)
    {
        var statistics = ring.GetStatistics();
        object?[] slots = (object?[])Storage(ring).Clone();
        var reader = State(ring, "reader");
        var writer = State(ring, "writer");
        object read = reader.GetType().GetField("Sequence")!.GetValue(reader)!;
        object write = writer.GetType().GetField("Sequence")!.GetValue(writer)!;
        object readIndex = reader.GetType().GetField("Index")!.GetValue(reader)!;
        object writeIndex = writer.GetType().GetField("Index")!.GetValue(writer)!;
        foreach (Action action in new Action[] { ring.Clear, ring.ResetStatistics })
        {
            Check.Throws<InvalidOperationException>(action);
            Check.True(ring.TryGetStatistics(out var after)); // finally 不能遗留“维护中”。
            Check.Equal(statistics, after);
            Check.Equal(read, reader.GetType().GetField("Sequence")!.GetValue(reader));
            Check.Equal(write, writer.GetType().GetField("Sequence")!.GetValue(writer));
            Check.Equal(readIndex, reader.GetType().GetField("Index")!.GetValue(reader));
            Check.Equal(writeIndex, writer.GetType().GetField("Index")!.GetValue(writer));
            for (int i = 0; i < slots.Length; i++) Check.True(ReferenceEquals(slots[i], Storage(ring)[i]));
        }
    }

    public static void WarmedInvalidState()
    {
        foreach (int length in new[] { 1, 8 })
        {
            foreach (bool readerAhead in new[] { true, false })
            {
                // 每个 API 使用独立实例，防止前一调用的刷新或污染掩盖缺陷。
                Func<SpscRingBuffer<int>> create = () =>
                {
                    var ring = new SpscRingBuffer<int>(16);
                    if (readerAhead)
                    {
                        Check.True(ring.TryWriteAll(new int[4]));
                        ring.Clear(); // 保留高水位 4，复现之前的快路径漏洞。
                        SetState(ring, "reader", "Sequence", 100L);
                    }
                    else
                    {
                        Check.True(ring.TryWriteAll(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }));
                        Check.Equal(3, ring.TryRead(new int[3]));
                        SetState(ring, "writer", "Sequence", 0L);
                    }
                    return ring;
                };
                Check.False(create().TryWrite(9));
                Check.Equal(0, create().TryWriteBatch(new int[length]));
                Check.False(create().TryWriteAll(new int[length]));
                Check.False(create().TryRead(out _));
                Check.False(create().TryPeek(out _));
                int[] destination = Enumerable.Repeat(-1, length).ToArray();
                Check.Equal(0, create().TryRead(destination));
                Check.False(create().TryReadExact(destination));
                Check.True(destination.All(value => value == -1));
                Check.Equal(0, create().Discard(length));
            }
        }
    }

    public static void MaintenanceWindow()
    {
        var ring = new SpscRingBuffer<int>(4);
        Check.True(ring.TryWriteAll(new int[] { 1, 2 }));
        var before = ring.GetStatistics();
        var type = ring.GetType();
        // 停止状态下通过反射进入/退出真实标记协议，确定性覆盖维护尚未结束的窗口。
        var begin = type.GetMethod("BeginMaintenance", PrivateInstance)!;
        var end = type.GetMethod("EndMaintenance", PrivateInstance)!;
        long version = (long)begin.Invoke(ring, null)!;
        try
        {
            Check.False(ring.TryGetStatistics(out var rejected));
            Check.Equal(default(RingBufferStatistics), rejected);
            Check.Throws<InvalidOperationException>(() => ring.GetStatistics());
            Check.Throws<InvalidOperationException>(ring.Clear);
            Check.Throws<InvalidOperationException>(ring.ResetStatistics);
        }
        finally { end.Invoke(ring, [version]); }
        Check.Equal(before, ring.GetStatistics());

        foreach (long initialVersion in new[] { long.MaxValue - 1, -2L })
        {
            type.GetField("_maintenanceVersion", PrivateInstance)!.SetValue(ring, initialVersion);
            ring.Clear();
            Check.Equal(unchecked(initialVersion + 2), (long)type.GetField("_maintenanceVersion", PrivateInstance)!.GetValue(ring)!);
            Check.True(ring.TryGetStatistics(out var snapshot));
            Check.Equal(0, snapshot.Count);
        }
    }

    public static void ConcurrentSampling()
    {
        var ring = new SpscRingBuffer<int>(4);
        int[] batch = [1, 2, 3, 4];
        Check.True(ring.TryWriteAll(batch));
        const int rounds = 100_000;
        int done = 0;
        long accepted = 0;
        ConcurrencyTests.RunPair(
            token =>
            {
                try
                {
                    for (int i = 0; i < rounds; i++)
                    {
                        if ((i & 1023) == 0) token.ThrowIfCancellationRequested();
                        // 单线程串行写入与维护，两端没有数据操作跨过维护；仅监控可并发。
                        ring.ResetStatistics(); // 奇数代：期初 4，其他累计量均 0。
                        ring.Clear();           // 偶数代：丢弃 4，写入可能尚未开始。
                        Check.True(ring.TryWriteAll(batch));
                    }
                }
                finally { Volatile.Write(ref done, 1); }
            },
            token =>
            {
                do
                {
                    token.ThrowIfCancellationRequested();
                    if (ring.TryGetStatistics(out var sample))
                    {
                        Validate(sample);
                        accepted++;
                    }
                    else Thread.Yield();
                } while (Volatile.Read(ref done) == 0);
                Validate(ring.GetStatistics());
                accepted++;
            }, () => { });
        Check.True(accepted > 0);
        Check.Equal((long)rounds * 2, ring.GetStatistics().Generation);

        static void Validate(RingBufferStatistics sample)
        {
            Check.Equal(0L, sample.TotalRead);
            Check.Equal(0L, sample.RejectedWrites);
            Check.Equal(4, sample.HighWatermark);
            if (sample.Generation == 0)
            {
                Check.Equal(0, sample.InitialCount);
                Check.Equal(4L, sample.TotalWritten);
                Check.Equal(0L, sample.TotalDiscarded);
                Check.Equal(4, sample.Count);
            }
            else if ((sample.Generation & 1) != 0)
            {
                Check.Equal(4, sample.InitialCount);
                Check.Equal(0L, sample.TotalWritten);
                Check.Equal(0L, sample.TotalDiscarded);
                Check.Equal(4, sample.Count);
            }
            else
            {
                Check.Equal(4, sample.InitialCount);
                Check.Equal(4L, sample.TotalDiscarded);
                Check.True(sample.TotalWritten is 0 or 4);
                Check.True(sample.Count is 0 or 4); // 普通写入的发布窗口仍允许近似采样。
            }
        }
    }

    private static object State<T>(SpscRingBuffer<T> ring, string name) =>
        ring.GetType().GetField(name, PrivateInstance)!.GetValue(ring)!;

    private static void SetState<T>(SpscRingBuffer<T> ring, string state, string field, object value)
    {
        object target = State(ring, state);
        target.GetType().GetField(field)!.SetValue(target, value);
    }

    private static object?[] Storage(SpscRingBuffer<object> ring) =>
        (object?[])ring.GetType().GetField("buffer", PrivateInstance)!.GetValue(ring)!;
}
