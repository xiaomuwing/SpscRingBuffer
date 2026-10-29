using System.Collections.Concurrent;
using System.Diagnostics;

namespace EdgeCompute.Infrastructure.Buffers.Tests;

internal enum TransferMode { Single, Batch, Mixed }

internal readonly record struct TestSample(long Sequence, long Timestamp, double Value, ulong Checksum)
{
    public static TestSample Create(long sequence) => new(
        sequence,
        sequence * 17,
        sequence + 0.25,
        unchecked((ulong)sequence * 0x9E3779B97F4A7C15UL) ^ 0xA5A55A5A12345678UL);
}

internal static class ConcurrencyTests
{
    public static void Transfer(int items, int capacity, TransferMode mode)
    {
        var ring = new SpscRingBuffer<TestSample>(capacity);
        RunTransfer(ring, items, mode);
        Check.Equal(items, checked((int)ring.GetStatistics().TotalWritten));
        Check.Equal(items, checked((int)ring.GetStatistics().TotalRead));
        Check.Equal(0L, ring.GetStatistics().TotalDiscarded);
        Check.True(ring.IsEmpty);
        CheckSnapshot(ring);
    }

    private static void RunTransfer(SpscRingBuffer<TestSample> ring, int items, TransferMode mode)
    {
        RunPair(
            token => Produce(ring, items, mode, token),
            token => Consume(ring, items, mode, token),
            () => CheckSnapshot(ring));
    }

    private static void Produce(SpscRingBuffer<TestSample> ring, int items, TransferMode mode, CancellationToken token)
    {
        var random = new Random(1009);
        TestSample[] batch = new TestSample[Math.Min(64, ring.Capacity)];
        int sequence = 0;
        int retries = 0;
        while (sequence < items)
        {
            token.ThrowIfCancellationRequested();
            if (mode == TransferMode.Single || (mode == TransferMode.Mixed && random.Next(3) == 0))
            {
                var sample = TestSample.Create(sequence);
                if (ring.TryWrite(sample)) sequence++;
                else Pause(ref retries, token);
                continue;
            }

            int length = Math.Min(items - sequence, mode == TransferMode.Batch ? batch.Length : random.Next(1, batch.Length + 1));
            for (int index = 0; index < length; index++) batch[index] = TestSample.Create(sequence + index);
            if (mode == TransferMode.Mixed && random.Next(2) == 0)
            {
                while (!ring.TryWriteAll(batch.AsSpan(0, length))) Pause(ref retries, token);
                sequence += length;
            }
            else
            {
                int offset = 0;
                while (offset < length)
                {
                    int written = ring.TryWriteBatch(batch.AsSpan(offset, length - offset));
                    if (written == 0) Pause(ref retries, token);
                    offset += written;
                }
                sequence += length;
            }
        }
    }

    private static void Consume(SpscRingBuffer<TestSample> ring, int items, TransferMode mode, CancellationToken token)
    {
        var random = new Random(2003);
        TestSample[] batch = new TestSample[37];
        int expected = 0;
        int retries = 0;
        while (expected < items)
        {
            token.ThrowIfCancellationRequested();
            if (mode == TransferMode.Single || (mode == TransferMode.Mixed && random.Next(3) == 0))
            {
                if (ring.TryRead(out TestSample item))
                {
                    Verify(item, expected);
                    expected++;
                }
                else Pause(ref retries, token);
                continue;
            }

            int length = Math.Min(items - expected, mode == TransferMode.Batch ? batch.Length : random.Next(1, batch.Length + 1));
            int read = mode == TransferMode.Mixed && random.Next(2) == 0
                ? (ring.TryReadExact(batch.AsSpan(0, length)) ? length : 0)
                : ring.TryRead(batch.AsSpan(0, length));
            if (read == 0) Pause(ref retries, token);
            for (int index = 0; index < read; index++) Verify(batch[index], expected++);
        }
    }

    private static void Verify(TestSample actual, int sequence)
    {
        if (actual != TestSample.Create(sequence))
            throw new InvalidOperationException($"数据缺失、重复、乱序或载荷损坏：预期序号 {sequence}，实际 {actual}。");
    }

    private static void Pause(ref int retries, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if ((++retries & 1023) == 0) Thread.Yield();
        else Thread.SpinWait(4);
    }

    public static void CheckSnapshot<T>(SpscRingBuffer<T> ring)
    {
        var statistics = ring.GetStatistics();
        Check.True(statistics.Count >= 0 && statistics.Count <= ring.Capacity, "并发监控的 Count 越界。");
        Check.True(statistics.HighWatermark >= 0 && statistics.HighWatermark <= ring.Capacity, "高水位越界。");
        Check.True(statistics.Usage is >= 0 and <= 1, "占用比例越界。");
        Check.True(statistics.TotalRead + statistics.TotalDiscarded <= statistics.InitialCount + statistics.TotalWritten,
            "无溢出且无并发维护时，消费量不应领先已提交写入量与期初积压之和。");
        Check.True(ring.FreeCount >= 0 && ring.FreeCount <= ring.Capacity, "可写数量越界。");
    }

    public static void RunPair(Action<CancellationToken> producer, Action<CancellationToken> consumer, Action monitor)
    {
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var start = new ManualResetEventSlim(false);
        var errors = new ConcurrentQueue<Exception>();

        Thread CreateThread(string name, Action<CancellationToken> action) => new(() =>
        {
            try
            {
                start.Wait(cancel.Token);
                action(cancel.Token);
            }
            catch (Exception error)
            {
                errors.Enqueue(error);
                cancel.Cancel();
            }
        }) { IsBackground = true, Name = name };

        Thread writer = CreateThread("SPSC-Test-Producer", producer);
        Thread reader = CreateThread("SPSC-Test-Consumer", consumer);
        writer.Start();
        reader.Start();
        start.Set();
        try
        {
            while (writer.IsAlive || reader.IsAlive)
            {
                monitor();
                writer.Join(10);
                reader.Join(10);
            }
        }
        finally
        {
            cancel.Cancel();
            writer.Join();
            reader.Join();
        }

        if (!errors.IsEmpty)
            throw new AggregateException("双线程测试失败或超过单轮 60 秒超时。", errors);
    }

    public static void Stress(int seconds)
    {
        // 同一个队列跨多个线程对使用；每轮 Join 后再同步更换所有者，保留索引和统计。
        var ring = new SpscRingBuffer<TestSample>(1023);
        var timer = Stopwatch.StartNew();
        int rounds = 0;
        long nextReport = 5;
        Console.WriteLine($"\n开始混合并发长测，持续至少 {seconds} 秒，每轮 100000 个元素。");
        while (timer.Elapsed.TotalSeconds < seconds)
        {
            RunTransfer(ring, 100_000, TransferMode.Mixed);
            rounds++;
            Check.Equal((long)rounds * 100_000, ring.GetStatistics().TotalWritten);
            Check.Equal((long)rounds * 100_000, ring.GetStatistics().TotalRead);
            Check.Equal(0, ring.Count);
            if (timer.Elapsed.TotalSeconds >= nextReport)
            {
                Console.WriteLine($"[STRESS] {timer.Elapsed.TotalSeconds:F1} 秒，已完整传输 {(long)rounds * 100_000:N0} 个元素。");
                nextReport += 5;
            }
        }
        Console.WriteLine($"[PASS] 混合并发长测：{rounds} 轮，{(long)rounds * 100_000:N0} 个元素，{timer.Elapsed.TotalSeconds:F1} 秒。");
    }
}

