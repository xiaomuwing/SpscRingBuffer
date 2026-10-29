using System.Reflection;
using System.Runtime.InteropServices;

namespace EdgeCompute.Infrastructure.Buffers.Tests;

internal static class RegressionTests
{
    public static void UnambiguousWritesAndNullability()
    {
        var references = new SpscRingBuffer<object?>(4);
        Check.True(references.TryWrite(null));
        Check.True(references.TryRead(out var value));
        Check.True(value is null);
        object[] array = [new(), new()];
        Check.True(references.TryWrite(array));
        Check.True(references.TryRead(out value));
        Check.True(ReferenceEquals(array, value));
        Check.Equal(2, references.TryWriteBatch(array));
        Check.Equal(2, references.Count);
        var integers = new SpscRingBuffer<int>(1);
        Check.True(integers.TryWrite(default));
        Check.True(integers.TryRead(out int number));
        Check.Equal(0, number);

        var strings = new SpscRingBuffer<string>(1);
        Check.False(strings.TryRead(out var missing));
        Check.True(missing is null);
        Check.True(strings.TryWrite("ok"));
        // 成功分支不需要 !；编译时验证 MaybeNullWhen 的流分析契约。
        if (strings.TryPeek(out var peek)) Check.Equal(2, peek.Length);
        else throw new InvalidOperationException();
        if (strings.TryRead(out var text)) Check.Equal(2, text.Length);
        else throw new InvalidOperationException();
        foreach (string method in new[] { "TryRead", "TryPeek" })
        {
            var parameter = typeof(SpscRingBuffer<string>).GetMethod(method, [typeof(string).MakeByRefType()])!.GetParameters()[0];
            Check.Equal(false, parameter.GetCustomAttribute<System.Diagnostics.CodeAnalysis.MaybeNullWhenAttribute>()!.ReturnValue);
        }
    }

    public static void BatchAndMaintenance()
    {
        foreach (bool all in new[] { false, true })
        {
            var ring = new SpscRingBuffer<int>(16);
            Check.Equal(8, ring.TryWriteBatch(new int[8]));
            Check.Equal(8, ring.Discard(8));
            Check.Equal(7, ring.TryWriteBatch(new int[7]));
            Check.Equal(7, ring.Discard(7));
            if (all) Check.True(ring.TryWriteAll(new int[4]));
            else Check.Equal(4, ring.TryWriteBatch(new int[4]));
            Check.Equal(8, ring.GetStatistics().HighWatermark);
            ring.ResetStatistics();
            Check.Equal(4, ring.GetStatistics().InitialCount);
            Check.True(ring.TryReadExact(new int[4]));
            ring.Clear();
            Check.True(ring.TryWrite(42));
            Check.True(ring.TryPeek(out int peek));
            Check.Equal(42, peek);
            Check.True(ring.TryRead(out int actual));
            Check.Equal(42, actual);
            var s = ring.GetStatistics();
            Check.Equal(2L, s.Generation);
            Check.Equal(s.InitialCount + s.TotalWritten, s.TotalRead + s.TotalDiscarded + s.Count);
            Check.Equal(16, s.FreeCount);
            Check.True(s.IsEmpty);
            Check.False(s.IsFull);
        }

        var partial = new SpscRingBuffer<int>(8);
        Check.Equal(8, partial.TryWriteBatch(new int[8]));
        Check.Equal(8, partial.Discard(8));
        Check.Equal(7, partial.TryWriteBatch(new int[7]));
        Check.Equal(7, partial.Discard(7));
        Check.Equal(4, partial.TryWriteBatch(new int[4])); // 已释放全部空间，不能无谓少写。
        Check.Equal(4, partial.TryRead(new int[4]));
        Check.True(partial.TryWriteAll(new int[8]));
        Check.Equal(8, partial.TryRead(new int[8])); // 本次应观察到完整发布的批次。
    }

    public static void StoppedClearAndPadding()
    {
        var ring = new SpscRingBuffer<object>(7);
        object[] source = Enumerable.Range(0, 7).Select(_ => new object()).ToArray();
        Check.True(ring.TryWriteAll(source.AsSpan(0, 6)));
        Check.Equal(5, ring.Discard(5));
        Check.True(ring.TryWriteAll(source.AsSpan(0, 4))); // 占用横跨数组尾部。
        ring.Clear();
        object?[] storage = (object?[])typeof(SpscRingBuffer<object>).GetField("buffer", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(ring)!;
        Check.True(storage.All(value => value is null));
        Check.Equal(10L, ring.GetStatistics().TotalDiscarded);
        Check.True(ring.TryWrite(source[0]));
        Check.True(ring.TryRead(out var item));
        Check.True(ReferenceEquals(source[0], item));

        foreach (string name in new[] { "SpscProducerState", "SpscConsumerState" })
        {
            Type type = typeof(SpscRingBuffer<>).Assembly.GetType("EdgeCompute.Infrastructure.Buffers." + name)!;
            Check.True(Marshal.SizeOf(type) >= 192, "状态对象缺少真实尾部字段。");
        }
    }

    public static void BatchProgressAndExpiry()
    {
        var ring = new SpscRingBuffer<int>(4);
        Check.True(ring.TryWriteAll(new int[] { 1, 2 }));
        int[] tail = [3, 4, 5];
        Check.False(ring.TryWriteAll(tail));
        Check.False(ring.TryReadExact(new int[3]));
        int[] drained = new int[3];
        Check.Equal(2, ring.TryRead(drained)); // 一端回退部分读取即可解除相互等待。
        Check.Sequence<int>([1, 2, 0], drained);
        Check.True(ring.TryWriteAll(tail));
        Check.True(ring.TryReadExact(drained));
        Check.Sequence<int>(tail, drained);
        Check.True(ring.TryWriteAll(new int[] { 10, 20, 30 }));
        while (ring.TryPeek(out int timestamp) && timestamp < 25) Check.Equal(1, ring.Discard(1));
        Check.True(ring.TryPeek(out int kept));
        Check.Equal(30, kept);
        Check.Equal(2L, ring.GetStatistics().TotalDiscarded);
    }

    public static void InvalidDifferenceDefense()
    {
        var ring = new SpscRingBuffer<int>(16);
        object reader = typeof(SpscRingBuffer<int>).GetField("reader", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(ring)!;
        reader.GetType().GetField("Sequence")!.SetValue(reader, 100L);
        Check.False(ring.TryRead(out _));
        Check.False(ring.TryPeek(out _));
        Check.Equal(0, ring.TryRead(new int[8]));
        Check.False(ring.TryReadExact(new int[8]));
        Check.Equal(0, ring.Discard(5));
        Check.False(ring.TryWrite(1));
        Check.Equal(0, ring.TryWriteBatch(new int[8]));
        Check.False(ring.TryWriteAll(new int[8]));
        Check.Equal(100L, reader.GetType().GetField("Sequence")!.GetValue(reader));
    }

    public static void AtomicStatistics()
    {
        const int delta = 16_777_217;
        const int attempts = 2_000_000;
        var ring = new SpscRingBuffer<byte>(1);
        Check.True(ring.TryWrite((byte)1));
        byte[] rejected = new byte[delta];
        long observations = 0;
        ConcurrencyTests.RunPair(
            token =>
            {
                for (int i = 0; i < attempts; i++)
                {
                    if ((i & 4095) == 0) token.ThrowIfCancellationRequested();
                    Check.Equal(0, ring.TryWriteBatch(rejected));
                }
            },
            token =>
            {
                long previous = 0;
                while (previous < (long)attempts * delta)
                {
                    token.ThrowIfCancellationRequested();
                    long current = ring.GetStatistics().RejectedWrites;
                    Check.Equal(0L, current % delta);
                    Check.True(current >= previous);
                    previous = current;
                    observations++;
                }
            }, () => { });
        Check.True(observations > 0);
        Check.Equal((long)attempts * delta, ring.GetStatistics().RejectedWrites);
    }

    public static void ConcurrentReferences()
    {
        var ring = new SpscRingBuffer<Payload>(7);
        const int items = 100_000;
        ConcurrencyTests.RunPair(
            token =>
            {
                for (int i = 0; i < items; i++)
                {
                    var value = new Payload(i, ~i);
                    while (!ring.TryWrite(value)) { token.ThrowIfCancellationRequested(); Thread.Yield(); }
                }
            },
            token =>
            {
                for (int i = 0; i < items; i++)
                {
                    Payload? peek;
                    while (!ring.TryPeek(out peek)) { token.ThrowIfCancellationRequested(); Thread.Yield(); }
                    Check.Equal(i, peek.Sequence);
                    Check.Equal(~i, peek.Checksum);
                    if (i % 3 == 0) Check.Equal(1, ring.Discard(1));
                    else
                    {
                        Check.True(ring.TryRead(out var value));
                        Check.True(ReferenceEquals(peek, value));
                    }
                }
            }, () => ConcurrencyTests.CheckSnapshot(ring));
        var s = ring.GetStatistics();
        Check.Equal((long)items, s.TotalRead + s.TotalDiscarded);
        Check.Equal(0, s.Count);
        var storage = (Payload?[])typeof(SpscRingBuffer<Payload>).GetField("buffer", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(ring)!;
        Check.True(storage.All(item => item is null));
    }

    public static void ConcurrentRollover()
    {
        foreach (long start in new[] { long.MaxValue - 32, -32L })
        {
            var ring = new SpscRingBuffer<int>(7);
            object State(string name) => typeof(SpscRingBuffer<int>).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(ring)!;
            void Set(object state, string name, long value) => state.GetType().GetField(name)!.SetValue(state, value);
            var writer = State("writer");
            var reader = State("reader");
            Set(writer, "Sequence", start);
            Set(reader, "Sequence", start);
            Set(writer, "Written", start);
            Set(reader, "Read", start);
            const int items = 20_000;
            ConcurrencyTests.RunPair(
                token =>
                {
                    for (int i = 0; i < items; i++)
                        while (!ring.TryWrite(i)) { token.ThrowIfCancellationRequested(); Thread.Yield(); }
                },
                token =>
                {
                    for (int i = 0; i < items; i++)
                    {
                        int actual;
                        while (!ring.TryRead(out actual)) { token.ThrowIfCancellationRequested(); Thread.Yield(); }
                        Check.Equal(i, actual);
                    }
                }, () => Check.True(ring.Count is >= 0 and <= 7));
            Check.Equal(unchecked(start + items), ring.GetStatistics().TotalWritten);
            Check.Equal(unchecked(start + items), ring.GetStatistics().TotalRead);
            Check.Equal(0, ring.Count);
            typeof(SpscRingBuffer<int>).GetField("generation", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(ring, long.MaxValue);
            ring.Clear();
            Check.Equal(long.MinValue, ring.GetStatistics().Generation);
            ring.ResetStatistics();
            Check.Equal(long.MinValue + 1, ring.GetStatistics().Generation);
            Check.True(ring.TryWrite(123));
            Check.True(ring.TryRead(out int after));
            Check.Equal(123, after);
        }
    }

    private sealed record Payload(int Sequence, int Checksum);
}
