using System.Reflection;

namespace EdgeCompute.Infrastructure.Buffers.Tests;

internal static class BufferTests
{
    public static void Construction()
    {
        Check.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer<int>(0));
        Check.Throws<ArgumentOutOfRangeException>(() => new SpscRingBuffer<int>(-1));
        var ring = new SpscRingBuffer<int>(3);
        Check.Equal(3, ring.Capacity);
        Check.Equal(0, ring.Count);
        Check.Equal(3, ring.FreeCount);
        Check.True(ring.IsEmpty);
        Check.False(ring.IsFull);
        Check.False(ring.TryRead(out int item));
        Check.Equal(0, item);
        Check.False(ring.TryPeek(out _));
        Check.Equal(new RingBufferStatistics(3, 0, 0, 0, 0, 0, 0), ring.GetStatistics());
    }

    public static void SingleAndPeek()
    {
        var ring = new SpscRingBuffer<int>(3);
        for (int value = 1; value <= 3; value++)
            Check.True(ring.TryWrite(value));
        Check.True(ring.IsFull);
        Check.Equal(0, ring.FreeCount);
        Check.False(ring.TryWrite(99));
        for (int value = 1; value <= 3; value++)
        {
            Check.True(ring.TryPeek(out int first));
            Check.True(ring.TryPeek(out int second));
            Check.Equal(value, first);
            Check.Equal(first, second);
            Check.True(ring.TryRead(out int actual));
            Check.Equal(value, actual);
        }

        Check.True(ring.IsEmpty);
        Check.Equal(new RingBufferStatistics(3, 0, 3, 3, 1, 3), ring.GetStatistics());
    }

    public static void CapacityOne()
    {
        var ring = new SpscRingBuffer<int>(1);
        for (int value = 0; value < 2048; value++)
        {
            Check.True(ring.TryWrite(value));
            Check.False(ring.TryWrite(-1));
            Check.True(ring.TryRead(out int actual));
            Check.Equal(value, actual);
            Check.False(ring.TryRead(out _));
        }

        Check.Equal(new RingBufferStatistics(1, 0, 2048, 2048, 2048, 1), ring.GetStatistics());
    }

    public static void PartialBatch()
    {
        var ring = new SpscRingBuffer<int>(3);
        Check.True(ring.TryWrite(10));
        int[] source = [20, 30, 40, 50];
        Check.Equal(2, ring.TryWriteBatch(source.AsSpan()));
        Check.Equal(0, ring.TryWriteBatch(source.AsSpan()));
        int[] destination = [-1, -1, -1, -1, -1];
        Check.Equal(3, ring.TryRead(destination.AsSpan()));
        Check.Sequence<int>([10, 20, 30, -1, -1], destination);
        Check.Sequence<int>([20, 30, 40, 50], source);
        Check.Equal(6L, ring.GetStatistics().RejectedWrites);
    }

    public static void AtomicBatch()
    {
        var ring = new SpscRingBuffer<int>(3);
        Check.True(ring.TryWriteAll(new int[] { 1, 2 }.AsSpan()));
        Check.False(ring.TryWriteAll(new int[] { 7, 8 }.AsSpan()));
        Check.Throws<ArgumentOutOfRangeException>(() => ring.TryWriteAll(new int[] { 4, 5, 6, 7 }));
        Check.Equal(2, ring.Count);
        int[] destination = new int[2];
        Check.True(ring.TryReadExact(destination.AsSpan()));
        Check.Sequence<int>([1, 2], destination);
        Check.Equal(new RingBufferStatistics(3, 0, 2, 2, 2, 2), ring.GetStatistics());
    }

    public static void ExactRead()
    {
        var ring = new SpscRingBuffer<int>(3);
        Check.True(ring.TryWriteAll(new int[] { 1, 2 }.AsSpan()));
        int[] destination = [-1, -1, -1];
        Check.False(ring.TryReadExact(destination.AsSpan()));
        Check.Sequence<int>([-1, -1, -1], destination);
        Check.Equal(2, ring.Count);
        Check.True(ring.TryReadExact(destination.AsSpan(0, 2)));
        Check.Sequence<int>([1, 2, -1], destination);
        int[] tooLarge = [-5, -5, -5, -5];
        Check.Throws<ArgumentOutOfRangeException>(() => ring.TryReadExact(tooLarge));
        Check.Sequence<int>([-5, -5, -5, -5], tooLarge);
    }

    public static void EmptyOperations()
    {
        var ring = new SpscRingBuffer<int>(1);
        var before = ring.GetStatistics();
        Check.Equal(0, ring.TryWriteBatch(ReadOnlySpan<int>.Empty));
        Check.True(ring.TryWriteAll(ReadOnlySpan<int>.Empty));
        Check.Equal(0, ring.TryRead(Span<int>.Empty));
        Check.True(ring.TryReadExact(Span<int>.Empty));
        Check.Equal(0, ring.Discard(0));
        Check.Equal(0, ring.Discard(-1));
        Check.Equal(0, ring.Discard(int.MaxValue));
        Check.Equal(before, ring.GetStatistics());
    }

    public static void BatchWraparound()
    {
        var ring = new SpscRingBuffer<int>(7);
        Check.True(ring.TryWriteAll(new int[] { 0, 1, 2, 3, 4, 5 }.AsSpan()));
        int[] destination = new int[5];
        Check.True(ring.TryReadExact(destination.AsSpan()));
        Check.Sequence<int>([0, 1, 2, 3, 4], destination);
        Check.True(ring.TryWriteAll(new int[] { 6, 7, 8, 9, 10, 11 }.AsSpan()));
        int[] all = new int[7];
        Check.Equal(7, ring.TryRead(all.AsSpan()));
        Check.Sequence<int>([5, 6, 7, 8, 9, 10, 11], all);
        for (int round = 0; round < 100; round++)
        {
            Check.Equal(7, ring.TryWriteBatch(all.AsSpan()));
            Check.True(ring.TryReadExact(all.AsSpan()));
            Check.Sequence<int>([5, 6, 7, 8, 9, 10, 11], all);
        }
    }

    public static void Statistics()
    {
        var ring = new SpscRingBuffer<int>(5);
        Check.True(ring.TryWriteAll(new int[] { 1, 2, 3, 4, 5 }.AsSpan()));
        Check.False(ring.TryWrite(6));
        Check.True(ring.TryRead(out int first));
        Check.Equal(1, first);
        Check.Equal(2, ring.Discard(2));
        Check.Equal(new RingBufferStatistics(5, 2, 5, 1, 1, 5, 2), ring.GetStatistics());
        Check.Equal(0.4, ring.GetStatistics().Usage);
        Check.Equal(1.0, ring.GetStatistics().PeakUsage);
        ring.Clear();
        Check.Equal(new RingBufferStatistics(5, 0, 5, 1, 1, 5, 4) { Generation = 1 }, ring.GetStatistics());
        Check.True(ring.TryWrite(42));
        Check.True(ring.TryRead(out int afterClear));
        Check.Equal(42, afterClear);
        Check.Equal(0, ring.Discard(10));
        Check.Equal(new RingBufferStatistics(5, 0, 6, 2, 1, 5, 4) { Generation = 1 }, ring.GetStatistics());
        ring.ResetStatistics();
        Check.Equal(new RingBufferStatistics(5, 0, 0, 0, 0, 0) { Generation = 2 }, ring.GetStatistics());
    }

    public static void ResetWhileStopped()
    {
        var ring = new SpscRingBuffer<int>(3);
        Check.True(ring.TryWriteAll(new int[] { 1, 2 }.AsSpan()));
        ring.ResetStatistics();
        Check.Equal(new RingBufferStatistics(3, 2, 0, 0, 0, 2) { Generation = 1, InitialCount = 2 }, ring.GetStatistics());
        Check.True(ring.TryRead(out int actual));
        Check.Equal(1, actual);
        Check.Equal(1, ring.Discard(1));
        Check.Equal(new RingBufferStatistics(3, 0, 0, 1, 0, 2, 1) { Generation = 1, InitialCount = 2 }, ring.GetStatistics());
    }

    public static void ReferenceRelease()
    {
        var ring = new SpscRingBuffer<object?>(3);
        object marker = new();
        Check.True(ring.TryWrite(marker));
        Check.True(ring.TryRead(out object? actual));
        Check.True(ReferenceEquals(marker, actual));
        Check.True(Storage(ring).All(value => value is null));
        Check.True(ring.TryWrite((object?)null));
        Check.True(ring.TryRead(out actual));
        Check.True(actual is null);
        object?[] items = [new object(), new object(), new object()];
        Check.True(ring.TryWriteAll(items.AsSpan()));
        object?[] destination = new object?[2];
        Check.Equal(2, ring.TryRead(destination.AsSpan()));
        Check.True(ReferenceEquals(items[0], destination[0]));
        Check.True(ReferenceEquals(items[1], destination[1]));
        Check.Equal(1, ring.Discard(3));
        Check.True(Storage(ring).All(value => value is null));
        Check.True(ring.TryWriteAll(items.AsSpan()));
        Check.True(ring.TryReadExact(new object?[3].AsSpan()));
        Check.True(Storage(ring).All(value => value is null));
        Check.True(ring.TryWrite(marker));
        ring.Clear();
        Check.True(Storage(ring).All(value => value is null));

        var structs = new SpscRingBuffer<ReferenceSample>(2);
        ReferenceSample[] samples = [new(marker, 1), new(marker, 2)];
        Check.True(structs.TryWriteAll(samples.AsSpan()));
        ReferenceSample[] read = new ReferenceSample[2];
        Check.True(structs.TryReadExact(read.AsSpan()));
        Check.Equal(samples[1], read[1]);
        Check.True(Storage(structs).All(value => value.Payload is null));
        Check.True(structs.TryWrite(samples[0]));
        Check.True(structs.TryRead(out _));
        Check.True(Storage(structs).All(value => value.Payload is null));
        Check.True(structs.TryWriteAll(samples.AsSpan()));
        Check.Equal(2, structs.Discard(2));
        Check.True(Storage(structs).All(value => value.Payload is null));
        Check.True(structs.TryWrite(samples[0]));
        structs.Clear();
        Check.True(Storage(structs).All(value => value.Payload is null));
    }

    public static void SequenceRollover()
    {
        // 仅在测试中通过反射把空队列序号移到边界；无生产者/消费者运行。
        foreach (long start in new[] { long.MaxValue - 1, -1L })
        {
            var ring = new SpscRingBuffer<int>(4);
            SetSequence(ring, "writer", start);
            SetSequence(ring, "reader", start);
            Check.True(ring.TryWrite(1));
            Check.True(ring.TryWrite(2));
            Check.True(ring.TryPeek(out int peek));
            Check.Equal(1, peek);
            Check.Equal(2, ring.Count);
            Check.True(ring.TryRead(out int first));
            Check.Equal(1, first);
            Check.Equal(1, ring.Discard(1));
            Check.True(ring.IsEmpty);
            Check.True(ring.TryWriteAll(new int[] { 3, 4, 5, 6 }.AsSpan()));
            Check.False(ring.TryWrite(99));
            Check.Equal(4, ring.Count);
            int[] destination = new int[4];
            Check.True(ring.TryReadExact(destination.AsSpan()));
            Check.Sequence<int>([3, 4, 5, 6], destination);
            Check.Equal(4, ring.TryWriteBatch(destination.AsSpan()));
            Check.Equal(4, ring.TryRead(destination.AsSpan()));
            Check.True(ring.IsEmpty);
            Check.False(ring.TryPeek(out _));
            Check.Equal(new RingBufferStatistics(4, 0, 10, 9, 1, 4, 1), ring.GetStatistics());

            var batch = new SpscRingBuffer<int>(4);
            SetSequence(batch, "writer", start);
            SetSequence(batch, "reader", start);
            Check.Equal(4, batch.TryWriteBatch(new int[] { 7, 8, 9, 10 }.AsSpan()));
            Check.Equal(4, batch.Count);
            Check.Equal(4, batch.TryRead(destination.AsSpan()));
            Check.Sequence<int>([7, 8, 9, 10], destination);

            var exact = new SpscRingBuffer<int>(4);
            SetSequence(exact, "writer", start);
            SetSequence(exact, "reader", start);
            Check.True(exact.TryWriteAll(new int[] { 11, 12, 13, 14 }.AsSpan()));
            Check.True(exact.TryReadExact(destination.AsSpan()));
            Check.Sequence<int>([11, 12, 13, 14], destination);
        }
    }

    public static void RandomizedOracle()
    {
        foreach (int capacity in new[] { 1, 2, 3, 7, 16, 257 })
        {
            var random = new Random(20261008 + capacity);
            var ring = new SpscRingBuffer<int>(capacity);
            var expected = new Queue<int>();
            long written = 0, read = 0, rejected = 0, discarded = 0;
            long generation = 0;
            int high = 0;
            int sequence = 0;
            for (int step = 0; step < 15_000; step++)
            {
                int length = random.Next(capacity + 4);
                switch (random.Next(9))
                {
                    case 0:
                        bool canWrite = expected.Count < capacity;
                        Check.Equal(canWrite, ring.TryWrite(sequence));
                        if (canWrite) { expected.Enqueue(sequence); written++; }
                        else rejected++;
                        sequence++;
                        break;
                    case 1:
                    case 2:
                        int[] source = new int[length];
                        for (int index = 0; index < length; index++) source[index] = sequence++;
                        bool all = random.Next(2) == 0;
                        if (all && length > capacity)
                        {
                            Check.Throws<ArgumentOutOfRangeException>(() => ring.TryWriteAll(source));
                            break;
                        }
                        int count = all ? (length <= capacity - expected.Count ? length : 0) : Math.Min(length, capacity - expected.Count);
                        if (all) Check.Equal(length <= capacity - expected.Count, ring.TryWriteAll(source.AsSpan()));
                        else Check.Equal(count, ring.TryWriteBatch(source.AsSpan()));
                        for (int index = 0; index < count; index++) expected.Enqueue(source[index]);
                        written += count;
                        rejected += length - count;
                        break;
                    case 3:
                        bool canRead = expected.Count > 0;
                        Check.Equal(canRead, ring.TryRead(out int item));
                        Check.Equal(canRead ? expected.Dequeue() : 0, item);
                        if (canRead) read++;
                        break;
                    case 4:
                    case 5:
                        int[] destination = Enumerable.Repeat(-1, length).ToArray();
                        bool exact = random.Next(2) == 0;
                        if (exact && length > capacity)
                        {
                            Check.Throws<ArgumentOutOfRangeException>(() => ring.TryReadExact(destination));
                            Check.True(destination.All(value => value == -1));
                            break;
                        }
                        int actual = exact ? (length <= expected.Count ? length : 0) : Math.Min(length, expected.Count);
                        if (exact) Check.Equal(length <= expected.Count, ring.TryReadExact(destination.AsSpan()));
                        else Check.Equal(actual, ring.TryRead(destination.AsSpan()));
                        for (int index = 0; index < actual; index++) Check.Equal(expected.Dequeue(), destination[index]);
                        for (int index = actual; index < length; index++) Check.Equal(-1, destination[index]);
                        read += actual;
                        break;
                    case 6:
                        Check.Equal(expected.Count > 0, ring.TryPeek(out int head));
                        Check.Equal(expected.Count > 0 ? expected.Peek() : 0, head);
                        break;
                    case 7:
                        int remove = Math.Min(length, expected.Count);
                        Check.Equal(remove, ring.Discard(length));
                        for (int index = 0; index < remove; index++) expected.Dequeue();
                        discarded += remove;
                        break;
                    case 8:
                        discarded += expected.Count;
                        expected.Clear();
                        ring.Clear();
                        generation++;
                        break;
                }

                high = Math.Max(high, expected.Count);
                Check.Equal(new RingBufferStatistics(capacity, expected.Count, written, read, rejected, high, discarded) { Generation = generation }, ring.GetStatistics());
                Check.Equal(capacity - expected.Count, ring.FreeCount);
            }
        }
    }

    public static void AllocationFree()
    {
        var ring = new SpscRingBuffer<int>(7);
        Check.True(Exercise(ring, 20_000));
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool valid = Exercise(ring, 100_000);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Check.True(valid);
        Check.Equal(0L, allocated);
    }

    private static bool Exercise(SpscRingBuffer<int> ring, int iterations)
    {
        Span<int> source = stackalloc int[] { 1, 2, 3 };
        Span<int> destination = stackalloc int[3];
        bool valid = true;
        for (int index = 0; index < iterations; index++)
        {
            valid &= ring.TryWrite(index);
            valid &= ring.TryPeek(out int peek) && peek == index;
            valid &= ring.TryRead(out int value) && value == index;
            valid &= ring.TryWriteBatch(source) == 3;
            valid &= ring.TryReadExact(destination);
            valid &= ring.TryWriteAll(source);
            valid &= ring.TryRead(destination) == 3;
            valid &= ring.TryWriteBatch(source) == 3;
            valid &= ring.Discard(3) == 3;
            valid &= ring.GetStatistics().Count == 0;
        }
        return valid;
    }

    public static void ConsumerPause()
    {
        var ring = new SpscRingBuffer<int>(8);
        using var produced = new ManualResetEventSlim(false);
        ConcurrencyTests.RunPair(
            token =>
            {
                try
                {
                    for (int index = 0; index < 100; index++)
                    {
                        token.ThrowIfCancellationRequested();
                        Check.Equal(index < 8, ring.TryWrite(index));
                    }
                }
                finally { produced.Set(); }
            },
            token =>
            {
                produced.Wait(token);
                for (int index = 0; index < 8; index++)
                {
                    Check.True(ring.TryRead(out int actual));
                    Check.Equal(index, actual);
                }
                Check.False(ring.TryRead(out _));
            },
            () => ConcurrencyTests.CheckSnapshot(ring));
        Check.Equal(new RingBufferStatistics(8, 0, 8, 8, 92, 8), ring.GetStatistics());
    }

    private static T[] Storage<T>(SpscRingBuffer<T> ring) =>
        (T[])typeof(SpscRingBuffer<T>).GetField("buffer", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(ring)!;

    private static void SetSequence<T>(SpscRingBuffer<T> ring, string stateName, long value)
    {
        object state = typeof(SpscRingBuffer<T>).GetField(stateName, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(ring)!;
        state.GetType().GetField("Sequence")!.SetValue(state, value);
    }

    private readonly record struct ReferenceSample(object? Payload, int Value);
}

