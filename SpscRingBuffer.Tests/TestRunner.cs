using System.Diagnostics;
using System.Globalization;

namespace EdgeCompute.Infrastructure.Buffers.Tests;

internal sealed record TestOptions(int Items = 1_000_000, int Repeat = 1, int StressSeconds = 0);

internal static class TestRunner
{
    public static int Run(string[] args)
    {
        if (args.Contains("--help") || args.Contains("-h"))
        {
            Console.WriteLine("SpscRingBuffer .NET 10 控制台测试");
            Console.WriteLine("--items N           每个常规并发用例传输数量，默认 1000000，范围 1..10000000");
            Console.WriteLine("--repeat N          完整测试重复次数，默认 1，范围 1..10000");
            Console.WriteLine("--stress-seconds N  完整测试后额外混合并发长测秒数，默认 0，范围 0..86400");
            Console.WriteLine("--help / -h         显示帮助");
            Console.WriteLine("退出码：0=全部通过；1=测试失败；2=参数错误。");
            return 0;
        }

        TestOptions options;
        try
        {
            options = Parse(args);
        }
        catch (ArgumentException error)
        {
            Console.Error.WriteLine($"参数错误：{error.Message}");
            return 2;
        }

        (string Name, Action Run)[] cases =
        [
            ("构造参数和初始状态", BufferTests.Construction),
            ("单元素 FIFO、Peek 和满载拒绝", BufferTests.SingleAndPeek),
            ("容量为 1 的反复环回", BufferTests.CapacityOne),
            ("部分批量写入与目标前缀", BufferTests.PartialBatch),
            ("整批写入的全部成功或全部拒绝", BufferTests.AtomicBatch),
            ("精确读取失败不改动数据", BufferTests.ExactRead),
            ("空批次与非正丢弃数量", BufferTests.EmptyOperations),
            ("非 2 的幂容量、跨尾部批量读写", BufferTests.BatchWraparound),
            ("读取、拒绝、主动丢弃和清空的统计", BufferTests.Statistics),
            ("非空队列的统计重置", BufferTests.ResetWhileStopped),
            ("引用类型及含引用值类型的槽位释放", BufferTests.ReferenceRelease),
            ("64 位序号极限环回", BufferTests.SequenceRollover),
            ("与标准 Queue 的确定性随机对照", BufferTests.RandomizedOracle),
            ("预热后正常读写的线程分配量", BufferTests.AllocationFree),
            ("消费者暂停时的满载数据策略", BufferTests.ConsumerPause),
            ("单点与批量写入无歧义、可空标注", RegressionTests.UnambiguousWritesAndNullability),
            ("批量可用量、高水位及维护代次", RegressionTests.BatchAndMaintenance),
            ("跨尾部引用清空与真实填充", RegressionTests.StoppedClearAndPadding),
            ("整批等待回退与过期前缀丢弃", RegressionTests.BatchProgressAndExpiry),
            ("非法序号差值防御", RegressionTests.InvalidDifferenceDefense),
            ("跨 32 位进位的统计原子性", RegressionTests.AtomicStatistics),
            ("并发引用载荷、Peek 与 Discard", RegressionTests.ConcurrentReferences),
            ("并发序号、计数与维护代次环回", RegressionTests.ConcurrentRollover),
            ("非法停止状态取消维护且无副作用", MaintenanceTests.InvalidStoppedState),
            ("历史高水位下所有读写入口的非法差值防御", MaintenanceTests.WarmedInvalidState),
            ("维护中采样拒绝、重叠维护及版本回绕", MaintenanceTests.MaintenanceWindow),
            ("跨维护统计不会混合代次", MaintenanceTests.ConcurrentSampling),
            ("双线程容量为 1", () => ConcurrencyTests.Transfer(Math.Min(options.Items, 50_000), 1, TransferMode.Single)),
            ("双线程单元素传输和载荷完整性", () => ConcurrencyTests.Transfer(options.Items, 1023, TransferMode.Single)),
            ("双线程批量传输和载荷完整性", () => ConcurrencyTests.Transfer(options.Items, 257, TransferMode.Batch)),
            ("双线程混合单点、批量、整批和精确读取", () => ConcurrencyTests.Transfer(options.Items, 1023, TransferMode.Mixed))
        ];

        Console.WriteLine($"运行环境：{Environment.Version} / {System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}");
        Console.WriteLine($"测试用例：{cases.Length}，每个常规并发用例：{options.Items:N0} 个元素，重复：{options.Repeat}");
        var total = Stopwatch.StartNew();
        int passed = 0;
        int failed = 0;

        for (int iteration = 1; iteration <= options.Repeat; iteration++)
        {
            Console.WriteLine($"\n第 {iteration}/{options.Repeat} 轮");
            foreach (var test in cases)
            {
                var timer = Stopwatch.StartNew();
                try
                {
                    test.Run();
                    passed++;
                    Console.WriteLine($"[PASS] {test.Name} ({timer.Elapsed.TotalMilliseconds:F1} ms)");
                }
                catch (Exception error)
                {
                    failed++;
                    Console.Error.WriteLine($"[FAIL] {test.Name}\n{error}");
                }
            }
        }

        if (failed == 0 && options.StressSeconds > 0)
        {
            try
            {
                ConcurrencyTests.Stress(options.StressSeconds);
                passed++;
            }
            catch (Exception error)
            {
                failed++;
                Console.Error.WriteLine($"[FAIL] 混合并发长测\n{error}");
            }
        }

        Console.WriteLine($"\n结果：{passed} 通过，{failed} 失败，总耗时 {total.Elapsed.TotalSeconds:F2} 秒。");
        return failed == 0 ? 0 : 1;
    }

    private static TestOptions Parse(string[] args)
    {
        var options = new TestOptions();
        for (int index = 0; index < args.Length; index += 2)
        {
            if (index + 1 >= args.Length || !int.TryParse(args[index + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
                throw new ArgumentException($"{args[index]} 必须跟一个整数。");

            options = args[index] switch
            {
                "--items" when value is >= 1 and <= 10_000_000 => options with { Items = value },
                "--repeat" when value is >= 1 and <= 10_000 => options with { Repeat = value },
                "--stress-seconds" when value is >= 0 and <= 86_400 => options with { StressSeconds = value },
                _ => throw new ArgumentException($"未知参数或数值超出范围：{args[index]} {value}。请使用 --help。")
            };
        }

        return options;
    }
}

internal static class Check
{
    public static void True(bool condition, string message = "预期条件为 true。")
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    public static void False(bool condition, string message = "预期条件为 false。") => True(!condition, message);

    public static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            throw new InvalidOperationException($"预期：{expected}，实际：{actual}。");
    }

    public static void Sequence<T>(ReadOnlySpan<T> expected, ReadOnlySpan<T> actual) where T : IEquatable<T>
    {
        if (!expected.SequenceEqual(actual))
            throw new InvalidOperationException("序列内容或顺序不一致。");
    }

    public static void Throws<T>(Action action) where T : Exception
    {
        try
        {
            action();
        }
        catch (T)
        {
            return;
        }

        throw new InvalidOperationException($"预期抛出 {typeof(T).Name}。");
    }
}
