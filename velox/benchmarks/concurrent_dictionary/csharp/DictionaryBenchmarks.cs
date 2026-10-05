using System.Collections.Concurrent;
using System.Globalization;
using BenchmarkDotNet.Attributes;

namespace DictionaryBench;

// Reference identity is intentional, matching MemoryCache entry comparisons.
public sealed class Entry(long id) { public long Id { get; } = id; }
public enum AccessPattern { Uniform, Hot }
public enum Operation { Hit, Miss, Mixed95, Mixed50, ConditionalUpdate, Churn }

public static class Data<TKey, TValue> where TKey : notnull
{
    public static TKey Key(int id) => (TKey)(typeof(TKey) == typeof(long)
        ? (object)(long)id : id.ToString("D16", CultureInfo.InvariantCulture));
    public static TValue Value(long id) => (TValue)(typeof(TValue) == typeof(long)
        ? (object)id : new Entry(id));
}

[MemoryDiagnoser]
[GenericTypeArguments(typeof(long), typeof(long))]
[GenericTypeArguments(typeof(string), typeof(long))]
[GenericTypeArguments(typeof(long), typeof(Entry))]
[GenericTypeArguments(typeof(string), typeof(Entry))]
public class DictionaryBenchmarks<TKey, TValue> where TKey : notnull
{
    // Aggregate work is constant. Reported ns/op is inverse throughput, not latency.
    public const int BatchSize = 262144;
    [Params(1024, 65536)] public int Count { get; set; }
    [ParamsSource(nameof(ThreadCounts))] public int Threads { get; set; }
    [Params(AccessPattern.Uniform, AccessPattern.Hot)] public AccessPattern Pattern { get; set; }
    public IEnumerable<int> ThreadCounts => ReadThreadCounts();
    public static int[] ReadThreadCounts()
    {
        var counts = (Environment.GetEnvironmentVariable("CD_BENCH_THREADS") ?? "1,4")
            .Split(',').Select(int.Parse).ToArray();
        if (counts.Any(n => n <= 0 || n > BatchSize || BatchSize % n != 0))
            throw new ArgumentException("CD_BENCH_THREADS must contain positive divisors of 262144.");
        return counts.Distinct().ToArray();
    }
    private ConcurrentDictionary<TKey, TValue> map = null!;
    private TKey[][] keys = null!;
    private TKey[][] misses = null!;
    private TValue[] values = null!;
    private Thread[] workers = [];
    private Barrier start = null!;
    private Barrier finish = null!;
    private bool stopping;
    private Operation operation;
    private long[] successes = null!;
    private Exception?[] errors = null!;
    public int EntryCount => map.Count;

    [GlobalSetup]
    public void Setup()
    {
        if (Count <= 0 || Threads <= 0 || BatchSize % Threads != 0)
            throw new ArgumentException("Invalid entry or thread count.");
        map = new();
        values = Enumerable.Range(0, 256).Select(i => Data<TKey, TValue>.Value(i)).ToArray();
        for (int i = 0; i < Count; i++) map.TryAdd(Data<TKey, TValue>.Key(i), values[i % 256]);
        keys = new TKey[Threads][];
        misses = new TKey[Threads][];
        successes = new long[Threads];
        errors = new Exception?[Threads];
        for (int worker = 0; worker < Threads; worker++)
        {
            keys[worker] = new TKey[BatchSize / Threads];
            misses[worker] = new TKey[BatchSize / Threads];
            uint state = 0x9e3779b9u ^ (uint)(worker + 1);
            for (int i = 0; i < keys[worker].Length; i++)
            {
                uint r = Next(ref state);
                int range = Pattern == AccessPattern.Hot && r % 100 < 90
                    ? Math.Max(1, Count / 100) : Count;
                int id = (int)(Next(ref state) % (uint)range);
                keys[worker][i] = Data<TKey, TValue>.Key(id);
                misses[worker][i] = Data<TKey, TValue>.Key(Count + id);
            }
        }
        start = new Barrier(Threads + 1);
        finish = new Barrier(Threads + 1);
        stopping = false;
        workers = new Thread[Threads];
        for (int i = 0; i < Threads; i++)
        {
            int worker = i;
            workers[i] = new Thread(() => Worker(worker)) { IsBackground = true };
            workers[i].Start();
        }
    }
    private static uint Next(ref uint x)
    {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        return x;
    }
    private void Worker(int id)
    {
        while (true)
        {
            start.SignalAndWait();
            if (stopping) return;
            try { successes[id] = Execute(id); }
            catch (Exception e) { errors[id] = e; }
            finally { finish.SignalAndWait(); }
        }
    }
    private long Execute(int worker)
    {
        var trace = operation == Operation.Miss ? misses[worker] : keys[worker];
        long success = 0;
        switch (operation)
        {
            case Operation.Hit:
            case Operation.Miss:
                foreach (var key in trace)
                    if (map.TryGetValue(key, out _)) success++;
                break;
            case Operation.Mixed95:
            case Operation.Mixed50:
                int period = operation == Operation.Mixed95 ? 20 : 2;
                for (int i = 0; i < trace.Length; i++)
                {
                    if ((i + worker) % period == 0)
                    {
                        map[trace[i]] = values[(i + worker) % 256];
                        success++;
                    }
                    else if (map.TryGetValue(trace[i], out _)) success++;
                }
                break;
            case Operation.ConditionalUpdate:
                for (int i = 0; i < trace.Length; i++)
                    if (map.TryGetValue(trace[i], out var oldValue) &&
                        map.TryUpdate(trace[i], values[(i + worker) % 256], oldValue)) success++;
                break;
            case Operation.Churn:
                for (int i = 0; i < trace.Length; i++)
                {
                    if (map.TryGetValue(trace[i], out var oldValue) &&
                        map.TryRemove(new KeyValuePair<TKey, TValue>(trace[i], oldValue))) success++;
                    map.TryAdd(trace[i], values[(i + worker) % 256]);
                }
                break;
        }
        return success;
    }
    public long Run(Operation next)
    {
        operation = next;
        start.SignalAndWait();
        finish.SignalAndWait();
        foreach (var error in errors)
            if (error is not null) throw new InvalidOperationException("Worker failed", error);
        return successes.Sum();
    }
    [Benchmark(OperationsPerInvoke = BatchSize)] public long Hit() => Run(Operation.Hit);
    [Benchmark(OperationsPerInvoke = BatchSize)] public long Miss() => Run(Operation.Miss);
    [Benchmark(OperationsPerInvoke = BatchSize)] public long Mixed95() => Run(Operation.Mixed95);
    [Benchmark(OperationsPerInvoke = BatchSize)] public long Mixed50() => Run(Operation.Mixed50);
    [Benchmark(OperationsPerInvoke = BatchSize)] public long ConditionalUpdate() => Run(Operation.ConditionalUpdate);
    [Benchmark(OperationsPerInvoke = BatchSize)] public long Churn() => Run(Operation.Churn);
    [GlobalCleanup]
    public void Cleanup()
    {
        stopping = true;
        start.SignalAndWait();
        foreach (var worker in workers) worker.Join();
        start.Dispose(); finish.Dispose();
    }
}

[MemoryDiagnoser]
[GenericTypeArguments(typeof(long), typeof(long))]
[GenericTypeArguments(typeof(string), typeof(long))]
[GenericTypeArguments(typeof(long), typeof(Entry))]
[GenericTypeArguments(typeof(string), typeof(Entry))]
public class GrowthBenchmarks<TKey, TValue> where TKey : notnull
{
    [Params(1024, 65536)] public int Count { get; set; }
    [Params(false, true)] public bool Presized { get; set; }
    private TKey[] keys = null!;
    private TValue[] values = null!;
    [GlobalSetup]
    public void Setup()
    {
        keys = Enumerable.Range(0, Count).Select(Data<TKey, TValue>.Key).ToArray();
        values = Enumerable.Range(0, Count).Select(i => Data<TKey, TValue>.Value(i)).ToArray();
    }
    [Benchmark]
    public int InsertAll()
    {
        var map = new ConcurrentDictionary<TKey, TValue>(-1, Presized ? Count : 31);
        for (int i = 0; i < keys.Length; i++) map.TryAdd(keys[i], values[i]);
        return map.Count;
    }
    [Benchmark]
    public int InsertNewValues()
    {
        var map = new ConcurrentDictionary<TKey, TValue>(-1, Presized ? Count : 31);
        for (int i = 0; i < keys.Length; i++) map.TryAdd(keys[i], Data<TKey, TValue>.Value(i));
        return map.Count;
    }
}
