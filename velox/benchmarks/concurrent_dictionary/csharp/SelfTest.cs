using System.Collections.Concurrent;
namespace DictionaryBench;

public static class SelfTest
{
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
    public static void Run()
    {
        var map = new ConcurrentDictionary<long, Entry>();
        var old = new Entry(1);
        var current = new Entry(1);
        Check(map.TryAdd(1, old), "insert");
        Check(map.TryUpdate(1, current, old), "replace");
        Check(!map.TryUpdate(1, old, old), "stale update");
        Check(!map.TryRemove(new KeyValuePair<long, Entry>(1, old)), "stale remove");
        Check(ReferenceEquals(map[1], current), "identity");
        Check(map.TryRemove(new KeyValuePair<long, Entry>(1, current)), "remove");
        Check(old.Id == 1 && current.Id == 1, "held values survive removal");
        Verify<long, long>(); Verify<string, long>(); Verify<long, Entry>(); Verify<string, Entry>();
        Console.WriteLine("PASS: identity semantics; all workloads/types; 1/4 workers; uniform/hot; growth.");
    }
    private static void Verify<TKey, TValue>() where TKey : notnull
    {
        foreach (int threads in new[] { 1, 4 })
            foreach (var pattern in Enum.GetValues<AccessPattern>())
            {
                var bench = new DictionaryBenchmarks<TKey, TValue> { Count = 1024, Threads = threads, Pattern = pattern };
                bench.Setup();
                try
                {
                    foreach (var operation in Enum.GetValues<Operation>())
                    {
                        long success = bench.Run(operation);
                        int total = DictionaryBenchmarks<TKey, TValue>.BatchSize;
                        Check(success >= 0 && success <= total, "success range");
                        if (operation == Operation.Miss) Check(success == 0, "miss count");
                        else if (operation is Operation.Hit or Operation.Mixed95 or Operation.Mixed50 || threads == 1)
                            Check(success == total, $"operation count: {operation}");
                        Check(bench.EntryCount == 1024, $"entry count: {operation}");
                        Console.WriteLine($"{typeof(TKey).Name}/{typeof(TValue).Name} {threads} {pattern} {operation}: {success}/{total}");
                    }
                    Check(bench.Hit() == DictionaryBenchmarks<TKey, TValue>.BatchSize, "keys restored after churn");
                }
                finally { bench.Cleanup(); }
            }
        foreach (bool presized in new[] { false, true })
        {
            var growth = new GrowthBenchmarks<TKey, TValue> { Count = 1024, Presized = presized };
            growth.Setup();
            Check(growth.InsertAll() == 1024, "growth precreated");
            Check(growth.InsertNewValues() == 1024, "growth new values");
        }
    }
}
