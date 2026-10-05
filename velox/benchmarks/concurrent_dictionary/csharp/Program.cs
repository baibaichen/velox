using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Running;
using DictionaryBench;

Console.WriteLine(RuntimeInformation.FrameworkDescription);
Console.WriteLine($"Dictionary assembly: {typeof(ConcurrentDictionary<,>).Assembly.Location}");
Console.WriteLine($"Visible processors: {Environment.ProcessorCount}");
if (args.SequenceEqual(new[] { "--self-test" }))
{
    SelfTest.Run();
    return;
}
if (args.Length == 7 && args[0] == "--fixed-time") { FixedTime.Run(args[1..]); return; }
if (args.Length == 7 && args[0] == "--fixed-cache") { FixedTime.Run(args[1..], true); return; }
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
