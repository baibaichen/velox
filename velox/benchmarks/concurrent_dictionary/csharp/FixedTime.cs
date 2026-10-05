using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
namespace DictionaryBench;
public static class FixedTime
{
    [DllImport("libc", SetLastError = true)]
    private static extern int sched_setaffinity(int pid, nuint size, byte[] mask);
    public static void Run(string[] args, bool useCache = false)
    {
        int n=int.Parse(args[0]), writes=int.Parse(args[1]);bool hot=int.Parse(args[2])!=0;
        double warm=double.Parse(args[3],System.Globalization.CultureInfo.InvariantCulture),seconds=double.Parse(args[4],System.Globalization.CultureInfo.InvariantCulture);
        int[] cpus=args[5].Split(',').Select(int.Parse).ToArray();
        if(n<1||n>64||writes<0||writes>100||warm<=0||seconds<=0||cpus.Length!=n)throw new ArgumentException();
        var map=new ConcurrentDictionary<string,string>();
        string scenario = Environment.GetEnvironmentVariable("CACHE_SCENARIO") ?? "steady";
        using var cache = useCache ? new MemoryCache(new MemoryCacheOptions { SizeLimit = scenario == "capacity" ? 10000 : 100000, ExpirationScanFrequency = TimeSpan.FromMilliseconds(10) }) : null;
        var options = new MemoryCacheEntryOptions().SetSize(1);
        if (scenario == "expiry") options.AbsoluteExpirationRelativeToNow = TimeSpan.FromMilliseconds(10);
        if (useCache) Console.WriteLine($"MemoryCache assembly: {typeof(MemoryCache).Assembly.Location}");
        var keys=Enumerable.Range(0,100000).Select(i=>i.ToString("D32",System.Globalization.CultureInfo.InvariantCulture)).ToArray();
        foreach(var k in keys) { if(useCache) cache!.Set(k,new string('a',128),options); else map.TryAdd(k,new string('a',128)); }
        var traces=new int[n][];
        for(int w=0;w<n;++w){traces[w]=new int[65536];uint state=0x9e3779b9u^(uint)(w+1);for(int i=0;i<65536;++i){uint r=Next(ref state);int range=hot&&r%100<90?1000:100000;traces[w][i]=(int)(Next(ref state)%(uint)range);}}
        using var gate=new Barrier(n+1);long deadline=0;
        var latencies=new List<double>[n];var misses=new long[n];var reads=new long[n];var updates=new long[n];var sums=new long[n];var workers=new Thread[n];
        for(int w=0;w<n;++w){int id=w;workers[w]=new Thread(()=>{
            byte[] mask=new byte[128];mask[cpus[id]/8]|=(byte)(1<<(cpus[id]%8));if(sched_setaffinity(0,(nuint)mask.Length,mask)!=0)Environment.FailFast("affinity failed");
            for(int phase=0;phase<2;++phase){gate.SignalAndWait();long r=0,u=0,sum=0,index=0,miss=0;var latency=new List<double>();
                do{for(int b=0;b<256;++b,++index){var k=keys[traces[id][index%65536]];long sampleStart=index%1024==0?Stopwatch.GetTimestamp():0;
                    if((index+id)%100<writes){var value=new string((char)('a'+index%26),128);if(useCache)cache!.Set(k,value,options);else map[k]=value;++u;}
                    else{string? v;
                        bool found=useCache?cache!.TryGetValue(k,out v):map.TryGetValue(k,out v);
                        if(!found){if(!useCache)Environment.FailFast("invalid lookup");++miss;}
                        else{if(v is null||v.Length!=128)Environment.FailFast("invalid value");sum+=v[0];}++r;}
                    if(index%1024==0)latency.Add((Stopwatch.GetTimestamp()-sampleStart)*1e9/Stopwatch.Frequency);
                }}while(Stopwatch.GetTimestamp()<deadline);
                latencies[id]=latency;misses[id]=miss;reads[id]=r;updates[id]=u;sums[id]=sum;gate.SignalAndWait();
            }
        });workers[w].Start();}
        double elapsed=0;foreach(double duration in new[]{warm,seconds}){long begin=Stopwatch.GetTimestamp();deadline=begin+(long)(duration*Stopwatch.Frequency);gate.SignalAndWait();gate.SignalAndWait();elapsed=(Stopwatch.GetTimestamp()-begin)/(double)Stopwatch.Frequency;}
        foreach(var t in workers)t.Join();if(!useCache && map.Count!=100000)throw new Exception("cardinality");
        var samples=latencies.SelectMany(x=>x).OrderBy(x=>x).ToArray();
        using var process=Process.GetCurrentProcess();
        Console.WriteLine(JsonSerializer.Serialize(new{cache=useCache,entries=useCache?cache!.Count:map.Count,misses=misses.Sum(),reads=reads.Sum(),writes=updates.Sum(),checksum=sums.Sum(),seconds=elapsed,ops_per_second=(reads.Sum()+updates.Sum())/elapsed,process_cpu_seconds=process.TotalProcessorTime.TotalSeconds,sample_p50_ns=samples[samples.Length/2],sample_p99_ns=samples[samples.Length*99/100],peak_rss_kb=process.PeakWorkingSet64/1024}));
    }
    private static uint Next(ref uint x){x^=x<<13;x^=x>>17;x^=x<<5;return x;}
}
