using System;
using System.Linq;
using System.Reflection;
using System.Collections.Concurrent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Internal;
var clock=new Clock();
var cache=new MemoryCache(new MemoryCacheOptions { TrackStatistics=true, Clock=clock, SizeLimit=1000 });
uint rng=123;
uint Next(){rng=unchecked(rng*1664525+1013904223);return rng;}
for(int i=0;i<1000;++i) {
 clock.UtcNow+=TimeSpan.FromTicks(1);
 int op=(int)((Next()>>16)%8),key=(int)((Next()>>16)%8),v=(int)((Next()>>16)%1000);
 string result="-";
 if(op<3)cache.Set(key,v,new MemoryCacheEntryOptions{Size=v%4,AbsoluteExpirationRelativeToNow=TimeSpan.FromTicks(1+v%20),Priority=(CacheItemPriority)(v%4)});
 else if(op==3) result=cache.TryGetValue(key,out object? value)?value!.ToString()!:"miss";
 else if(op==4)cache.Remove(key);
 else if(op==5)cache.Compact(0.5);
 else if(op==6) clock.UtcNow+=TimeSpan.FromTicks(3);
 else cache.Clear();
 var s=cache.GetCurrentStatistics()!;
 var state=typeof(MemoryCache).GetField("_coherentState",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(cache)!;
 var entries=(ConcurrentDictionary<object,CacheEntry>)state.GetType().GetField("_nonStringEntries",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(state)!;
 var reasons=string.Join(",",entries.OrderBy(x=>(int)x.Key).Select(x=>$"{x.Key}:{x.Value.EvictionReason}"));
 Console.WriteLine($"{i}|{result}|{string.Join(",",cache.Keys.Cast<int>().OrderBy(x=>x))}|{s.TotalHits}|{s.TotalMisses}|{s.TotalEvictions}|{s.CurrentEstimatedSize}|{reasons}");
}
class Clock:ISystemClock {public DateTimeOffset UtcNow{get;set;}=DateTimeOffset.UnixEpoch;}
