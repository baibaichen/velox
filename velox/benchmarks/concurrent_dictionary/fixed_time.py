#!/usr/bin/env python3
"""Run the approved fixed-duration matrix sequentially, one process per case."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

root = Path(__file__).resolve().parents[3]
parser = argparse.ArgumentParser()
parser.add_argument('--smoke', action='store_true')
parser.add_argument('--approved', action='store_true')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--write-percent', type=int, nargs='+', default=[0,5,50])
parser.add_argument('--implementations', nargs='+', choices=['simd', 'boost', 'csharp', 'cache', 'csharp-cache'], default=['simd', 'boost', 'csharp'])
args = parser.parse_args()
if any(w < 0 or w > 100 for w in args.write_percent):
    parser.error('write percentages must be between 0 and 100')
if not args.smoke and not args.approved:
    parser.error('full run requires --approved')
args.output.mkdir(parents=True, exist_ok=False)
# Verified i9-13900KF topology: one logical CPU per P core, then E cores,
# then P-core siblings. At 64 workers, repeat this 32-CPU order.
order = list(range(0,16,2)) + list(range(16,32)) + list(range(1,16,2))
if not set(order) <= os.sched_getaffinity(0):
    raise RuntimeError('Expected CPU set is unavailable')
topology = json.loads(subprocess.check_output(['lscpu','-J','-e=CPU,CORE,MAXMHZ']))
cores = {int(c['cpu']): int(c['core']) for c in topology['cpus']}
if len({cores[c] for c in order[:8]}) != 8 or any(cores[i] != cores[i+1] for i in range(0,16,2)):
    raise RuntimeError('Unexpected topology; update CPU mapping')
dotnet = os.environ.get('DOTNET', '/home/chang/.dotnet/dotnet')
build = root/'cmake-build-relwithdebinfo/velox/benchmarks/concurrent_dictionary/cpp'
dll = root/'velox/benchmarks/concurrent_dictionary/csharp/bin/Release/net10.0/ConcurrentDictionaryBench.dll'
commands = {'csharp-cache':[dotnet,str(dll),'--fixed-cache'], 'cache':[str(build/'memory_cache_bench')], 'simd':[str(build/'fixed_time_simd')], 'boost':[str(build/'fixed_time_boost')], 'csharp':[dotnet,str(dll),'--fixed-time']}
metadata={'topology':topology,'cpu_order':order,'warmup':0.1 if args.smoke else 10,'measure':0.2 if args.smoke else 50,'size':100000,'key_chars':32,'value_chars':128,'revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'dotnet':subprocess.check_output([dotnet,'--info'],text=True)}
(args.output/'metadata.json').write_text(json.dumps(metadata,indent=2))
threads=[1,64] if args.smoke else [1,2,4,8,16,32,64]
case=0
with (args.output/'results.jsonl').open('w') as results:
    for n in threads:
        cpus=','.join(str(order[i%32]) for i in range(n))
        for hot in [0,1]:
            for writes in args.write_percent:
                variants=args.implementations
                shift=case%len(variants)
                for variant in variants[shift:]+variants[:shift]:
                    label=f'{variant}-t{n}-hot{hot}-w{writes}'
                    command=commands[variant]+[str(n),str(writes),str(hot),str(metadata['warmup']),str(metadata['measure']),cpus]
                    start=time.time()
                    run=subprocess.run(command,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
                    (args.output/(label+'.log')).write_text(run.stdout)
                    if run.returncode:
                        results.write(json.dumps(dict(implementation=variant, threads=n, hot=bool(hot), write_percent=writes, returncode=run.returncode, error="process failed"))+chr(10))
                        results.flush()
                        print(label, "FAILED", run.returncode, flush=True)
                        continue
                    row=json.loads(run.stdout.splitlines()[-1])
                    if row['reads']+row['writes']<=0 or row['seconds']<metadata['measure']:
                        raise RuntimeError('Invalid result: '+label)
                    if writes==0 and row['writes']!=0: raise RuntimeError('Unexpected writes')
                    row.update(implementation=variant,threads=n,hot=bool(hot),write_percent=writes,wall_seconds=time.time()-start,cpus=cpus)
                    results.write(json.dumps(row)+'\n');results.flush()
                    print(label,round(row['ops_per_second']/1e6,3),'Mops/s',flush=True)
                case+=1
print('COMPLETE',flush=True)
