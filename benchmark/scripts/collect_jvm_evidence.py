#!/usr/bin/env python3
"""Read-only JVM GC/heap/CPU evidence sampler for running ICDE campaigns."""
import argparse,datetime,importlib.util,json,time,urllib.parse
from pathlib import Path
REPO=Path(__file__).resolve().parents[2]
s=importlib.util.spec_from_file_location('dev',REPO/'docker/lib/briskstate_dev.py');dev=importlib.util.module_from_spec(s);s.loader.exec_module(dev)
KEYS=['Status.JVM.GarbageCollector.All.Count','Status.JVM.GarbageCollector.All.Time',
      'Status.JVM.GarbageCollector.G1_Young_Generation.Count','Status.JVM.GarbageCollector.G1_Young_Generation.Time',
      'Status.JVM.GarbageCollector.G1_Old_Generation.Count','Status.JVM.GarbageCollector.G1_Old_Generation.Time',
      'Status.JVM.Memory.Heap.Used','Status.JVM.Memory.Heap.Committed','Status.JVM.Memory.Heap.Max','Status.JVM.CPU.Load']
def snapshot():
    row={'timestamp_ms':int(time.time()*1000),'roles':{}}
    row['running_jobs']=[j['jid'] for j in dev.api('/jobs/overview')['jobs'] if j['state']=='RUNNING']
    for role in ['tm1','tm2']:
        pid=int(next((dev.RUNTIME/role/'pid').glob('*.pid')).read_text().strip())
        metrics=dev.api('/taskmanagers/briskstate-'+role+'/metrics?get='+urllib.parse.quote(','.join(KEYS),safe=','))
        row['roles'][role]={'pid':pid,'metrics':{x['id']:float(x['value']) for x in metrics}}
    return row
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--seconds',type=int,default=7200);args=p.parse_args()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    end=time.monotonic()+args.seconds
    with args.output.open('a',buffering=1) as f:
        while time.monotonic()<end:
            try:f.write(json.dumps(snapshot())+'\n')
            except Exception as e:f.write(json.dumps({'timestamp_ms':int(time.time()*1000),'error':str(e)})+'\n')
            time.sleep(2)
