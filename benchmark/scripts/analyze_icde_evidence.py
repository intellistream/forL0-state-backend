#!/usr/bin/env python3
"""Summarize retained ICDE measurements, GC pause logs and collapsed profiles."""
import argparse,csv,datetime,json,re,statistics
from collections import Counter,defaultdict
from pathlib import Path

def profiles(paths):
    leaf=Counter();groups=Counter();total=0
    tests={
      'native_engine_inclusive':lambda fs:any('NativeEngine' in f for f in fs),
      'map_batch_inclusive':lambda fs:any('mapAddSequentialAndSum' in f or 'LongInnerMap::add_sequence' in f for f in fs),
      'value_fused_inclusive':lambda fs:any('valueAddAndGet' in f for f in fs),
      'reflection_inclusive':lambda fs:any('/reflect/' in f for f in fs),
      'long_boxing_inclusive':lambda fs:any('Long.valueOf' in f for f in fs),
      'allocator_leaf':lambda fs:any(x in fs[-1] for x in ['jemalloc','operator new','operator delete','malloc','free']),
      'hash_leaf':lambda fs:any(x in fs[-1] for x in ['unordered_map','SwissTable','HashMap']),
      'gc_inclusive':lambda fs:any(x in f for f in fs for x in ['G1Par','G1Collected','G1Scan','GCTask','GangWorker'])}
    for path in paths:
        for line in path.read_text().splitlines():
            stack,n=line.rsplit(' ',1);n=int(n);fs=stack.split(';');total+=n;leaf[fs[-1]]+=n
            for name,test in tests.items():
                if test(fs):groups[name]+=n
    return {'weight':total,'group_percent':{k:100*groups[k]/total if total else 0 for k in tests},
            'top_leaf':[{'frame':f,'weight':n,'percent':100*n/total} for f,n in leaf.most_common(20)]}

def timestamp(text):return datetime.datetime.fromisoformat(text.replace('Z','+00:00')).timestamp()*1000

def gc_logs(root):
    all_logs=defaultdict(list)
    for role in ('tm1','tm2'):
        by_pid=defaultdict(list)
        for p in (root/role/'log').glob('gc-*.log*'):
            m=re.search(r'gc-(\d+)\.log',p.name)
            if m:by_pid[int(m[1])].append(p)
        for pid,paths in by_pid.items():
            pauses=[];first=None;last=None;stops=[]
            for p in paths:
                for line in p.read_text(errors='replace').splitlines():
                    m=re.match(r'^\[([^]]+)\]',line)
                    if not m:continue
                    try:t=timestamp(m[1])
                    except ValueError:continue
                    first=t if first is None else min(first,t);last=t if last is None else max(last,t)
                    pause=re.search(r'\[gc +\]\s+GC\((\d+)\) (Pause.*?) ([\d.]+)ms$',line)
                    if pause:pauses.append((t,float(pause[3]),pause[2],int(pause[1])))
                    stop=re.search(r'Total time for which application threads were stopped: ([\d.]+) seconds',line)
                    if stop:stops.append((t,float(stop[1])*1000))
            all_logs[role].append({'pid':pid,'first_ms':first,'last_ms':last,'pauses':pauses,'stops':stops,'files':[str(p) for p in paths]})
    return all_logs

def gc_for_job(job,row,logs):
    start,end=job['start-time'],job['end-time'];result={}
    for role,entries in logs.items():
        wanted=row.get('taskmanager_pids',{}).get(role)
        candidates=[x for x in entries if x['first_ms'] is not None and x['first_ms']<=end and (wanted is None or x['pid']==wanted)]
        if not candidates:continue
        item=max(candidates,key=lambda x:x['first_ms'])
        selected=[x for x in item['pauses'] if start<=x[0]<=end]
        stopped=[x[1] for x in item['stops'] if start<=x[0]<=end]
        values=sorted(x[1] for x in selected)
        result[role]={'pid':item['pid'],'start_covered':item['first_ms']<=start,
            'pause_count':len(values),'pause_ms':sum(values),'pause_max_ms':max(values,default=0),
            'pause_p95_ms':values[min(len(values)-1,int(len(values)*.95))] if values else 0,
            'full_gc_count':sum('Pause Full' in x[2] for x in selected),
            'safepoint_stop_ms':sum(stopped),'pause_wall_percent':sum(values)*100/max(1,end-start),
            'logs':item['files']}
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runs',type=Path,required=True);p.add_argument('--runtime',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=True);logs=gc_logs(a.runtime);rows=[];flames={}
    for f in sorted(a.runs.glob('icde_*/*/measurement.json')):
        row=json.loads(f.read_text());folder=f.parent;job=json.loads((folder/'job.json').read_text())
        row['gc_pause_analysis']=gc_for_job(job,row,logs)
        metrics=row.get('gc_metrics')
        if metrics:
            row['gc_metric_deltas']={r:{k:metrics['after'][r][k]-v for k,v in metrics['before'][r].items()
                if k in metrics['after'][r] and (k.endswith('.Count') or k.endswith('.Time'))} for r in metrics['before']}
        for event in ('cpu','alloc'):
            paths=list(folder.glob('tm*-'+event+'.collapsed'))
            if paths:flames[str(folder)]={'event':event,'units':'cpu_samples' if event=='cpu' else 'allocation_samples',**profiles(paths)}
        rows.append(row)
    groups=defaultdict(list)
    for row in rows:
        if not row['profiled'] and row['keys']==524288 and row['records']==8000000:
            groups[(row['variant'],row['workload'],row['backend'])].append(row)
    medians=[]
    for (variant,workload,backend),items in sorted(groups.items()):
        secs=[r['seconds'] for r in items]
        medians.append({'variant':variant,'workload':workload,'backend':backend,'n':len(items),'median_s':statistics.median(secs),'min_s':min(secs),'max_s':max(secs)})
    (a.output/'measurements.json').write_text(json.dumps(rows,indent=2)+'\n')
    (a.output/'medians.json').write_text(json.dumps(medians,indent=2)+'\n')
    (a.output/'profiles.json').write_text(json.dumps(flames,indent=2)+'\n')
    with (a.output/'measurements.csv').open('w',newline='') as f:
        fields=['variant','workload','backend','round','keys','records','seconds','profiled','batch_access','job_id','native_sha256','jar_sha256']
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    print(json.dumps({'measurements':len(rows),'profile_pairs':len(flames),'summary':str(a.output)},indent=2))
if __name__=='__main__':main()
