#!/usr/bin/env python3
"""Fixed ICDE 2017 task graphs with Long state-pressure workloads; no checkpoints."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
import urllib.parse

REPO=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('briskstate_dev',REPO/'docker/lib/briskstate_dev.py')
dev=importlib.util.module_from_spec(spec);spec.loader.exec_module(dev)
CONTRACT=json.loads((REPO/'benchmark/benchset/reference/topology-contract.json').read_text())
PROFILER=Path('/root/opt/async-profiler-4.4-linux-arm64/bin/asprof')
LONG_RECORDS={'wc':48000000,'fd':32000000,'sd':24000000,'tm':24000000,'lg':12000000,'vs':3000000,'lr':3000000}


def validate_graph(path,workload):
    actual=json.loads(path.read_text()); expected=CONTRACT['workloads'][workload]
    assert {n['name'] for n in actual['nodes']}==set(expected['nodes']),(workload,actual)
    count=lambda es:Counter((e['from'],e['to']) for e in es)
    assert count(actual['edges'])==count(expected['edges']),(workload,actual['edges'])
    for n in actual['nodes']:
        assert n['parallelism']==(1 if n['name']=='Global ACD' else 8),n


def signed(n):return ((n+(1<<63))%(1<<64))-(1<<63)

def mix(x):
    mask=(1<<64)-1
    x=((x^(x>>30))*0xbf58476d1ce4e5b9)&mask
    x=((x^(x>>27))*0x94d049bb133111eb)&mask
    return x^(x>>31)


def pids():
    return {role:int(next((dev.RUNTIME/role/'pid').glob('*.pid')).read_text().strip()) for role in ('tm1','tm2')}


def process_stats(pid):
    proc=Path('/proc')/str(pid); fields=(proc/'stat').read_text().split()
    import os
    rss=int(next(x.split()[1] for x in (proc/'status').read_text().splitlines() if x.startswith('VmRSS:')))*1024
    return {'cpu_seconds':(int(fields[13])+int(fields[14]))/os.sysconf('SC_CLK_TCK'),'rss_bytes':rss}


def profile(action,targets,directory,event='cpu'):
    def one(item):
        role,pid=item
        if action=='start':
            sampling=['-e','cpu','-i','5ms','--cstack','dwarf'] if event=='cpu' else ['-e','alloc','--alloc','1m']
            cmd=[str(PROFILER),'start']+sampling+[str(pid)]
            subprocess.run(cmd,check=True,stdout=subprocess.DEVNULL)
        else:
            for command,fmt,suffix in [('dump','flamegraph','html'),('stop','collapsed','collapsed')]:
                subprocess.run([str(PROFILER),command,'-o',fmt,'-f',str(directory/(role+'-'+event+'.'+suffix)),str(pid)],check=True,stdout=subprocess.DEVNULL)
    with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(one,targets.items()))


def gc_snapshot():
    keys=['Status.JVM.GarbageCollector.All.Count','Status.JVM.GarbageCollector.All.Time',
          'Status.JVM.GarbageCollector.G1_Young_Generation.Count','Status.JVM.GarbageCollector.G1_Young_Generation.Time',
          'Status.JVM.GarbageCollector.G1_Old_Generation.Count','Status.JVM.GarbageCollector.G1_Old_Generation.Time',
          'Status.JVM.Memory.Heap.Used','Status.JVM.Memory.Heap.Committed','Status.JVM.Memory.Heap.Max']
    return {r:{x['id']:float(x['value']) for x in dev.api('/taskmanagers/briskstate-'+r+'/metrics?get='+urllib.parse.quote(','.join(keys),safe=','))} for r in ('tm1','tm2')}


def run_one(args,workload,backend,round_index,campaign):
    directory=campaign/(f'{round_index:02d}_{workload}_{backend}_{uuid.uuid4().hex[:6]}');directory.mkdir()
    factory='hashmap' if backend=='heap' else 'org.apache.flink.state.forl0.ForL0StateBackendFactory'
    command=[str(dev.FLINK/'bin/flink'),'run','-d','-m','127.0.0.1:8081',
             '-Dstate.backend.type='+factory,'-Dstate.backend.forl0.l0-cache.enabled=false',
             '-Drestart-strategy.type=none','-p','8','-c','org.apache.flink.benchmark.benchset.BenchsetMain',
             str(REPO/'benchmark/benchset/target/benchset-1.0-SNAPSHOT.jar'),
             '--benchmark',workload,'--numKeys',str(args.keys),
             '--numRecords',str(args.records),'--featureWidth',str(args.width),'--touches',str(args.touches),
             '--parallelism','8','--checkpointInterval','0',
             '--outputDirectory',str(directory)]
    (directory/'command.json').write_text(json.dumps(command,indent=2)+'\n')
    targets=pids();before={r:process_stats(pid) for r,pid in targets.items()};peak={r:x['rss_bytes'] for r,x in before.items()}
    gc_before=gc_snapshot()
    result=subprocess.run(command,env=dev.role_env('jm'),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=120)
    (directory/'submission.log').write_text(result.stdout)
    if result.returncode:raise RuntimeError(result.stdout)
    jid=re.search(r'Job has been submitted with JobID ([0-9a-f]{32})',result.stdout)
    assert jid,result.stdout
    jid=jid[1];started=False;t0=time.monotonic()
    try:
        while time.monotonic()-t0<args.timeout:
            job=dev.api('/jobs/'+jid)
            for r,pid in targets.items():peak[r]=max(peak[r],process_stats(pid)['rss_bytes'])
            if args.profile and not started and job['state']=='RUNNING' and time.monotonic()-t0>5:
                profile('start',targets,directory,args.profile_event);started=True
            if job['state'] in ('FINISHED','FAILED','CANCELED'):break
            time.sleep(1)
        else:
            subprocess.run([str(dev.FLINK/'bin/flink'),'cancel','-m','127.0.0.1:8081',jid],check=True,env=dev.role_env('jm'))
            raise TimeoutError(jid)
    finally:
        if started:profile('stop',targets,directory,args.profile_event)
    for name,suffix in [('job',''),('checkpoints','/checkpoints'),('exceptions','/exceptions'),('config','/config')]:
        (directory/(name+'.json')).write_text(json.dumps(dev.api('/jobs/'+jid+suffix),indent=2)+'\n')
    assert job['state']=='FINISHED',(jid,job['state'],str(directory))
    validate_graph(directory/'topology.json',workload)
    checkpoints=json.loads((directory/'checkpoints.json').read_text())
    assert checkpoints['counts']['total']==0,checkpoints
    sink_rows=[json.loads(f.read_text()) for f in (directory/'sinks').glob('*.json')]
    observed={field:signed(sum(row[field] for row in sink_rows)) for field in ('records','feature_projection','source_signature')}
    expected=CONTRACT['workloads'][workload]['sink_multiplier']*args.records
    assert observed['records']==expected,(workload,observed,expected)
    if args.validate:
        signature=signed(sum(mix(i) for i in range(args.records))*CONTRACT['workloads'][workload]['sink_multiplier'])
        assert observed['source_signature']==signature,(workload,observed,signature)
    after={r:process_stats(pid) for r,pid in targets.items()}
    row=dict(workload=workload,backend=backend,round=round_index,variant=args.variant,
             keys=args.keys,records=args.records,width=args.width,touches=args.touches,
             access_protocol="standard-flink-api-v2-large-long",profiled=args.profile,profile_event=args.profile_event if args.profile else None,checkpointing=False,
             job_id=jid,seconds=job['duration']/1000,records_per_second=args.records*1000/job['duration'],
             sink=observed,peak_rss_bytes=peak,
             taskmanager_pids=targets,
             cpu_seconds={r:after[r]['cpu_seconds']-before[r]['cpu_seconds'] for r in targets},directory=str(directory))
    gc_after=gc_snapshot()
    row['gc_metrics']={'before':gc_before,'after':gc_after}
    row['native_sha256']=hashlib.sha256((dev.RUNTIME/'native/libforl0_engine.so').read_bytes()).hexdigest()
    row['jar_sha256']=hashlib.sha256((REPO/'benchmark/benchset/target/benchset-1.0-SNAPSHOT.jar').read_bytes()).hexdigest()
    (directory/'measurement.json').write_text(json.dumps(row,indent=2)+'\n')
    print('PASS',workload,backend,f"{row['seconds']:.3f}s",'sink='+str(observed['records']),str(directory),flush=True)
    return row


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend',choices=['all','heap','forl0'],default='all')
    parser.add_argument('--benchmarks',default='wc,fd,sd,tm,lg,vs,lr')
    parser.add_argument('--rounds',type=int,default=2)
    parser.add_argument('--keys',type=int,default=524288)
    parser.add_argument('--records',type=int,help='Override the per-workload long-run record count')
    parser.add_argument('--width',type=int,default=64)
    parser.add_argument('--touches',type=int,default=128)
    parser.add_argument('--timeout',type=int,default=1800)
    parser.add_argument('--fresh-jvm',action='store_true',help='Restart the idle cluster before each measurement')
    parser.add_argument('--profile',action='store_true')
    parser.add_argument('--profile-event',choices=['cpu','alloc'],default='cpu')
    parser.add_argument('--validate',action='store_true',help='Independent source signature check; use a small input')
    parser.add_argument('--variant',default='standard-api-v2-large-long')
    args=parser.parse_args()
    benchmarks=args.benchmarks.split(',');assert all(x in CONTRACT['workloads'] for x in benchmarks)
    assert args.keys>0 and args.rounds>0
    assert args.records is None or args.records>=args.keys
    overview=dev.api('/overview');assert overview['taskmanagers']==2 and overview['slots-total']==8,overview
    campaign=REPO/'benchmark/results/runs'/('icde_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]);campaign.mkdir()
    (campaign/'settings.json').write_text(json.dumps({**vars(args),'effective_records':{w:args.records if args.records is not None else LONG_RECORDS[w] for w in benchmarks}},indent=2)+'\n')
    rows=[]
    for index in range(1,args.rounds+1):
        for workload in benchmarks:
            backends=['heap','forl0'] if args.backend=='all' else [args.backend]
            if index%2==0:backends.reverse()
            pair=[]
            run_args=argparse.Namespace(**vars(args))
            run_args.records=args.records if args.records is not None else LONG_RECORDS[workload]
            for backend in backends:
                assert dev.api('/overview')['jobs-running']==0, 'A job is already running'
                if args.fresh_jvm:
                    dev.stop();dev.start()
                row=run_one(run_args,workload,backend,index,campaign);rows.append(row);pair.append(row)
                (campaign/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
            if len(pair)==2:
                assert pair[0]['sink']==pair[1]['sink'],('Paired output mismatch',pair)
    print('CAMPAIGN_COMPLETE',str(campaign),flush=True)

if __name__=='__main__':main()
