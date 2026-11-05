#!/usr/bin/env python3
"""Full-job per-thread CPU/affinity evidence; original backend and JAR unchanged."""
import concurrent.futures,hashlib,importlib.util,json,os,re,subprocess,threading,time,traceback
from collections import Counter
from pathlib import Path
REPO=Path('/root/forL0-state-backend');BASE=Path('/root/briskstate-setup/fair-standard-api-20261103')
s=importlib.util.spec_from_file_location('dev',REPO/'docker/lib/briskstate_dev.py');dev=importlib.util.module_from_spec(s);s.loader.exec_module(dev)
s=importlib.util.spec_from_file_location('contract',REPO/'benchmark/scripts/run_benchset.py');contract=importlib.util.module_from_spec(s);s.loader.exec_module(contract)
ASPROF='/root/opt/async-profiler-4.4-linux-arm64/bin/asprof';HZ=os.sysconf('SC_CLK_TCK')
ORIGINAL=REPO/'benchmark/benchset/target/benchset-1.0-SNAPSHOT.jar';ARRAY=BASE/'heap-array-benchset/target/benchset-1.0-SNAPSHOT.jar'
GC_PREFIX=('GC Thread','G1 Main Marker','G1 Conc','G1 Refine','G1 Young RemSet')
def stat(path):
 text=path.read_text();end=text.rfind(')');f=text[end+2:].split()
 return {'name':text[text.find('(')+1:end],'ticks':int(f[11])+int(f[12]),'start_ticks':int(f[19]),'last_cpu':int(f[36])}
def pids():return {r:int(next((dev.RUNTIME/r/'pid').glob('*.pid')).read_text().strip()) for r in ['tm1','tm2']}
def snapshot(targets):
 data={'time_ms':int(time.time()*1000),'roles':{}}
 for role,pid in targets.items():
  root=Path('/proc')/str(pid);proc=stat(root/'stat');ts={};masks=Counter();errors=[];allowed=set(range(4,8) if role=='tm1' else range(8,12))
  for d in (root/'task').iterdir():
   try:
    st=stat(d/'stat');raw=next(x.split(':',1)[1].strip() for x in (d/'status').read_text().splitlines() if x.startswith('Cpus_allowed_list:'));actual=set()
    for token in raw.split(','):
     a,*b=token.split('-');actual.update(range(int(a),int(b[0])+1) if b else [int(a)])
    if not actual or not actual<=allowed:errors.append({'tid':int(d.name),'name':st['name'],'affinity':raw})
    st['affinity']=raw;ts[d.name]=st;masks[raw]+=1
   except (FileNotFoundError,ProcessLookupError):pass
  data['roles'][role]={'pid':pid,'process':proc,'threads':ts,'mask_counts':dict(masks),'violations':errors}
 return data

def profile(action,targets,folder):
 def one(pair):
  role,pid=pair
  with (folder/'profiler.log').open('a') as log:
   if action=='start':
    subprocess.run([ASPROF,'start','-e','cpu','-i','5ms','--cstack','dwarf','-t',str(pid)],stdout=log,stderr=log,check=True)
   else:
    for command,fmt,suffix in [('dump','flamegraph','html'),('stop','collapsed','collapsed')]:
     subprocess.run([ASPROF,command,'-t','-o',fmt,'-f',str(folder/(role+'-cpu.'+suffix)),str(pid)],stdout=log,stderr=log,check=True)
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(one,targets.items()))

def summarize_threads(first,last,history):
 result={}
 for role in first['roles']:
  initial=first['roles'][role];base=initial['threads'];observed={}
  for snap in history+[last]:
   for tid,t in snap['roles'][role]['threads'].items():
    key=(tid,t['start_ticks']);old=observed.get(key)
    if old is None or t['ticks']>old['ticks']:observed[key]=t
  rows=[]
  for (tid,start),t in observed.items():
   old=base.get(tid);initial_ticks=old['ticks'] if old and old['start_ticks']==start else 0
   rows.append({'tid':int(tid),'start_ticks':start,'name':t['name'],'cpu_seconds':max(0,t['ticks']-initial_ticks)/HZ,'is_gc_worker':t['name'].startswith(GC_PREFIX)})
  process_cpu=(last['roles'][role]['process']['ticks']-initial['process']['ticks'])/HZ
  result[role]={'pid':initial['pid'],'process_cpu_seconds':process_cpu,'gc_worker_cpu_seconds':sum(r['cpu_seconds'] for r in rows if r['is_gc_worker']),'observed_thread_cpu_seconds':sum(r['cpu_seconds'] for r in rows),'threads':rows,
    'affinity_violations':[v for snap in [first]+history+[last] for v in snap['roles'][role]['violations']],
    'affinity_masks':sorted({m for snap in [first]+history+[last] for m in snap['roles'][role]['mask_counts']})}
 return result

def run_one(workload,backend,index,keys=524288,records=8000000,width=64,touches=32,profiled=True,fresh=True,batch=True):
 overview=dev.api('/overview');assert overview['jobs-running']==0,overview
 if fresh:dev.stop();dev.start()
 folder=BASE/'runs'/f'{index:02d}_{workload}_{backend}';folder.mkdir(parents=True)
 jar=ORIGINAL
 factory='org.apache.flink.state.forl0.ForL0StateBackendFactory' if backend=='forl0' else 'hashmap'
 cmd=[str(dev.FLINK/'bin/flink'),'run','-d','-m','127.0.0.1:8081','-Dstate.backend.type='+factory,'-Dstate.backend.forl0.l0-cache.enabled=false','-Drestart-strategy.type=none','-p','8','-c','org.apache.flink.benchmark.benchset.BenchsetMain',str(jar),
 '--benchmark',workload,'--numKeys',str(keys),'--numRecords',str(records),'--featureWidth',str(width),'--touches',str(touches),'--parallelism','8','--checkpointInterval','0','--outputDirectory',str(folder)]
 (folder/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');targets=pids()
 if profiled:profile('start',targets,folder)
 first=snapshot(targets);samples=[];stop=threading.Event();failure=[]
 def watch():
  with (folder/'thread-samples.jsonl').open('w',buffering=1) as out:
   out.write(json.dumps(first)+'\n')
   while not stop.wait(.5):
    try:
     now=snapshot(targets);samples.append(now);out.write(json.dumps(now)+'\n')
     if any(v['violations'] for v in now['roles'].values()):failure.append('Affinity violation')
    except Exception as e:failure.append(str(e))
 worker=threading.Thread(target=watch,daemon=True);worker.start()
 try:
  submitted=subprocess.run(cmd,env=dev.role_env('jm'),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=120)
  (folder/'submission.log').write_text(submitted.stdout);assert submitted.returncode==0,submitted.stdout
  jid=re.search(r'Job has been submitted with JobID ([0-9a-f]{32})',submitted.stdout)[1];t0=time.monotonic()
  while time.monotonic()-t0<1800:
   job=dev.api('/jobs/'+jid)
   if job['state'] in ['FINISHED','FAILED','CANCELED']:break
   time.sleep(.5)
  else:raise TimeoutError(jid)
  last=snapshot(targets)
 finally:
  stop.set();worker.join(timeout=5)
  if profiled:profile('stop',targets,folder)
 assert not failure,failure
 for name,suffix in [('job',''),('checkpoints','/checkpoints'),('exceptions','/exceptions')]:
  (folder/(name+'.json')).write_text(json.dumps(dev.api('/jobs/'+jid+suffix),indent=2)+'\n')
 assert job['state']=='FINISHED',job
 assert json.loads((folder/'checkpoints.json').read_text())['counts']['total']==0
 contract.validate_graph(folder/'topology.json',workload)
 sink_rows=[json.loads(p.read_text()) for p in (folder/'sinks').glob('*.json')]
 sink={f:contract.signed(sum(x[f] for x in sink_rows)) for f in ['records','feature_projection','source_signature']}
 assert sink['records']==records*contract.CONTRACT['workloads'][workload]['sink_multiplier']
 row={'workload':workload,'backend':backend,'keys':keys,'records':records,'width':width,'touches':touches,'seconds':job['duration']/1000,'job_id':jid,'profiled':profiled,'access_protocol':'standard-flink-api-v1','capture_start_ms':first['time_ms'],'capture_end_ms':last['time_ms'],'capture_seconds':(last['time_ms']-first['time_ms'])/1000,'sink':sink,
      'native_sha256':hashlib.sha256((dev.RUNTIME/'native/libforl0_engine.so').read_bytes()).hexdigest(),'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'roles':summarize_threads(first,last,samples),'directory':str(folder)}
 assert all(not v['affinity_violations'] for v in row['roles'].values())
 (folder/'measurement.json').write_text(json.dumps(row,indent=2)+'\n')
 print('PASS',workload,backend,round(row['seconds'],3),'cpu',round(sum(x['process_cpu_seconds'] for x in row['roles'].values()),2),'gc_cpu',round(sum(x['gc_worker_cpu_seconds'] for x in row['roles'].values()),2),str(folder),flush=True)
 return row

def main():
 rows=[];index=0
 for workload in ['wc','vs','lr','fd','sd','tm','lg']:
  pair=[]
  for backend in ['heap','forl0']:
   index+=1;r=run_one(workload,backend,index);rows.append(r);pair.append(r)
   (BASE/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
  assert pair[0]['sink']==pair[1]['sink']
  assert pair[0]['jar_sha256']==pair[1]['jar_sha256']
  def canonical(command):
   command=command[:]
   command=[x for x in command if not x.startswith('-Dstate.backend.type=')]
   command[command.index('--outputDirectory')+1]='OUTPUT'
   return command
  assert canonical(json.loads((Path(pair[0]['directory'])/'command.json').read_text()))==canonical(json.loads((Path(pair[1]['directory'])/'command.json').read_text()))
 for round_index in [1,2]:
  pair=[]
  for backend in (['heap','forl0'] if round_index==1 else ['forl0','heap']):
   index+=1;r=run_one('wc',backend,index,profiled=False);rows.append(r);pair.append(r)
   (BASE/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
  assert pair[0]['sink']==pair[1]['sink']
 pair=[]
 for backend in ['heap','forl0']:
  index+=1;r=run_one('vs',backend,index,profiled=False);rows.append(r);pair.append(r)
  (BASE/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
 assert pair[0]['sink']==pair[1]['sink']
 print('FAIR_STANDARD_API_COMPLETE',flush=True)
if __name__=='__main__':main()
