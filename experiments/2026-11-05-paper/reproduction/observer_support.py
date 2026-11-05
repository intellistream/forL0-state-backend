#!/usr/bin/env python3
import importlib.util,subprocess,json,time,hashlib,shutil
from pathlib import Path
b=Path('/root/briskstate-setup/deep-opt-20261103');repo=Path('/root/forL0-state-backend')
while not(b/'selected.json').exists():time.sleep(5)
selected=json.loads((b/'selected.json').read_text());v=b/'variants'/selected['revision'];jar=repo/'benchmark/flink-benchmarks/target-state-subset/state-subset.jar'
out=b/'micro';out.mkdir(exist_ok=True)
def measured_process(cmd, logfile, evidence_file):
 observed={}
 with logfile.open('w') as log:
  process=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
  while process.poll() is None:
   pending=[process.pid];seen=set()
   while pending:
    pid=pending.pop()
    if pid in seen:continue
    seen.add(pid);root=Path('/proc')/str(pid)
    try:
     for thread in (root/'task').iterdir():
      try:pending.extend(map(int,(thread/'children').read_text().split()))
      except FileNotFoundError:pass
     executable=(root/'cmdline').read_bytes().split(b'\0',1)[0].decode(errors='replace')
     if Path(executable).name!='java':continue
     st=(root/'status').read_text();rss=int(next(x.split()[1]for x in st.splitlines()if x.startswith('VmRSS:')))*1024
     record=observed.setdefault(str(pid),{'rss_peak_bytes':0,'affinity_masks':[],'native_mappings':[]})
     record['rss_peak_bytes']=max(record['rss_peak_bytes'],rss)
     record['forked_jmh']=b'ForkedMain' in (root/'cmdline').read_bytes()
     masks={next(x.split(':',1)[1].strip()for x in(f/'status').read_text().splitlines()if x.startswith('Cpus_allowed_list:'))for f in(root/'task').iterdir()}
     record['affinity_masks']=sorted(set(record['affinity_masks'])|masks)
     mappings={line.split()[-1]for line in(root/'maps').read_text().splitlines()if 'libforl0_engine.so' in line}
     record['native_mappings']=sorted(set(record['native_mappings'])|mappings)
    except(FileNotFoundError,ProcessLookupError,StopIteration):pass
   time.sleep(1)
  code=process.wait()
 evidence_file.write_text(json.dumps(observed,indent=2)+'\n')
 if code:raise subprocess.CalledProcessError(code,cmd)
 masks={m for r in observed.values()for m in r['affinity_masks']}
 mapped={p for r in observed.values()for p in r['native_mappings']}
 expected=str(Path(next(x.split('=',1)[1]for x in cmd if x.startswith('-Djava.library.path=')))/'libforl0_engine.so')
 evidence_file.with_suffix('.summary.json').write_text(json.dumps({'java_affinity_masks':sorted(masks),'java_masks_bound':masks=={'4-7'},'observed_jmh_forks':sum(r.get('forked_jmh',False)for r in observed.values()),'native_mappings':sorted(mapped),'expected_native':expected,'native_matches':mapped=={expected}if'backendType=FORL0'in cmd else None},indent=2)+'\n')


java=['taskset' ,'-c','4-7','/usr/lib/jvm/java-11-openjdk/bin/java','-Xms8g','-Xmx8g','-XX:ActiveProcessorCount=4','-XX:+UnlockDiagnosticVMOptions','-XX:+DebugNonSafepoints','-XX:+PreserveFramePointer','-Djava.library.path='+str(v),'-Dorg.slf4j.simpleLogger.defaultLogLevel=error','-cp',str(v/'backend.jar')+':'+str(jar),'org.openjdk.jmh.Main']
metadata={'revision':selected['revision'],'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'backend_sha256':hashlib.sha256((v/'backend.jar').read_bytes()).hexdigest(),'native_sha256':hashlib.sha256((v/'libforl0_engine.so').read_bytes()).hexdigest(),'cpus':'4-7','heap_gib':8,'threads':1,'memory_sweep_overlapping':False}
(out/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
# Alternate the backend order across operations; use two independent forks for each configuration.
ops=['valueAdd','valueGet','valueUpdate','listAdd','listAddAll','listGet','listIterate','listUpdate','mapAdd','mapGet','mapPutAll','mapRemove','mapUpdate']
commands=[]
for i,op in enumerate(ops):
 for backend in (['HEAP','FORL0']if i%2==0 else ['FORL0','HEAP']):
  name=op+'-'+backend
  cmd=java+['LongStateMicrobenchmarks.'+op+'$','-p','backendType='+backend,'-f','2','-wi','3','-w','3s','-i','5','-r','3s','-t','1','-rf','json','-rff',str(out/(name+'.json')),'-foe','true']
  commands.append(cmd);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
  measured_process(cmd,out/(name+'.log'),out/(name+'-processes.json'))
  score=json.load(open(out/(name+'.json')))[0]['primaryMetric']['score'];print('MICRO',op,backend,round(score,3),flush=True)
# Boundary case: changing entity key after every operation, same 64-entry maps.
for op in ['mapGet','mapUpdate']:
 for backend in ['HEAP','FORL0']:
  name=op+'-touch1-'+backend;cmd=java+['LongStateMicrobenchmarks.'+op+'$','-p','backendType='+backend,'-p','touchesPerKey=1','-f','2','-wi','3','-w','3s','-i','5','-r','3s','-t','1','-rf','json','-rff',str(out/(name+'.json')),'-foe','true']
  commands.append(cmd);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
  measured_process(cmd,out/(name+'.log'),out/(name+'-processes.json'))
  print('BOUNDARY',name,flush=True)
diagnostics=b/'micro-diagnostics';diagnostics.mkdir(exist_ok=True)
for rev in ['r4',selected['revision']]:
 candidate=b/'variants'/rev
 prefix=java[:];prefix[prefix.index('-cp')+1]=str(candidate/'backend.jar')+':'+str(jar)
 prefix=[('-Djava.library.path='+str(candidate))if x.startswith('-Djava.library.path=')else x for x in prefix]
 for op in ['listGet','listUpdate','listIterate']:
  for prof in ['gc','cpu']:
   name=rev+'-'+op+'-'+prof
   args=['-prof','gc']if prof=='gc'else['-prof','async:libPath=/root/opt/async-profiler-4.4-linux-arm64/lib/libasyncProfiler.so;event=cpu;output=flamegraph,collapsed;dir='+str(diagnostics/name)+';interval=1000000;rawCommand=cstack=dwarf;threads=true']
   cmd=prefix+['LongStateMicrobenchmarks.'+op+'$','-p','backendType=FORL0','-f','1','-wi','2','-w','2s','-i','3','-r','3s','-t','1','-rf','json','-rff',str(diagnostics/(name+'.json')),'-foe','true']+args
   measured_process(cmd,diagnostics/(name+'.log'),diagnostics/(name+'-processes.json'))
   print('DIAGNOSTIC',name,flush=True)
print('MICRO_COMPLETE',flush=True)
s=importlib.util.spec_from_file_location('dev',repo/'docker/lib/briskstate_dev.py');d=importlib.util.module_from_spec(s);s.loader.exec_module(d)
# Restore the selected backend and standard 32 GiB/TM cluster after the experiments.
shutil.copy2(v/'backend.jar',d.FLINK/'lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar');shutil.copy2(v/'libforl0_engine.so',d.RUNTIME/'native/libforl0_engine.so');d.start()
print('FINAL_CLUSTER_RESTORED',flush=True)
