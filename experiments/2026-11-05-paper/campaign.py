"""Paper campaign: frozen common job, blocked repeats, independent diagnostics."""
from pathlib import Path
import argparse,concurrent.futures,gzip,hashlib,importlib.util,json,os,random,re,shutil,subprocess,threading,time,traceback,urllib.request

B=Path('/root/briskstate-setup/paper-experiments-20261104')
REPO=Path('/root/forL0-state-backend')
src=(B/'reproduction/capture_support.py').read_text()
src=src.replace("REPO/'docker/lib/briskstate_dev.py'", "Path('/root/briskstate-setup/paper-experiments-20261104/reproduction/repo/docker/lib/briskstate_dev.py')")
src=src.replace("REPO/'benchmark/scripts/run_benchset.py'", "Path('/root/briskstate-setup/paper-experiments-20261104/reproduction/repo/benchmark/scripts/run_benchset.py')")
src=src.replace("allowed=set(range(4,8) if role=='tm1' else range(8,12))", "allowed=set(range(int(dev.ROLES[role][0].split('-')[0]),int(dev.ROLES[role][0].split('-')[1])+1))")
c={'__name__':'frozen_capture'};exec(compile(src,'<collector>','exec'),c)
dev=c['dev'];orig_configure=dev.configure
ACTIVE={};sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
WORKLOADS=['wc','fd','sd','tm','lg','vs','lr'];MEMORIES=[32,40,48,56,64]
MULTIPLIERS={'wc':1,'fd':1,'sd':1,'tm':1,'lg':3,'vs':8,'lr':12}
BACKEND_SHA='81d12bb9dc9366053b15159fb6a8b90ed1dfaf28cffdb4b4ba5104af9627f37b'
NATIVE_SHA='8ffa4d449766e03f9fe06efea91b59337c814fa34a71314ecfe1f34fd833ba03'
JAR=B/'steady-job/paper-steady-benchset.jar'

def dump(p,value):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);q=p.with_suffix(p.suffix+'.tmp')
    q.write_text(json.dumps(value,indent=2)+'\n');q.replace(p)

def configure():
    orig_configure()
    for role in dev.ROLES:
        p=dev.RUNTIME/role/'conf/config.yaml'
        cfg={k:json.loads(v) for k,v in (line.split(': ',1) for line in p.read_text().splitlines())}
        cfg.update({'taskmanager.memory.process.size':str(ACTIVE['memory_gib'])+'g',
                    'taskmanager.memory.task.off-heap.size':'8g' if ACTIVE['backend']=='forl0' else '0g',
                    'taskmanager.memory.managed.size':'0m'})
        cfg['env.java.opts.all']+=' -XX:+UseG1GC'
        p.write_text(''.join(k+': '+json.dumps(v)+'\n' for k,v in cfg.items()))
dev.configure=configure

def stop():
    def one(role):
        if (dev.RUNTIME/role/'conf').exists():
            subprocess.run([str(dev.FLINK/'bin'/dev.ROLES[role][1]),'stop'],check=True,env=dev.role_env(role),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(one,['tm2','tm1','jm']))
dev.stop=stop

def restore_default():
    stop()
    s=importlib.util.spec_from_file_location('restore_default',REPO/'docker/lib/briskstate_dev.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);m.start()

def api(path):return dev.api(path)

def read_numbers(p):
    result={}
    for line in p.read_text().splitlines():
        k,_,v=line.partition(':');parts=v.split()
        if parts and parts[0].isdigit():result[k]=int(parts[0])*(1024 if len(parts)>1 and parts[1]=='kB' else 1)
    return result

def snapshot(pids,extra=False):
    targets=dict(pids)
    if ACTIVE.get('jm_pid'):targets['jm']=ACTIVE['jm_pid']
    row=c['snapshot'](targets);row['monotonic_ns']=time.monotonic_ns()
    for role,pid in targets.items():
        p=Path('/proc')/str(pid);st=read_numbers(p/'status')
        row['roles'][role]['memory']={k:st.get(k) for k in ['VmRSS','VmHWM','RssAnon','RssFile','RssShmem','VmSwap','VmSize']}
        row['roles'][role]['io']=read_numbers(p/'io')
        if extra:
            row['roles'][role]['smaps_rollup']=read_numbers(p/'smaps_rollup')
            row['roles'][role]['numa_maps']=(p/'numa_maps').read_text() if ACTIVE.get('save_numa_snapshot') else None
    if extra:
        row['host']={'loadavg':Path('/proc/loadavg').read_text(),'stat':Path('/proc/stat').read_text(),
                     'cgroup_memory_current':int(Path('/sys/fs/cgroup/memory.current').read_text()),
                     'cgroup_memory_stat':Path('/sys/fs/cgroup/memory.stat').read_text(),
                     'cgroup_cpu_stat':Path('/sys/fs/cgroup/cpu.stat').read_text(),
                     'memory_pressure':Path('/proc/pressure/memory').read_text(),
                     'cpu_pressure':Path('/proc/pressure/cpu').read_text()}
    return row

def collect_environment():
    out=B/'environment';out.mkdir(exist_ok=True)
    commands={'uname':['uname','-a'],'lscpu':['lscpu','-J'],'java':['/usr/lib/jvm/java-11-openjdk/bin/java','-version'],
              'gcc':['g++','--version'],'cmake':['cmake','--version'],'maven':['mvn','-version'],
              'filesystem':['df','-T','/root','/tmp'],'kernel_sysctl':['sh','-c','cat /proc/sys/kernel/numa_balancing /proc/sys/vm/swappiness /proc/sys/kernel/perf_event_paranoid'],
              'git_head':['git','-C',str(REPO),'rev-parse','HEAD'],'git_status':['git','-C',str(REPO),'status','--short'],
              'git_diff':['git','-C',str(REPO),'diff','--binary'],
              'clock':['date','--iso-8601=ns']}
    for name,cmd in commands.items():
        r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);(out/(name+'.txt')).write_text(r.stdout)
    for name,path in {'os-release':'/etc/os-release','cpuinfo':'/proc/cpuinfo','meminfo':'/proc/meminfo','mountinfo':'/proc/self/mountinfo','cgroup':'/proc/self/cgroup',
                      'cgroup_memory_max':'/sys/fs/cgroup/memory.max','cgroup_cpu_max':'/sys/fs/cgroup/cpu.max','cgroup_cpuset_effective':'/sys/fs/cgroup/cpuset.cpus.effective'}.items():
        (out/(name+'.txt')).write_text(Path(path).read_text())
    for p in Path('/sys/devices/system/node').glob('node*/cpulist'):(out/(p.parent.name+'-cpulist.txt')).write_text(p.read_text())
    for p in Path('/sys/devices/system/cpu/cpu4/cache').glob('index*/size'):(out/('cpu4-cache-'+p.parent.name+'.txt')).write_text(p.read_text())
    dump(out/'runtime.json',{'clock_ticks_per_second':c['HZ'],'uname':list(os.uname()),'controller_affinity':sorted(os.sched_getaffinity(0))})
    for p in [dev.FLINK/'lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar',dev.RUNTIME/'native/libforl0_engine.so',JAR]:assert p.exists()
    assert sha(dev.FLINK/'lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar')==BACKEND_SHA
    assert sha(dev.RUNTIME/'native/libforl0_engine.so')==NATIVE_SHA
    dump(out/'artifacts.json',{str(p):sha(p) for p in [dev.FLINK/'lib/flink-statebackend-forL0-1.0-SNAPSHOT.jar',dev.RUNTIME/'native/libforl0_engine.so',JAR]})

def flags_and_configs(folder,pids):
    result={}
    for role,pid in pids.items():
        command=Path('/proc',str(pid),'cmdline').read_bytes().replace(b'\0',b' ').decode();(folder/(role+'-command.txt')).write_text(command)
        shutil.copy2(dev.RUNTIME/role/'conf/config.yaml',folder/(role+'-config.yaml'))
        f=subprocess.check_output(['/usr/lib/jvm/java-11-openjdk/bin/jcmd',str(pid),'VM.flags','-all'],text=True)
        (folder/(role+'-flags.txt')).write_text(f)
        result[role]={x.split()[1]:x.split()[3] for x in f.splitlines() if any(k in x for k in ['MaxHeapSize','InitialHeapSize','UseCompressedOops','UseCompressedClassPointers','G1HeapRegionSize','ParallelGCThreads','ConcGCThreads','UseNUMA','ActiveProcessorCount']) and len(x.split())>3}
    return result

def metric_ids():
    return {r:r for r in []}

def submit(workload,backend,folder,keys,steady=True,old=False,records=None):
    jar=B/'reproduction/benchset-1.0-SNAPSHOT.jar' if old else JAR
    cmd=[str(dev.FLINK/'bin/flink'),'run','-d','-m','127.0.0.1:8081',
         '-Dstate.backend.type='+('hashmap' if backend=='heap' else 'org.apache.flink.state.forl0.ForL0StateBackendFactory'),
         '-Dstate.backend.forl0.l0-cache.enabled=false','-Drestart-strategy.type=none','-p','8','-c','org.apache.flink.benchmark.benchset.BenchsetMain',str(jar),
         '--benchmark',workload,'--numKeys',str(keys),'--numRecords',str(records or keys),
         '--featureWidth','64','--touches','128','--window','16','--parallelism','8','--checkpointInterval','0','--outputDirectory',str(folder)]
    if steady:cmd+=['--controlDirectory',str(folder/'control')]
    elif not old:cmd+=['--materializeHistory','true']
    dump(folder/'command.json',cmd)
    t=time.monotonic();r=subprocess.run(cmd,env=dev.role_env('jm'),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=180)
    (folder/'submission.log').write_text(r.stdout)
    if r.returncode:raise RuntimeError(r.stdout)
    match=re.search(r'Job has been submitted with JobID ([0-9a-f]{32})',r.stdout)
    if not match:raise RuntimeError('No submitted job ID')
    return match[1],time.monotonic()-t

def profile_start(pids,folder):
    def one(pair):
        role,pid=pair
        with (folder/(role+'-profiler.log')).open('a') as log:
            subprocess.run([c['ASPROF'],'start','-e','cpu','-i','5ms','--cstack','dwarf','-t','-o','jfr','-f',str(folder/(role+'-full.jfr')),str(pid)],stdout=log,stderr=log,check=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(one,pids.items()))

def profile_stop(pids,folder,start,end):
    def one(pair):
        role,pid=pair
        with (folder/(role+'-profiler.log')).open('a') as log:
            subprocess.run([c['ASPROF'],'stop','-o','jfr','-f',str(folder/(role+'-full.jfr')),str(pid)],stdout=log,stderr=log,check=True)
            for fmt,suffix in [('collapsed','collapsed'),('html','html')]:
                subprocess.run(['/root/opt/async-profiler-4.4-linux-arm64/bin/jfrconv','--cpu','--threads','--from',str(start),'--to',str(end),'-o',fmt,str(folder/(role+'-full.jfr')),str(folder/(role+'-cpu.'+suffix))],stdout=log,stderr=log,check=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(one,pids.items()))

def final_artifacts(folder,jid,pids):
    for name,suffix in [('job',''),('checkpoints','/checkpoints'),('exceptions','/exceptions'),('execution-config','/config')]:dump(folder/(name+'.json'),api('/jobs/'+jid+suffix))
    for role,pid in pids.items():
        for log in (dev.RUNTIME/role/'log').glob('gc-'+str(pid)+'.log*'):shutil.copy2(log,folder/(role+'-'+log.name))
        for log in (dev.RUNTIME/role/'log').glob('*'+str(pid)+'*'):
            if log.is_file() and log.stat().st_size<32*1024*1024 and not log.name.startswith('gc-'):shutil.copy2(log,folder/(role+'-'+log.name))
    c['contract'].validate_graph(folder/'topology.json',ACTIVE['workload'])
    assert json.loads((folder/'checkpoints.json').read_text())['counts']['total']==0

def run_steady(spec,category,keys=524288,warm=60,measure=120):
    ACTIVE.clear();ACTIVE.update(spec)
    name=f"{spec['workload']}-m{spec['memory_gib']}-{spec['backend']}-r{spec['repeat']}"
    folder=B/category/name
    if (folder/'measurement.json').exists():return json.loads((folder/'measurement.json').read_text())
    folder.mkdir(parents=True,exist_ok=False);(folder/'control').mkdir()
    dump(B/'status.json',{'state':'running','category':category,'condition':spec,'phase':'cluster-start','time':time.time()})
    stop();dev.start();pids=c['pids']()
    ACTIVE['jm_pid']=int(next((dev.RUNTIME/'jm/pid').glob('*.pid')).read_text().strip())
    flags=flags_and_configs(folder,{**pids,'jm':ACTIVE['jm_pid']})
    baseline=snapshot(pids,True);dump(folder/'baseline.json',baseline)
    events=[];snaps=[];errors=[];stop_watch=threading.Event();tms=api('/taskmanagers')['taskmanagers']
    metric_paths={tm['id']:[m['id'] for m in api('/taskmanagers/'+tm['id']+'/metrics') if any(x in m['id'] for x in ['Status.JVM.Memory.','Status.JVM.GarbageCollector.'])] for tm in tms}
    def watch():
        seq=0
        try:
            with gzip.open(folder/'thread-samples.jsonl.gz','wt',compresslevel=3) as out,gzip.open(folder/'jvm-metrics.jsonl.gz','wt',compresslevel=3) as metrics:
                while not stop_watch.is_set():
                    t=time.monotonic();row=snapshot(pids,seq%4==0);snaps.append(row);out.write(json.dumps(row,separators=(',',':'))+'\n')
                    if any(v['violations'] for v in row['roles'].values()):errors.append('CPU affinity violation')
                    if seq%10==0:
                        for tm,ids in metric_paths.items():
                            try:
                                vals=api('/taskmanagers/'+tm+'/metrics?get='+','.join(ids))
                                metrics.write(json.dumps({'time_ms':int(time.time()*1000),'tm_id':tm,'values':vals},separators=(',',':'))+'\n')
                            except Exception as exc:events.append({'metric_error':str(exc),'time_ms':int(time.time()*1000)})
                    seq+=1;stop_watch.wait(max(0,.5-(time.monotonic()-t)))
        except Exception:errors.append(traceback.format_exc())
    watcher=threading.Thread(target=watch,daemon=True);watcher.start()
    profiled=spec.get('profiled',category=='profiles');profiler_active=False
    try:
        begin=time.monotonic();jid,submission_seconds=submit(spec['workload'],spec['backend'],folder,keys)
        expected=keys*MULTIPLIERS[spec['workload']];deadline=time.monotonic()+3600
        while time.monotonic()<deadline:
            job=api('/jobs/'+jid)
            if job['state'] in ['FAILED','CANCELED','FINISHED']:raise RuntimeError('Job ended before coverage barrier: '+str(job))
            ready=len(list((folder/'control').glob('ready-*')))==8
            sinks=[v for v in job['vertices'] if v['name'].startswith('Sink:')]
            received=sum(v.get('metrics',{}).get('read-records',0) for v in sinks)
            if ready and received==expected:break
            time.sleep(.5)
        else:raise TimeoutError('coverage barrier')
        coverage_end=time.monotonic();dump(folder/'coverage-job.json',job)
        dump(folder/'coverage-memory.json',snapshot(pids,True))
        native_proof={}
        for role,pid in pids.items():
            mappings=sorted({line.split()[-1] for line in Path('/proc',str(pid),'maps').read_text().splitlines() if 'libforl0_engine.so' in line})
            native_proof[role]={'paths':mappings,'sha256':{p:sha(p) for p in mappings if Path(p).is_file()}}
            if spec['backend']=='forl0':
                assert mappings==[str(dev.RUNTIME/'native/libforl0_engine.so')],native_proof
                assert all(v==NATIVE_SHA for v in native_proof[role]['sha256'].values())
        dump(folder/'native-loaded.json',native_proof)
        for role,pid in pids.items():
            with gzip.open(folder/(role+'-coverage-numa-maps.txt.gz'),'wt') as out:out.write(Path('/proc',str(pid),'numa_maps').read_text())
        dump(B/'status.json',{'state':'running','category':category,'condition':spec,'phase':'warmup','time':time.time()})
        (folder/'control/release').write_text('release\n');release=time.monotonic()
        while time.monotonic()-release<warm:time.sleep(.25)
        if profiled:profile_start(pids,folder);profiler_active=True
        start=int(time.time()*1000)+3000;end=start+measure*1000
        dump(folder/'control/window.json',[start,end]);dump(folder/'measurement-window.json',{'start_ms':start,'end_ms':end})
        dump(B/'status.json',{'state':'running','category':category,'condition':spec,'phase':'measurement','start_ms':start,'end_ms':end})
        next_detail=time.monotonic()
        while time.time()*1000<end:
            if time.monotonic()>=next_detail:
                j=api('/jobs/'+jid);events.append({'time_ms':int(time.time()*1000),'job':j});next_detail=time.monotonic()+5
                if j['state']!='RUNNING':raise RuntimeError('Job left RUNNING during measurement')
            time.sleep(.2)
        if profiled:profile_stop(pids,folder,start,end);profiler_active=False
        # Retained-footprint diagnostic is strictly after the timed window.
        (folder/'control/pause').write_text('pause\n');deadline=time.monotonic()+600
        while time.monotonic()<deadline:
            paused=list((folder/'control').glob('paused-*.json'))
            if len(paused)==8:
                emitted=sum(json.loads(p.read_text())['records'] for p in paused)
                drained=api('/jobs/'+jid)
                received=sum(v.get('metrics',{}).get('read-records',0) for v in drained['vertices'] if v['name'].startswith('Sink:'))
                if received==emitted*MULTIPLIERS[spec['workload']]:break
            time.sleep(.5)
        else:raise TimeoutError('post-window quiescence')
        dump(folder/'retained-before-gc.json',snapshot(pids,True))
        def full_gc(pair):
            role,pid=pair
            t=time.monotonic();result=subprocess.run(['/usr/lib/jvm/java-11-openjdk/bin/jcmd',str(pid),'GC.run'],capture_output=True,text=True,check=True)
            (folder/(role+'-post-window-full-gc.txt')).write_text(result.stdout+result.stderr)
            heap=subprocess.check_output(['/usr/lib/jvm/java-11-openjdk/bin/jcmd',str(pid),'GC.heap_info'],text=True)
            (folder/(role+'-retained-heap-info.txt')).write_text(heap)
            return role,{'diagnostic_seconds':time.monotonic()-t,'heap_info':heap}
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:gc_diagnostic=dict(pool.map(full_gc,pids.items()))
        dump(folder/'retained-after-gc.json',snapshot(pids,True))
        def histogram(pair):
            role,pid=pair
            result=subprocess.run(['/usr/lib/jvm/java-11-openjdk/bin/jcmd',str(pid),'GC.class_histogram','-all'],capture_output=True,text=True,timeout=120,check=True)
            (folder/(role+'-retained-class-histogram.txt')).write_text(result.stdout+result.stderr)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(histogram,pids.items()))
        for role,pid in pids.items():
            with gzip.open(folder/(role+'-retained-numa-maps.txt.gz'),'wt') as out:out.write(Path('/proc',str(pid),'numa_maps').read_text())
        (folder/'control/stop').write_text('stop\n');deadline=time.monotonic()+600
        while time.monotonic()<deadline:
            job=api('/jobs/'+jid)
            if job['state'] in ['FINISHED','FAILED','CANCELED']:break
            time.sleep(.5)
        else:raise TimeoutError('drain completion')
        if job['state']!='FINISHED':raise RuntimeError('job failed at drain: '+str(job))
        final_artifacts(folder,jid,pids)
        source=[json.loads(p.read_text()) for p in (folder/'control').glob('source-*.json')]
        sinks=[json.loads(p.read_text()) for p in (folder/'sinks').glob('*.json')]
        assert len(source)==8
        total_source=sum(x['records'] for x in source);total_sink=sum(x['records'] for x in sinks)
        signature=c['contract'].signed(sum(x['source_signature'] for x in sinks))
        assert total_sink==total_source*MULTIPLIERS[spec['workload']]
        assert signature==c['contract'].signed(sum(x['signature'] for x in source)*MULTIPLIERS[spec['workload']])
        oracle=json.loads(subprocess.check_output(['/usr/lib/jvm/java-11-openjdk/bin/java','-Xmx256m','-XX:ActiveProcessorCount=4','-cp',str(B/'source-oracle')+':'+str(JAR),'PaperSourceOracle',str(folder/'control'),str(keys),spec['workload']],text=True))
        actual_projection=c['contract'].signed(sum(x['feature_projection'] for x in sinks))
        assert actual_projection==oracle['expected_feature_projection'],(actual_projection,oracle)
        oracle['actual_feature_projection']=actual_projection;oracle['matches']=True;dump(folder/'source-oracle.json',oracle)
        assert all(x['measurement_start_ms']==start and x['measurement_end_ms']==end for x in sinks)
        counts=[sum(x['second_counts'][i] for x in sinks) for i in range(measure)]
        measured=sum(x['measured_records'] for x in sinks);assert measured==sum(counts)
        r={**spec,'keys':keys,'width':64,'touches':128,'window':16,'backend_revision':'r30','profiled':profiled,
           'job_id':jid,'directory':str(folder),'job_sha256':sha(JAR),'backend_sha256':BACKEND_SHA,'native_sha256':NATIVE_SHA,
           'measurement_start_ms':start,'measurement_end_ms':end,'measurement_seconds':measure,
           'warmup_seconds_minimum':warm,'coverage_seconds_including_submission':coverage_end-begin,
           'job_seconds':job['duration']/1000,'submission_seconds':submission_seconds,
           'source_records_total':total_source,'sink_records_total':total_sink,'source_signature':oracle['source_signature'],'sink_source_signature':signature,
           'source_oracle_matches':True,'feature_projection':actual_projection,
           'measured_sink_records':measured,'sink_multiplier':MULTIPLIERS[spec['workload']],
           'input_equivalent_records':measured/MULTIPLIERS[spec['workload']],
           'throughput_input_records_s':measured/MULTIPLIERS[spec['workload']]/measure,
           'per_second_sink_records':counts,'jvm_flags':flags,
           'roles_capture':c['summarize_threads'](snaps[0],snaps[-1],snaps),
           'checkpoint_count':0,'forl0_l0_enabled':False,'config_budget_is_hard_rss_limit':False}
        r['post_window_full_gc']=gc_diagnostic
    except Exception:
        dump(folder/'failure.json',{'traceback':traceback.format_exc(),'condition':spec})
        (folder/'control/stop').write_text('stop\n');(folder/'control/release').write_text('release\n')
        raise
    finally:
        if profiler_active:
            for role,pid in pids.items():subprocess.run([c['ASPROF'],'stop',str(pid)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        stop_watch.set();watcher.join(timeout=30);dump(folder/'events.json',events)
    if errors:raise RuntimeError(errors)
    assert not any(v['affinity_violations'] for v in r['roles_capture'].values())
    dump(folder/'measurement.json',r)
    print('STEADY_PASS',category,name,round(r['throughput_input_records_s'],2),'records/s',flush=True)
    return r

def oracle():
    ACTIVE.update(memory_gib=32,backend='heap',workload='wc');stop();dev.start()
    rows=[]
    for workload in WORKLOADS:
        pair=[];ACTIVE['workload']=workload
        for old in [True,False]:
            folder=B/'oracle'/(workload+('-original' if old else '-steady-finite'));folder.mkdir(parents=True,exist_ok=True)
            jid,_=submit(workload,'heap',folder,4096,steady=False,old=old,records=16384)
            while True:
                job=api('/jobs/'+jid)
                if job['state'] in ['FINISHED','FAILED','CANCELED']:break
                time.sleep(.25)
            dump(folder/'job.json',job);assert job['state']=='FINISHED'
            c['contract'].validate_graph(folder/'topology.json',workload)
            sinks=[json.loads(p.read_text()) for p in (folder/'sinks').glob('*.json')]
            row={k:c['contract'].signed(sum(x[k] for x in sinks)) for k in ['records','feature_projection','source_signature']}
            pair.append(row);rows.append({'workload':workload,'original':old,'sink':row})
        assert pair[0]==pair[1],(workload,pair)
        print('ORACLE_PASS',workload,flush=True)
    dump(B/'oracle/summary.json',rows)

def plan():
    rng=random.Random(20261104);formal=[];profiles=[]
    for repeat in range(1,4):
        conditions=[(w,m) for w in WORKLOADS for m in MEMORIES];rng.shuffle(conditions)
        for w,m in conditions:
            order=['heap','forl0'] if (repeat+MEMORIES.index(m)+WORKLOADS.index(w))%2 else ['forl0','heap']
            for backend in order:formal.append({'workload':w,'memory_gib':m,'backend':backend,'repeat':repeat})
    for repeat in range(1,3):
        conditions=[(w,m) for w in WORKLOADS for m in [32,64]];rng.shuffle(conditions)
        for w,m in conditions:
            for backend in (['heap','forl0'] if repeat==1 else ['forl0','heap']):profiles.append({'workload':w,'memory_gib':m,'backend':backend,'repeat':repeat})
    return {'seed':20261104,'formal':formal,'profiles':profiles,'memory_gib_per_tm':MEMORIES,
            'formal_repeats':3,'profile_repeats':2,'warmup_seconds':60,'measurement_seconds':120,
            'main_backends':['heap','forl0'],'rocksdb_micro_only':True}

def main():
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['oracle','pilot','formal','profiles','all'],default='all');a=p.parse_args()
    collect_environment();schedule=plan()
    if (B/'plan.json').exists():assert json.loads((B/'plan.json').read_text())==schedule
    else:dump(B/'plan.json',schedule)
    if a.mode=='oracle':oracle();return
    if a.mode=='pilot':
        rows=[]
        for w in WORKLOADS:
            for backend in ['heap','forl0']:
                rows.append(run_steady({'workload':w,'memory_gib':32,'backend':backend,'repeat':1},'steady-pilot',keys=4096,warm=2,measure=5))
                dump(B/'steady-pilot/summary.json',rows)
        return
    for category,key in [('formal','formal'),('profiles','profiles')]:
        if a.mode not in ['all',category]:continue
        rows=[]
        for spec in schedule[key]:
            rows.append(run_steady(spec,category));dump(B/category/'summary.json',rows)
        print('CATEGORY_COMPLETE',category,len(rows),flush=True)
    restore_default();dump(B/'status.json',{'state':'complete','time':time.time()});print('PAPER_CAMPAIGN_COMPLETE',flush=True)

if __name__=='__main__':main()
