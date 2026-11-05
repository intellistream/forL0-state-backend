from pathlib import Path
import ast, hashlib, json, subprocess, argparse, time

B=Path('/root/briskstate-setup/paper-experiments-20261104')
OLD=Path('/root/briskstate-setup/deep-opt-20261103')
p=argparse.ArgumentParser();p.add_argument('--pilot',action='store_true');p.add_argument('--long-writes',action='store_true');p.add_argument('--long-map-read',action='store_true');a=p.parse_args()
source=(B/'reproduction/observer_support.py').read_text().replace("if 'libforl0_engine.so' in line", "if 'libforl0_engine.so' in line or 'librocksdbjni' in line")
if a.long_writes or a.long_map_read:
    source=source.replace("record['forked_jmh']=", "record.setdefault('io_samples',[]).append({'time_ms':int(time.time()*1000),'io':(root/'io').read_text(),'process_stat':(root/'stat').read_text(),'threads':[(d/'stat').read_text() for d in (root/'task').iterdir()]})\n     record['forked_jmh']=")
observer=ast.parse(source)
fn=next(n for n in observer.body if isinstance(n,ast.FunctionDef) and n.name=='measured_process')
exec(compile(ast.Module(body=[fn],type_ignores=[]),'<frozen-observer>','exec'))
out=B/('micro-pilot' if a.pilot else 'micro-rocksdb-long-map-read' if a.long_map_read else 'micro-rocksdb-long-writes' if a.long_writes else 'micro-rocksdb');out.mkdir(exist_ok=True)
cp=':'.join(str(x) for x in [B/'micro-factory/rocksdb-factory-overlay.jar',B/'reproduction/flink-statebackend-forL0-1.0-SNAPSHOT.jar',B/'reproduction/micro-measured.jar'])
java=['taskset','-c','4-7','/usr/lib/jvm/java-11-openjdk/bin/java','-Xms8g','-Xmx8g','-XX:ActiveProcessorCount=4','-XX:+UnlockDiagnosticVMOptions','-XX:+DebugNonSafepoints','-XX:+PreserveFramePointer','-Dorg.slf4j.simpleLogger.defaultLogLevel=error','-Djava.library.path='+str(B/'reproduction'),'-Djava.io.tmpdir='+str(out/'tmp'),'-cp',cp,'org.openjdk.jmh.Main']
(out/'tmp').mkdir(exist_ok=True)
ops=['valueGet','valueUpdate','listAdd','listAddAll','listGet','listIterate','listUpdate','mapAdd','mapGet','mapPutAll','mapRemove','mapUpdate','valueAdd']
commands=[]
selected=['mapGet'] if a.pilot or a.long_map_read else [op for op in ops if op not in ['valueGet','listGet','listIterate','mapGet']] if a.long_writes else ops
for op in selected:
    for repeat in range(1 if a.pilot else 2):
        name=f'{op}-{repeat}-ROCKSDB';dest=out/(name+'.json')
        if dest.exists() and json.loads(dest.read_text()):continue
        wi,w,mi,r=(1,'1s',1,'1s') if a.pilot else (240,'5s',12,'5s') if a.long_map_read else (24,'5s',12,'5s') if a.long_writes else ((10,'5s',5,'5s') if op=='valueAdd' else (3,'3s',5,'3s'))
        cmd=java+['LongStateMicrobenchmarks.'+op+'$','-p','backendType=ROCKSDB','-p','keyCount=1048576','-f','1','-wi',str(wi),'-w',w,'-i',str(mi),'-r',r,'-t','1','-prof','gc','-rf','json','-rff',str(dest),'-foe','true']
        commands.append(cmd);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
        measured_process(cmd,out/(name+'.log'),out/(name+'-processes.json'))
        result=json.loads(dest.read_text())[0]
        print(json.dumps({'op':op,'repeat':repeat,'throughput_ops_ms':result['primaryMetric']['score'],'allocation_B_op':result['secondaryMetrics']['gc.alloc.rate.norm']['score']}),flush=True)
print('ROCKS_MICRO_COMPLETE',flush=True)
