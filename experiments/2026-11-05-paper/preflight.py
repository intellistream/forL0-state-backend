import concurrent.futures, importlib.util, json, os, subprocess, time
from pathlib import Path

B = Path('/root/briskstate-setup/paper-experiments-20261104')
src = Path('/root/briskstate-setup/fair-standard-api-20261103/collect_fair.py').read_text()
src = src.replace("factory='org.apache.flink.state.forl0.ForL0StateBackendFactory' if backend=='forl0' else 'hashmap'", "factory={'forl0':'org.apache.flink.state.forl0.ForL0StateBackendFactory','heap':'hashmap','rocksdb':'rocksdb'}[backend]")
src = src.replace('time.monotonic()-t0<1800', 'time.monotonic()-t0<14400')
ns = {'__name__':'paper_capture'}
exec(compile(src, '<frozen-collector>', 'exec'), ns)
dev=ns['dev']; original_configure=dev.configure
def configure():
    original_configure()
    for role in dev.ROLES:
        p=dev.RUNTIME/role/'conf/config.yaml'
        c={k:json.loads(v) for k,v in (line.split(': ',1) for line in p.read_text().splitlines())}
        c.pop('taskmanager.memory.managed.size',None)
        c.update({'taskmanager.memory.task.off-heap.size':'0g',
                  'taskmanager.memory.managed.fraction':0.4,
                  'state.backend.rocksdb.memory.managed':True,
                  'state.backend.rocksdb.localdir':str(B/'rocks-local'),
                  'state.backend.rocksdb.predefined-options':'DEFAULT',
                  'state.backend.rocksdb.checkpoint.transfer.thread.num':4})
        p.write_text(''.join(k+': '+json.dumps(v)+'\n' for k,v in c.items()))
dev.configure=configure
def stop():
    def one(role):
        subprocess.run([str(dev.FLINK/'bin'/dev.ROLES[role][1]),'stop'],check=True,env=dev.role_env(role),stdin=subprocess.DEVNULL)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(one,['tm2','tm1','jm']))
dev.stop=stop
if __name__=='__main__':
    assert dev.api('/overview')['jobs-running']==0
    ns['BASE']=B/'pilot';ns['BASE'].mkdir(exist_ok=True)
    ns['ORIGINAL']=B/'reproduction/benchset-1.0-SNAPSHOT.jar'
    stop();dev.start()
    t=time.monotonic()
    r=ns['run_one']('wc','rocksdb',1,records=524288,touches=128,profiled=False,fresh=False)
    (B/'pilot/summary.json').write_text(json.dumps([r],indent=2)+'\n')
    print('PILOT_WALL',time.monotonic()-t,flush=True)
