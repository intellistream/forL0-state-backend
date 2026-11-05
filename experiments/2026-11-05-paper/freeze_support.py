from pathlib import Path
import hashlib,json,shutil
B=Path('/root/briskstate-setup/paper-experiments-20261104')
R=Path('/root/forL0-state-backend');out=B/'reproduction';out.mkdir(exist_ok=True)
sources={
 'capture_support.py':Path('/root/briskstate-setup/fair-standard-api-20261103/collect_fair.py'),
 'observer_support.py':Path('/root/briskstate-setup/deep-opt-20261103/run_final_micro.py'),
 'repo/docker/lib/briskstate_dev.py':R/'docker/lib/briskstate_dev.py',
 'repo/benchmark/scripts/run_benchset.py':R/'benchmark/scripts/run_benchset.py',
 'repo/benchmark/benchset/reference/topology-contract.json':R/'benchmark/benchset/reference/topology-contract.json',
 'micro-measured.jar':Path('/root/briskstate-setup/deep-opt-20261103/reproduction/micro-measured.jar'),
 'flink-official.sha512':Path('/root/briskstate-setup/flink-official.sha512')}
proof={}
for name,p in sources.items():
 q=out/name;q.parent.mkdir(parents=True,exist_ok=True)
 digest=hashlib.sha256(p.read_bytes()).hexdigest()
 if q.exists():assert hashlib.sha256(q.read_bytes()).hexdigest()==digest,(q,'support changed during campaign')
 else:shutil.copy2(p,q)
 proof[name]={'source':str(p),'sha256':digest}
for name in ['r30-source','build-evidence']:
 old=Path('/root/briskstate-setup/micro-opt-20261104');source=old/name
 if not source.exists():source=old/'delivery'/name
 shutil.copytree(source,out/name,dirs_exist_ok=True)
(out/'support-provenance.json').write_text(json.dumps(proof,indent=2)+'\n')
print('SUPPORT_FROZEN',len(proof),flush=True)
