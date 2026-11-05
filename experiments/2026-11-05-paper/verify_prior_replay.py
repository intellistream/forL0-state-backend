"""Validate the independent mathematical oracle against real previous long runs."""
from pathlib import Path
import json,subprocess
B=Path('/root/briskstate-setup/paper-experiments-20261104');prior=json.loads(Path('/root/briskstate-setup/fair-long-20261103/summary.json').read_text())
out=B/'oracle-replay-prior';out.mkdir(exist_ok=True);rows=[]
signed=lambda n:((n+(1<<63))%(1<<64))-(1<<63)
for w in ['wc','fd','sd','tm','lg','vs','lr']:
 r=next(x for x in prior if x['workload']==w and not x['profiled'] and x['backend']=='heap')
 folder=out/w;control=folder/'control';control.mkdir(parents=True,exist_ok=True)
 for index in range(8):(control/('source-'+str(index)+'.json')).write_text(json.dumps({'records':r['records']//8+(index<r['records']%8)}))
 z=json.loads(subprocess.check_output(['/usr/lib/jvm/java-11-openjdk/bin/java','-Xmx256m','-XX:ActiveProcessorCount=4','-cp',str(B/'source-oracle')+':'+str(B/'steady-job/paper-steady-benchset.jar'),'PaperSourceOracle',str(control),str(r['keys']),w],text=True))
 multiplier={'wc':1,'fd':1,'sd':1,'tm':1,'lg':3,'vs':8,'lr':12}[w]
 assert r['sink']['feature_projection']==z['expected_feature_projection'],(w,r['sink'],z)
 assert r['sink']['source_signature']==signed(multiplier*z['source_signature'])
 row={'workload':w,'original_directory':r['directory'],'original_job_id':r['job_id'],'original_job_sha256':r['jar_sha256'],
      'records':r['records'],'original_sink':r['sink'],'oracle':z,'matches':True}
 rows.append(row);(folder/'comparison.json').write_text(json.dumps(row,indent=2)+'\n');print('PRIOR_REPLAY_PASS',w,flush=True)
(out/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
