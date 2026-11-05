"""Finish the paper artifact after all serial measurements, including deferred Rocks reads."""
from pathlib import Path
import hashlib,itertools,json,re,subprocess,tarfile,time,traceback,urllib.parse
B=Path('/root/briskstate-setup/paper-experiments-20261104')
PY='/root/opt/briskstate-analysis-venv/bin/python'
def dump(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
try:
    while not (B/'MICRO_MAP_READ_COMPLETE').exists():
        for name in ['campaign.log','micro-map-read.log']:
            if (B/name).exists() and 'Traceback' in (B/name).read_text():raise RuntimeError('Inspect failure in '+name)
        time.sleep(30)
    for script in ['analyze.py','memory_mechanism.py','micro_analysis.py','build_report.py']:
        subprocess.run([PY,str(B/script)],check=True,cwd=B)
    formal=json.loads((B/'analysis/formal-runs.json').read_text());profiles=json.loads((B/'analysis/profiles-runs.json').read_text());micro=json.loads((B/'analysis/micro-results.json').read_text())
    assert len(formal)==210 and len(profiles)==56 and len(micro)==13
    assert all(len(r['rocksdb']['fork_scores'])==2 for r in micro)
    assert all(r['source_oracle_matches'] and r['checkpoint_count']==0 for r in formal+profiles)
    assert all(not role['affinity_violations'] for r in formal+profiles for role in r['roles_capture'].values())
    workloads=['wc','fd','sd','tm','lg','vs','lr']
    condition=lambda r:(r['workload'],r['memory_gib'],r['backend'],r['repeat'])
    assert {condition(r) for r in formal}==set(itertools.product(workloads,[32,40,48,56,64],['heap','forl0'],[1,2,3]))
    assert {condition(r) for r in profiles}==set(itertools.product(workloads,[32,64],['heap','forl0'],[1,2]))
    assert {r['job_sha256'] for r in formal+profiles}=={'7f6929397ca2187d14b26ceb52ce1dd1a7ab7e276738b9606e45a65f3d337c15'}
    review={'formal_runs':210,'profile_runs':56,'selected_micro_forks':78,'source_oracle_passes':sum(r['source_oracle_matches'] for r in formal+profiles),
            'checkpoint_count':sum(r['checkpoint_count'] for r in formal+profiles),'same_job_sha256':formal[0]['job_sha256'],
            'minimum_profile_nongc_cpu_coverage':min(r['roles_window'][role]['sampled_nongc_cpu_coverage'] for r in profiles for role in ['tm1','tm2']),
            'unmatched_profile_stack_samples':sum(r['roles_window'][role]['unmatched_stack_samples'] for r in profiles for role in ['tm1','tm2']),
            'formal_window_trend_flags':sum(abs((r['last_first_30s_ratio'] or 1)-1)>.15 for r in formal),
            'scope':'210 main + 56 independent CPU diagnostics + 13 operations x 3 backends x 2 forks; unrequested supplementary runs excluded'}
    dump(B/'QUALITY_REVIEW.json',review)
    dump(B/'status.json',{'state':'complete','category':'original-scope','formal_completed':210,'profiles_completed':56,'micro_operations':13,'time_ms':int(time.time()*1000)})
    excluded={'MANIFEST.sha256','finalization.log','FINALIZATION_FAILURE.json','READY_DELIVERY.json'}
    files=[p for p in sorted(B.rglob('*')) if p.is_file() and p.name not in excluded and '__pycache__' not in p.parts and not p.name.endswith('.tmp')]
    (B/'MANIFEST.sha256').write_text(''.join(sha(p)+'  '+str(p.relative_to(B))+'\n' for p in files))
    links=[]
    for u in re.findall(r'!?\[[^\]]*\]\(([^)]+)\)',(B/'REPORT.md').read_text()):
        if u.startswith(('http:','https:','#')):continue
        target=B/urllib.parse.unquote(u.split('#',1)[0]);assert target.exists(),u;links.append(u)
    archive=B.parent/(B.name+'-delivery.tar.gz')
    with tarfile.open(archive,'w:gz',compresslevel=5) as tar:
        for p in files+[B/'MANIFEST.sha256']:
            tar.add(p,arcname=str(p.relative_to(B)),recursive=False)
    ready={'formal_runs':len(formal),'profile_runs':len(profiles),'micro_operations':len(micro),'manifest_files':len(files),
           'report_links_verified':len(links),'archive':str(archive),'archive_bytes':archive.stat().st_size,'archive_sha256':sha(archive),
           'frozen_micro_jar_sha256':'bc258cea2877431005b499210096217e0ca6adcd24ddcb142976c87c26ec6ad6',
           'report':str(B/'REPORT.md'),'completed_time_ms':int(time.time()*1000)}
    dump(B.parent/(B.name+'-READY.json'),ready);dump(B/'READY_DELIVERY.json',ready)
    print('PAPER_ARTIFACT_READY',json.dumps(ready),flush=True)
except Exception:
    dump(B/'FINALIZATION_FAILURE.json',{'traceback':traceback.format_exc(),'time_ms':int(time.time()*1000)})
    raise
