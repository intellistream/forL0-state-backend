from pathlib import Path
import datetime,json,time
B=Path('/root/briskstate-setup/paper-experiments-20261104')
tz=datetime.timezone(datetime.timedelta(hours=8))
while True:
    try:status=json.loads((B/'status.json').read_text())
    except (OSError,ValueError):status={}
    row={'time_shanghai':datetime.datetime.now(tz).isoformat(timespec='seconds'),
         'formal_completed':len(list((B/'formal').glob('*/measurement.json'))),'formal_total':210,
         'profiles_completed':len(list((B/'profiles').glob('*/measurement.json'))),'profiles_total':56,
         'phase':status.get('phase',status.get('state')),'condition':status.get('condition'),
         'formal_failures':[str(p.relative_to(B)) for p in (B/'formal').glob('*/failure.json')],
         'profile_failures':[str(p.relative_to(B)) for p in (B/'profiles').glob('*/failure.json')],
         'long_map_reads_completed':sum(p.stat().st_size>4 for p in (B/'micro-rocksdb-long-map-read').glob('*-ROCKSDB.json')),
         'artifact_ready':(B/'READY_DELIVERY.json').exists(),'finalization_failure':(B/'FINALIZATION_FAILURE.json').exists()}
    print(json.dumps(row,ensure_ascii=False),flush=True)
    if row['artifact_ready'] or row['finalization_failure'] or row['formal_failures'] or row['profile_failures']:break
    time.sleep(60)
