"""Serial execution avoids CPU/memory interference between macro and micro timing."""
from pathlib import Path
import json,subprocess,time
B=Path('/root/briskstate-setup/paper-experiments-20261104')
while True:
    text=(B/'micro-formal.log').read_text()
    if 'ROCKS_MICRO_COMPLETE' in text:break
    if 'Traceback' in text:raise RuntimeError('RocksDB microbenchmark requires inspection')
    time.sleep(5)
subprocess.run(['python3','-u',str(B/'run_rocks_micro.py'),'--long-writes'],check=True,stdout=(B/'micro-long-writes.log').open('a'),stderr=subprocess.STDOUT)
subprocess.run(['python3','-u',str(B/'campaign.py'),'--mode','oracle'],check=True)
subprocess.run(['python3','-u',str(B/'campaign.py'),'--mode','pilot'],check=True)
import campaign
for backend in ['heap','forl0']:
    campaign.run_steady({'workload':'wc','memory_gib':32,'backend':backend,'repeat':0,'profiled':True},'profile-pilot',keys=4096,warm=2,measure=5)
(B/'PREFLIGHT_COMPLETE').write_text('Real finite-output, topology, steady-window and JFR checks finished.\n')
while not (B/'PREFLIGHT_ACCEPTED').exists():time.sleep(5)
subprocess.run(['python3','-u',str(B/'campaign.py'),'--mode','all'],check=True)
print('PAPER_DRIVER_COMPLETE',flush=True)
