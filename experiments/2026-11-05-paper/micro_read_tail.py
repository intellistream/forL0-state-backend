from pathlib import Path
import subprocess,time
B=Path('/root/briskstate-setup/paper-experiments-20261104')
while True:
    text=(B/'campaign.log').read_text() if (B/'campaign.log').exists() else ''
    if 'PAPER_DRIVER_COMPLETE' in text:break
    if 'Traceback' in text:raise RuntimeError('Inspect the campaign failure before continuing')
    time.sleep(5)
import campaign
campaign.stop()
subprocess.run(['python3','-u',str(B/'run_rocks_micro.py'),'--long-map-read'],check=True)
campaign.restore_default()
(B/'MICRO_MAP_READ_COMPLETE').write_text('Long Map read completed; preflight acceptance can be considered.\n')
