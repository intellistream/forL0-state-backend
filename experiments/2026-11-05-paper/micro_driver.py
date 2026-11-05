from pathlib import Path
import subprocess,importlib.util
B=Path('/root/briskstate-setup/paper-experiments-20261104')
assert (B/'micro-factory/proof.json').exists()
import preflight
assert preflight.dev.api('/overview')['jobs-running']==0
preflight.stop()
subprocess.run(['python3','-u','run_rocks_micro.py','--pilot'],check=True)
subprocess.run(['python3','-u','run_rocks_micro.py'],check=True)
print('PAPER_MICRO_DRIVER_COMPLETE',flush=True)
