"""Display evidence already recorded by a campaign; never starts a probe."""
import json
from pathlib import Path
import sys

data = json.loads(Path(sys.argv[1]).read_text())
diagnostic = data.get('l0_diagnostics') or {}
sdk = data.get('l0_sdk') or {}
blocks = []
for arm, record in (data.get('l0_preflight') or {}).items():
    if record.get('status') == 'passed':
        continue
    lines = []
    for path, text in (record.get('evidence') or {}).items():
        if Path(path).name in ['tm1.log', 'tm2.log']:
            lines.append(Path(path).name + '：' + ' | '.join(str(text).splitlines()[-3:]))
    if lines:
        blocks.append('L0预检原始错误（%g MiB共享）：\n%s' %
                      (record.get('requested_total_bytes', 0) / 1024 ** 2, '\n'.join(lines)))
if blocks:
    print('\n最终SDK路径：' + str(diagnostic.get('selected_library') or sdk.get('library') or '未记录'))
    print('\n'.join(blocks))
