"""Read-only L0 discovery; uses the offline Python and never initializes a pool."""
import os
from pathlib import Path
import re
import stat
import struct
import subprocess
import time


def command(argv, env, timeout=10):
    try:
        proc = subprocess.run(argv, env=env, capture_output=True, text=True,
                              errors='replace', timeout=timeout)
        return dict(argv=[str(x) for x in argv], exit_code=proc.returncode,
                    stdout=proc.stdout[-32768:], stderr=proc.stderr[-8192:], timed_out=False)
    except subprocess.TimeoutExpired as error:
        def decode(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else value or ''
        return dict(argv=[str(x) for x in argv], exit_code=None,
                    stdout=decode(error.stdout)[-32768:], stderr=decode(error.stderr)[-8192:], timed_out=True)
    except OSError as error:
        return dict(argv=[str(x) for x in argv], exit_code=None, stdout='', stderr=str(error), timed_out=False)


def file_info(path):
    path = Path(path)
    info = dict(path=str(path), exists=False)
    try:
        value = path.stat()
        info.update(exists=True, real_path=str(path.resolve()), mode=stat.filemode(value.st_mode),
                    uid=value.st_uid, gid=value.st_gid, readable=os.access(path, os.R_OK),
                    writable=os.access(path, os.W_OK), character_device=stat.S_ISCHR(value.st_mode))
        if stat.S_ISCHR(value.st_mode):
            info['device_number'] = '%d:%d' % (os.major(value.st_rdev), os.minor(value.st_rdev))
            info['sysfs_path'] = str((Path('/sys/dev/char') / info['device_number']).resolve())
    except OSError as error:
        info['error'] = str(error)
    return info


def inspect_l0(runtime, env, progress=None):
    started = time.monotonic()
    package = Path(__file__).resolve().parents[1]
    repo = package.parents[1]
    report = dict(schema_version=1, uid=os.getuid(), gid=os.getgid(),
                  home=env.get('HOME'), sudo_user=env.get('SUDO_USER'), cwd=str(Path.cwd()),
                  repository=str(repo), hostname=os.uname().nodename,
                  kernel=os.uname().release, architecture=os.uname().machine,
                  overrides={name: env.get(name) for name in
                             ['L0_DEVICE_HOST_PATH', 'L0_MEMPOOL_LIB_HOST_PATH', 'LD_LIBRARY_PATH',
                              'FORL0_L0_SEARCH_ROOTS']}, issues=[])
    if progress:
        progress('自动检查L0设备、驱动和SDK安装位置')
    # Keep main's fast lookup rules; the expanded search below has a time limit.
    detection_env = dict(env, FORL0_L0_SEARCH_ROOTS='/__briskstate_skip_unbounded_search__',
                         FORL0_NUMA_SEARCH_ROOTS='/__briskstate_skip_unbounded_search__')
    detector = command(['bash', '-c', 'source "$1"; forl0_detect_l0_environment; detected=$?; '
                        'forl0_print_l0_detection; printf "\\nDEVICE=%s\\nLIBRARY=%s\\n" '
                        '"$FORL0_L0_DEVICE_PATH" "$FORL0_L0_LIBRARY_PATH"; exit "$detected"',
                        'l0-detect', str(package / 'scripts/l0_detector.sh')], detection_env)
    report['main_detector'] = detector
    device_match = re.search(r'^DEVICE=(.+)', detector['stdout'], re.M)
    library_match = re.search(r'^LIBRARY=(.+)', detector['stdout'], re.M)
    device = device_match.group(1) if device_match else None

    devices = {Path('/dev/l0'), Path('/dev/hisi_l0')}
    if env.get('L0_DEVICE_HOST_PATH'):
        devices.add(Path(env['L0_DEVICE_HOST_PATH']))
    devices.update(Path('/dev').glob('*l0*'))
    modules = sorted(path.name for path in Path('/sys/module').glob('*l0*'))
    report['driver_modules'] = modules
    report['sysfs_devices'] = []
    for entry in Path('/sys/class/misc').glob('*l0*'):
        item = dict(path=str(entry), real_path=str(entry.resolve()))
        try:
            item['device_number'] = (entry / 'dev').read_text().strip()
        except OSError as error:
            item['error'] = str(error)
        report['sysfs_devices'].append(item)
        devices.add(Path('/dev') / entry.name)
    report['devices'] = [file_info(path) for path in sorted(devices)]
    report['driver_parameters'] = {}
    for module in modules:
        parameters = Path('/sys/module') / module / 'parameters'
        for path in sorted(parameters.glob('*')):
            try:
                report['driver_parameters'][str(path)] = path.read_text()[:4096].strip()
            except OSError as error:
                report['driver_parameters'][str(path)] = str(error)
    report['execution_context'] = {}
    report['container_markers'] = {name: Path(name).exists() for name in ['/.dockerenv', '/run/.containerenv']}
    for name in ['/proc/self/cgroup', '/proc/1/comm']:
        try:
            report['execution_context'][name] = Path(name).read_text()[:8192].strip()
        except OSError as error:
            report['execution_context'][name] = str(error)

    candidates = []
    report['library_discovery_errors'] = {}
    def add_library(path):
        path = Path(path)
        try:
            if path.is_file():
                resolved = str(path.resolve())
                if resolved not in candidates:
                    candidates.append(resolved)
        except OSError as error:
            report['library_discovery_errors'][str(path)] = str(error)
    if env.get('L0_MEMPOOL_LIB_HOST_PATH'):
        add_library(env['L0_MEMPOOL_LIB_HOST_PATH'])
    if library_match:
        add_library(library_match.group(1))
    roots = []
    for name in [*env.get('FORL0_L0_SEARCH_ROOTS', '').split(':'), str(repo),
                 str(Path.cwd()), '/opt', '/usr/local', '/home', env.get('HOME'), '/root',
                 '/data', '/mnt', '/media', '/srv', '/workspace', '/workspaces', '/app', '/tmp', '/var/tmp',
                 '/usr/lib', '/usr/lib64', '/lib', '/lib64']:
        if name and Path(name).is_dir():
            resolved = str(Path(name).resolve())
            # Never recursively scan /, virtual filesystems or network mounts.
            if resolved not in ['/', '/proc', '/sys', '/dev'] and resolved not in roots:
                roots.append(resolved)
    report['search'] = dict(roots=roots, max_depth=8, timeout_seconds=45,
                           performed=False, scope='local filesystems; no downloads')

    probe_code = ('import ctypes,json,os,sys; '
                  'lib=ctypes.CDLL(sys.argv[1], mode=os.RTLD_NOW|os.RTLD_LOCAL); '
                  'names=["cache_tuner_init","cache_tuner_destroy","l0_mem_alloc","l0_mem_free"]; '
                  'missing=[name for name in names if not hasattr(lib,name)]; '
                  'print(json.dumps({"loaded":True,"missing_symbols":missing})); '
                  'sys.stderr.write("Missing SDK symbols: "+", ".join(missing)+"\\n" if missing else ""); '
                  'sys.exit(2 if missing else 0)')
    report['sdk_candidates'] = []
    library = None
    tested = set()
    def select_library():
        nonlocal library
        for path in candidates[:8]:
            if path in tested:
                continue
            tested.add(path)
            item = file_info(path)
            try:
                with open(path, 'rb') as stream:
                    header = stream.read(20)
                if len(header) >= 20 and header[:4] == b'\x7fELF':
                    item['elf_machine'] = struct.unpack('<H' if header[5] == 1 else '>H', header[18:20])[0]
                    expected = {'aarch64': 183, 'x86_64': 62}.get(os.uname().machine)
                    if expected and item['elf_machine'] != expected:
                        item['error'] = 'ELF architecture mismatch (expected %d)' % expected
                        report['sdk_candidates'].append(item)
                        continue
                else:
                    item['error'] = 'Not an ELF shared library'
                    report['sdk_candidates'].append(item)
                    continue
            except OSError as error:
                item['error'] = str(error)
                report['sdk_candidates'].append(item)
                continue
            probe_env = dict(env, BRISKSTATE_L0_LIBRARY_DIRS=str(Path(path).parent))
            item['load_check'] = command([str(Path(runtime) / 'python/bin/python3'), '-I',
                                          '-c', probe_code, path], probe_env)
            report['sdk_candidates'].append(item)
            if item['load_check']['exit_code'] == 0:
                library = path
                return
    select_library()
    if not library:
        if progress:
            progress('自动搜索仓库、/home及常见SDK目录，最长45秒')
        # -H handles an explicitly symlinked root; -xdev avoids crossing mounts.
        # Filesystem types known to be remote are skipped even when explicit roots.
        search = command(['find', '-H', *roots, '-xdev', '-maxdepth', '8',
                          '(', '-fstype', 'nfs', '-o', '-fstype', 'nfs4', '-o', '-fstype', 'cifs',
                          '-o', '-fstype', 'autofs', '-o', '-fstype', 'sshfs', '-o', '-fstype', 'fuse.sshfs', ')', '-prune',
                          '-o', '-type', 'f', '(', '-name', 'libl0mempool.so', '-o',
                          '-name', 'libl0mempool.so.*', ')', '-print'], env, timeout=45)
        report['search'].update(performed=True, command_result=search)
        for line in search['stdout'].splitlines():
            add_library(line)
        select_library()

    report['selected_device'] = device
    report['selected_library'] = library
    report['status'] = 'ready_for_allocation'
    issue = None
    if not device:
        existing = [item['path'] for item in report['devices'] if item.get('character_device')]
        if existing:
            issue = ('L0_DEVICE_PATH_UNSUPPORTED', '发现L0候选设备%s，但当前后端要求/dev/l0或/dev/hisi_l0。'
                     % ', '.join(existing), '开发者或驱动提供方',
                     ['将此JSON返回开发者核对设备接口；脚本不会猜测设备ABI或修改/dev。'])
        elif modules or report['sysfs_devices']:
            issue = ('L0_DEVICE_NOT_EXPOSED', '发现L0内核模块或sysfs记录，但当前运行环境看不到可用的L0设备节点。',
                     '管理员', ['检查当前是否为容器及设备是否已透传；在裸机上检查驱动和udev设备节点生成情况。'])
        else:
            issue = ('L0_DEVICE_MISSING', '在当前/dev及sysfs检查范围未发现L0设备，未发现名称包含l0的已加载模块。',
                     '管理员', ['确认运行在有L0的目标机器上，再按厂商说明检查驱动是否加载；检查范围和原始输出已保存在JSON。'])
    elif device not in ['/dev/l0', '/dev/hisi_l0'] and not any(Path(p).exists() for p in ['/dev/l0', '/dev/hisi_l0']):
        issue = ('L0_DEVICE_PATH_UNSUPPORTED', '指定设备%s存在，但当前native后端仍要求/dev/l0或/dev/hisi_l0。' % device,
                 '开发者或驱动提供方', ['返回此JSON适配设备路径；仅修改环境变量不能改变SDK/驱动接口。'])
    elif not os.access(device, os.R_OK | os.W_OK):
        issue = ('ACCESS_DENIED', '当前uid=%d无法读写L0设备%s。' % (os.getuid(), device),
                 '管理员', ['按JSON记录的设备权限和运行uid授予访问权限，再重新执行。'])
    if issue:
        report['issues'].append(dict(error_code=issue[0], cause=issue[1]))
    if not library:
        failed = report['sdk_candidates']
        if failed:
            details = next((item.get('error') or item['load_check']['stderr'].strip().splitlines()[-1]
                            for item in failed if item.get('error') or item.get('load_check', {}).get('stderr')), '')
            abi_error = any('architecture mismatch' in item.get('error', '') or
                            item.get('load_check', {}).get('exit_code') == 2 for item in failed)
            sdk_issue = ('SDK_ABI_MISMATCH' if abi_error else 'SDK_LOAD_FAILED',
                         '找到%d个SDK候选库，但架构、依赖或所需导出符号检查未通过：%s' % (len(failed), details),
                         'SDK提供方或管理员', ['检查sdk_candidates中的真实路径、ELF架构、缺失符号和动态加载原始错误。'])
        else:
            suffix = '搜索已达时间上限，并非全盘不存在。' if report['search'].get('command_result', {}).get('timed_out') else '不等于整台机器未安装SDK。'
            sdk_issue = ('L0_SDK_MISSING', '自动搜索范围内未找到可读的libl0mempool.so；' + suffix,
                         '管理员或SDK提供方', ['JSON已列出搜索目录与错误；若SDK在其他位置，用L0_MEMPOOL_LIB_HOST_PATH指定实际库文件。'])
        report['issues'].append(dict(error_code=sdk_issue[0], cause=sdk_issue[1]))
        if issue:
            issue = (*issue[:3], issue[3] + sdk_issue[3])
        else:
            issue = sdk_issue
    if issue:
        code, cause, owner, steps = issue
        if len(report['issues']) > 1:
            cause += ' 同时：' + ' '.join(item['cause'] for item in report['issues'][1:])
        report.update(status='blocked', message=cause,
                      guidance=dict(error_code=code, cause=cause, responsible_party=owner, next_steps=steps))
    report['elapsed_seconds'] = round(time.monotonic() - started, 3)
    report['discovered_library_paths'] = candidates
    return report
