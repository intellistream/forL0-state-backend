#!/usr/bin/env python3
"""Read-only perf_event_open capability probe; does not change host settings."""
import ctypes,json,os,platform,shutil
from pathlib import Path

def probe():
    arch=platform.machine(); syscall={'aarch64':241,'x86_64':298}.get(arch)
    out={'architecture':arch,'kernel':platform.release(),'tools':{x:shutil.which(x) for x in ['perf','kperf','sysper']}}
    for name in ['perf_event_paranoid','kptr_restrict','perf_user_access']:
        p=Path('/proc/sys/kernel')/name
        out[name]=p.read_text().strip() if p.exists() else None
    out['capabilities']=[s for s in Path('/proc/self/status').read_text().splitlines() if s.startswith(('CapEff:','Seccomp:'))]
    out['event_sources']=sorted(p.name for p in Path('/sys/bus/event_source/devices').glob('*'))
    if syscall:
        # perf_event_attr v0: type=HARDWARE, config=CPU_CYCLES,
        # disabled=1, exclude_kernel=1, exclude_hv=1, pid=self/cpu=any.
        attr=(ctypes.c_ubyte*64)()
        ctypes.c_uint32.from_buffer(attr,0).value=0
        ctypes.c_uint32.from_buffer(attr,4).value=64
        ctypes.c_uint64.from_buffer(attr,8).value=0
        ctypes.c_uint64.from_buffer(attr,40).value=1|(1<<5)|(1<<6)
        libc=ctypes.CDLL(None,use_errno=True)
        fd=libc.syscall(syscall,ctypes.byref(attr),0,-1,-1,0)
        errno=ctypes.get_errno()
        out['user_cycles_open']={'success':fd>=0,'errno':errno if fd<0 else 0,'message':os.strerror(errno) if fd<0 else 'opened'}
        if fd>=0:os.close(fd)
    return out
if __name__=='__main__':print(json.dumps(probe(),indent=2))
