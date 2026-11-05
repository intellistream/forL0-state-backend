"""Offline Eden sizing and bounded Java-heap residency from saved diagnostic maps."""
from pathlib import Path
import csv,datetime,gzip,json,re,statistics

B=Path(__file__).resolve().parent
W=['wc','fd','sd','tm','lg','vs','lr']
def load(p):return json.loads(Path(p).read_text())
def dump(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def mean(xs):return statistics.mean(xs) if xs else None

def residency_bounds(folder,role):
    text=(folder/(role+'-retained-heap-info.txt')).read_text()
    match=re.search(r'\[(0x[0-9a-f]+),\s*(0x[0-9a-f]+)\)',text)
    if not match:return None
    lo,hi=[int(v,16) for v in match.groups()]
    with gzip.open(folder/(role+'-retained-numa-maps.txt.gz'),'rt') as f:lines=f.read().splitlines()
    maps=[]
    for line in lines:
        cols=line.split();addr=int(cols[0],16)
        fields=dict(x.split('=',1) for x in cols[1:] if '=' in x)
        pages=sum(int(v) for k,v in fields.items() if re.fullmatch(r'N\d+',k))
        maps.append((addr,pages*int(fields.get('kernelpagesize_kB','4'))*1024))
    maps.sort();lower=upper=0
    for (start,rss),(following,_) in zip(maps,maps[1:]):
        overlap=max(0,min(following,hi)-max(start,lo))
        if not overlap:continue
        # numa_maps gives VMA starts, not ends. following is a conservative
        # upper bound on the VMA end, so merged boundary mappings yield bounds.
        outside=max(0,lo-start)+max(0,following-hi)
        lower+=max(0,rss-outside);upper+=min(rss,overlap)
    total=sum(x[1] for x in maps)
    return {'java_heap_address_start':hex(lo),'java_heap_address_end':hex(hi),
            'numa_total_resident_gib':total/2**30,'java_heap_resident_lower_gib':lower/2**30,
            'java_heap_resident_upper_gib':upper/2**30,
            'outside_java_heap_lower_gib':(total-upper)/2**30,
            'outside_java_heap_upper_gib':(total-lower)/2**30,
            'scope':'post-window, paused/drained, after diagnostic Full GC and class histogram; RSS bounds, not PSS or live allocations'}

def summarize(folder):
    meta=load(folder/'measurement.json');a,z=meta['measurement_start_ms'],meta['measurement_end_ms']
    cached=folder/'analysis.json'
    if cached.exists():analysis=load(cached)
    else:
        from analyze import summarize_run
        analysis=summarize_run(folder);dump(cached,analysis)
    roles={}
    for role in ['tm1','tm2']:
        region=int(meta['jvm_flags'][role]['G1HeapRegionSize']);eden=[];before=[]
        for p in folder.glob(role+'-gc-*.log*'):
            for line in p.read_text(errors='replace').splitlines():
                stamp=re.match(r'^\[([^]]+)\]',line)
                if not stamp:continue
                try:t=datetime.datetime.fromisoformat(stamp[1]).timestamp()*1000
                except ValueError:continue
                if not a<=t<z:continue
                m=re.search(r'Eden regions: (\d+)->(\d+)\((\d+)\)',line)
                if m:before.append(int(m[1])*region);eden.append(int(m[3])*region)
        flags={}
        for line in (folder/(role+'-flags.txt')).read_text().splitlines():
            fields=line.split()
            if len(fields)>3 and fields[1] in ['NewSize','MaxNewSize','G1MaxNewSizePercent','G1NewSizePercent','MaxGCPauseMillis','AlwaysPreTouch']:flags[fields[1]]=fields[3]
        roles[role]={'xmx_gib':int(meta['jvm_flags'][role]['MaxHeapSize'])/2**30,
                     'median_eden_target_gib':statistics.median(eden)/2**30 if eden else None,
                     'median_eden_before_gc_gib':statistics.median(before)/2**30 if before else None,
                     'eden_gc_events':len(eden),'jvm_flags':flags,'residency':residency_bounds(folder,role)}
    memory=analysis['memory'];bounds=[r['residency'] for r in roles.values()]
    return {'workload':meta['workload'],'memory_gib':meta['memory_gib'],'backend':meta['backend'],'repeat':meta['repeat'],
            'directory':str(folder.relative_to(B)),'measurement_seconds':meta['measurement_seconds'],
            'warmup_seconds':meta['warmup_seconds_minimum'],'throughput':meta['throughput_input_records_s'],
            'last_first_30s_ratio':analysis['last_first_30s_ratio'],'window_cv':analysis['throughput_30s_window_cv'],
            'xmx_per_tm_gib':roles['tm1']['xmx_gib'],
            'two_tm_eden_target_gib':sum(r['median_eden_target_gib'] for r in roles.values()) if all(r['median_eden_target_gib'] is not None for r in roles.values()) else None,
            'two_tm_mean_pss_gib':memory['Pss']['mean']/2**30,
            'two_tm_mean_uss_gib':(memory['Private_Clean']['mean']+memory['Private_Dirty']['mean'])/2**30,
            'two_tm_post_gc_heap_used_gib':sum(memory['post_gc_retained_heap_used_bytes'].values())/2**30,
            'two_tm_diagnostic_resident_gib':sum(r['numa_total_resident_gib'] for r in bounds) if all(bounds) else None,
            'two_tm_java_heap_resident_lower_gib':sum(r['java_heap_resident_lower_gib'] for r in bounds) if all(bounds) else None,
            'two_tm_java_heap_resident_upper_gib':sum(r['java_heap_resident_upper_gib'] for r in bounds) if all(bounds) else None,
            'roles':roles}

def main():
    out=B/'analysis';out.mkdir(exist_ok=True);groups={}
    for category in ['formal','younggen-control','stability-validation']:
        rows=[summarize(p.parent) for p in sorted((B/category).glob('*/measurement.json'))]
        groups[category]=rows
    summary=[]
    for w in W:
        for m in [32,40,48,56,64]:
            for be in ['heap','forl0']:
                rr=[r for r in groups['formal'] if (r['workload'],r['memory_gib'],r['backend'])==(w,m,be)]
                if not rr:continue
                keys=['xmx_per_tm_gib','two_tm_eden_target_gib','two_tm_mean_pss_gib','two_tm_post_gc_heap_used_gib',
                      'two_tm_diagnostic_resident_gib','two_tm_java_heap_resident_lower_gib','two_tm_java_heap_resident_upper_gib']
                summary.append({'workload':w,'memory_gib':m,'backend':be,'n':len(rr)}|{k:mean([r[k] for r in rr if r[k] is not None]) for k in keys})
    result={'formal_conditions':summary,**groups,
            'bounds_method':'Count per-VMA resident NUMA pages. For a VMA touching a Java-heap boundary, allow all address space outside heap up to the next VMA start to hold resident pages; derive conservative lower/upper bounds. Does not equate outside-heap RSS with ForL0 state.',
            'time_scope':'Eden from formal timed window; heap residency from separate paused post-GC/post-histogram diagnostic. Not an exact same-instant subtraction from runtime PSS.'}
    dump(out/'memory-mechanism.json',result)
    with (out/'memory-mechanism.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(summary[0]));writer.writeheader();writer.writerows(summary)
    print(json.dumps({k:len(v) for k,v in groups.items()}),flush=True)

if __name__=='__main__':main()
