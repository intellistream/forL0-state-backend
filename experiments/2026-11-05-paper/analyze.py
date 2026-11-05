"""Window-aligned CPU accounting, footprint summaries and paper figures."""
from pathlib import Path
from collections import Counter,defaultdict
import argparse,bisect,csv,datetime,gzip,json,math,re,statistics
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams.update({'font.family':'DejaVu Serif','font.size':8,'axes.labelsize':8,'axes.titlesize':10,
                     'xtick.labelsize':7,'ytick.labelsize':7,'legend.fontsize':8,'pdf.fonttype':42,'svg.fonttype':'none',
                     'axes.spines.top':False,'axes.spines.right':False})

B=Path(__file__).resolve().parent
ANALYSIS_SCHEMA=3
W=['wc','fd','sd','tm','lg','vs','lr'];M=[32,40,48,56,64]
GC=('GC Thread','G1 Main Marker','G1 Conc','G1 Refine','G1 Young RemSet')
STRICT=('org.apache.flink.runtime.state.heap.','org.apache.flink.state.forl0.','forl0::','Java_org_apache_flink_state_forl0_',
        'org.apache.flink.runtime.state.UserFacing','org.apache.flink.runtime.state.AbstractKeyedStateBackend.',
        'org.apache.flink.runtime.state.InternalKeyContext')

def dump(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def load(p):return json.loads(Path(p).read_text())
def lerp(ps,t):
    ps=sorted(ps);i=bisect.bisect_right([x[0] for x in ps],t)
    if i==0:return ps[0][1]
    if i==len(ps):return ps[-1][1]
    a,x=ps[i-1];b,y=ps[i];return x+(y-x)*(t-a)/(b-a) if b>a else y

def union(xs):
    out=[]
    for a,b in sorted(xs):
        if out and a<=out[-1][1]:out[-1][1]=max(out[-1][1],b)
        else:out.append([a,b])
    return out

def stack_counts(path):
    counts=defaultdict(Counter);leaves=Counter();unmatched=0
    for line in Path(path).read_text().splitlines():
        stack,n=line.rsplit(' ',1);n=int(n);frames=stack.split(';')
        tid=re.search(r'tid[=:](\d+)',frames[0])
        if not tid:unmatched+=n;continue
        v=counts[int(tid[1])];v['all']+=n;leaves[frames[-1]]+=n
        body=[x.replace('/','.') for x in frames[1:]]
        strict=any(any(m in f for m in STRICT) for f in body)
        if strict:v['strict_state']+=n
        if strict or any('LongStateAccess' in f for f in body):v['inclusive_state']+=n
    return counts,leaves,unmatched

def pauses(folder,role,start,end):
    intervals=[];rows=[]
    for p in folder.glob(role+'-gc-*.log*'):
        for line in p.read_text(errors='replace').splitlines():
            stamp=re.match(r'^\[([^]]+)\]',line);m=re.search(r'\[gc +\]\s+GC\((\d+)\) (Pause.*?) ([\d.]+)ms$',line)
            if not stamp or not m:continue
            try:t=datetime.datetime.fromisoformat(stamp[1]).timestamp()*1000
            except ValueError:continue
            a=max(start,t-float(m[3]));b=min(end,t)
            if b>a:intervals.append([a,b]);rows.append({'gc_id':int(m[1]),'kind':m[2],'overlap_ms':b-a,'full_pause_ms':float(m[3]),'end_ms':t})
    return {'seconds':sum(b-a for a,b in intervals)/1000,'intervals':intervals,'events':rows}

def summarize_run(folder):
    meta=load(folder/'measurement.json');start,end=meta['measurement_start_ms'],meta['measurement_end_ms']
    with gzip.open(folder/'thread-samples.jsonl.gz','rt') as f:snaps=[json.loads(l) for l in f]
    inside=[s for s in snaps if start<=s['time_ms']<end]
    if len(inside)<5:raise RuntimeError('Insufficient window samples: '+str(folder))
    roles={};allpauses=[];hz=load(B/'environment/runtime.json')['clock_ticks_per_second']
    for role in ['tm1','tm2']:
        total=(lerp([(s['time_ms'],s['roles'][role]['process']['ticks']) for s in snaps],end)-lerp([(s['time_ms'],s['roles'][role]['process']['ticks']) for s in snaps],start))/hz
        points=defaultdict(list);names=defaultdict(set)
        for s in snaps:
            for tid,v in s['roles'][role]['threads'].items():
                k=(int(tid),v['start_ticks']);points[k].append((s['time_ms'],v['ticks']));names[k].add(v['name'])
        profiled=meta['profiled'];pc,leaf,unmatched=stack_counts(folder/(role+'-cpu.collapsed')) if profiled else (defaultdict(Counter),Counter(),0)
        gc=state=strict=known_nongc=sampled=0;threads=[]
        for k,ps in points.items():
            tid,birth=k;first=min(t for t,_ in ps)
            if first>snaps[0]['time_ms']:
                previous=max((s['time_ms'] for s in snaps if s['time_ms']<first),default=snaps[0]['time_ms']);ps=ps+[(previous,0)]
            cpu=max(0,(lerp(ps,end)-lerp(ps,start))/hz);isgc=any(n.startswith(GC) for n in names[k])
            samples=pc[tid]['all'];fraction=pc[tid]['inclusive_state']/samples if samples else 0;sf=pc[tid]['strict_state']/samples if samples else 0
            if isgc:gc+=cpu
            else:
                known_nongc+=cpu
                if samples:sampled+=cpu
                state+=cpu*fraction;strict+=cpu*sf
            threads.append({'tid':tid,'start_ticks':birth,'names':sorted(names[k]),'cpu_seconds':cpu,'gc_worker':isgc,'samples':samples,'inclusive_state_fraction':fraction,'strict_state_fraction':sf})
        if gc>total+.1:raise RuntimeError('GC accounting exceeds process: '+str(folder))
        scale=min(1,max(0,total-gc)/known_nongc) if known_nongc else 1
        state*=scale;strict*=scale
        pause=pauses(folder,role,start,end);allpauses+=pause['intervals']
        if total/meta['measurement_seconds']>4.05:raise RuntimeError('CPU capacity exceeded: '+str(folder))
        roles[role]={'cpu_seconds':total,'gc_cpu_seconds':gc,'state_cpu_seconds_estimated':state if profiled else None,
                     'strict_state_cpu_seconds_estimated':strict if profiled else None,'other_cpu_seconds_estimated':total-gc-state if profiled else None,
                     'cpu_calibration_scale':scale,'sampled_nongc_cpu_coverage':sampled/(total-gc) if total>gc else 0,
                     'unmatched_stack_samples':unmatched,'stack_samples':sum(v['all'] for v in pc.values()),
                     'gc_pause':pause,'threads':threads,'top_leaf_frames':leaf.most_common(20)}
        roles[role]['gc_components']={
            'refinement':sum(t['cpu_seconds'] for t in threads if any(n.startswith('G1 Refine') for n in t['names'])),
            'parallel_workers':sum(t['cpu_seconds'] for t in threads if any(n.startswith('GC Thread') for n in t['names'])),
            'concurrent_mark':sum(t['cpu_seconds'] for t in threads if any(n.startswith(('G1 Conc','G1 Main Marker')) for n in t['names']))}
        if profiled and roles[role]['stack_samples']==0:raise RuntimeError('Empty window profile: '+str(folder))
    def describe(xs):
        if not xs:return None
        return {'mean':float(np.mean(xs)),'p50':float(np.percentile(xs,50)),'p95':float(np.percentile(xs,95)),'peak':max(xs),'minimum':min(xs),'samples':len(xs)}
    mem={}
    for field in ['VmRSS','RssAnon','RssFile','RssShmem','VmSwap']:
        mem[field]=describe([sum(s['roles'][r]['memory'].get(field,0) or 0 for r in roles) for s in inside])
    for field in ['Pss','Private_Dirty','Private_Clean','Anonymous']:
        mem[field]=describe([sum(s['roles'][r]['smaps_rollup'].get(field,0) for r in roles) for s in inside if all('smaps_rollup' in s['roles'][r] for r in roles)])
    mem['whole_capture_including_post_gc_rss_peak_bytes']=max(sum(s['roles'][r]['memory']['VmRSS'] for r in roles) for s in snaps)
    mem['initialization_to_window_end_rss_peak_bytes']=max(sum(s['roles'][r]['memory']['VmRSS'] for r in roles) for s in snaps if s['time_ms']<=end)
    baseline=load(folder/'baseline.json');mem['baseline_rss_bytes']=sum(baseline['roles'][r]['memory']['VmRSS'] for r in roles)
    mem['incremental_mean_rss_bytes']=mem['VmRSS']['mean']-mem['baseline_rss_bytes']
    if 'jm' in inside[0]['roles']:
        mem['jobmanager_rss']=describe([s['roles']['jm']['memory']['VmRSS'] for s in inside])
        mem['three_process_rss']=describe([sum(s['roles'][r]['memory']['VmRSS'] for r in ['tm1','tm2','jm']) for s in inside])
        mem['three_process_pss']=describe([sum(s['roles'][r]['smaps_rollup']['Pss'] for r in ['tm1','tm2','jm']) for s in inside if all('smaps_rollup' in s['roles'][r] for r in ['tm1','tm2','jm'])])
    retained=load(folder/'retained-after-gc.json')
    mem['post_gc_retained_rss_bytes']=sum(retained['roles'][r]['memory']['VmRSS'] for r in roles)
    mem['post_gc_retained_pss_bytes']=sum(retained['roles'][r]['smaps_rollup']['Pss'] for r in roles)
    mem['post_gc_retained_heap_used_bytes']={}
    for role in roles:
        text=(folder/(role+'-retained-heap-info.txt')).read_text()
        match=re.search(r'garbage-first heap\s+total\s+\d+[KMG],\s+used\s+(\d+)([KMG])',text)
        if match:mem['post_gc_retained_heap_used_bytes'][role]=int(match[1])*{'K':1024,'M':1024**2,'G':1024**3}[match[2]]
    jm=[]
    with gzip.open(folder/'jvm-metrics.jsonl.gz','rt') as f:metrics=[json.loads(l) for l in f]
    for i in range(0,len(metrics)-1,2):
        pair=metrics[i:i+2]
        if pair[0]['tm_id']==pair[1]['tm_id'] or abs(pair[0]['time_ms']-pair[1]['time_ms'])>2000:continue
        if not all(start<=x['time_ms']<end for x in pair):continue
        vals=[{v['id']:float(v['value']) for v in x['values'] if re.match(r'^-?\d+(\.\d+)?$',v['value'])} for x in pair]
        merged={k:sum(v.get(k,0) for v in vals) for k in set(vals[0])&set(vals[1])};jm.append(merged)
    mem['jvm_metrics']={k:describe([v[k] for v in jm if k in v]) for k in (set.intersection(*(set(x) for x in jm)) if jm else [])}
    cpu=sum(v['cpu_seconds'] for v in roles.values());gc=sum(v['gc_cpu_seconds'] for v in roles.values())
    state=sum(v['state_cpu_seconds_estimated'] for v in roles.values()) if meta['profiled'] else None
    strict=sum(v['strict_state_cpu_seconds_estimated'] for v in roles.values()) if meta['profiled'] else None
    pause_union=sum(b-a for a,b in union(allpauses))/1000
    gc_components={k:sum(r['gc_components'][k] for r in roles.values()) for k in ['refinement','parallel_workers','concurrent_mark']}
    gc_components['remaining_workers']=gc-sum(gc_components.values())
    windows=[sum(meta['per_second_sink_records'][i:i+30])/30/meta['sink_multiplier'] for i in range(0,meta['measurement_seconds'],30)]
    result={**meta,'analysis_schema':ANALYSIS_SCHEMA,'memory':mem,'cpu_seconds':cpu,'gc_cpu_seconds':gc,'state_cpu_seconds_estimated':state,'strict_state_cpu_seconds_estimated':strict,
            'other_cpu_seconds_estimated':cpu-gc-state if state is not None else None,'average_cores':cpu/meta['measurement_seconds'],
            'gc_components_cpu_seconds':gc_components,
            'gc_pause_union_seconds':pause_union,'gc_pause_union_fraction':pause_union/meta['measurement_seconds'],
            'roles_window':roles,'throughput_30s_windows':windows,
            'last_first_30s_ratio':windows[-1]/windows[0] if windows[0] else None,
            'throughput_30s_window_cv':statistics.stdev(windows)/statistics.mean(windows) if len(windows)>1 else None}
    if meta['profiled']:
        scale=1e6/meta['input_equivalent_records']
        result['cpu_seconds_per_million_inputs']={'gc':gc*scale,'state':state*scale,'other':(cpu-gc-state)*scale,'total':cpu*scale}
        result['cpu_equivalent_seconds_at_8_cores']={'gc':gc/8,'state':state/8,'other':(cpu-gc-state)/8,'unused_capacity':meta['measurement_seconds']-cpu/8}
    return result

def stats(xs):
    n=len(xs);avg=statistics.mean(xs);sd=statistics.stdev(xs) if n>1 else 0
    critical={2:12.706204736,3:4.30265273,4:3.1824463,5:2.7764451}.get(n)
    error=critical*sd/math.sqrt(n) if critical else None
    return {'n':n,'mean':avg,'sd':sd,'cv':sd/avg if avg else None,'min':min(xs),'max':max(xs),'median':statistics.median(xs),'ci95':[avg-error,avg+error] if error is not None else None,'raw':xs}

def write_csv(p,rows):
    if not rows:return
    with p.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

def aggregate(rows):
    cells=[]
    for workload in W:
        for memory in M:
            cell={'workload':workload,'memory_gib':memory};by={}
            for backend in ['heap','forl0']:
                rr=sorted([r for r in rows if r['workload']==workload and r['memory_gib']==memory and r['backend']==backend],key=lambda x:x['repeat'])
                if not rr:continue
                by[backend]=rr
                cell[backend]={'throughput':stats([r['throughput_input_records_s'] for r in rr]),
                               'rss_mean_gib':stats([r['memory']['VmRSS']['mean']/2**30 for r in rr]),
                               'rss_peak_gib':stats([r['memory']['VmRSS']['peak']/2**30 for r in rr]),
                               'pss_mean_gib':stats([r['memory']['Pss']['mean']/2**30 for r in rr]),
                               'retained_pss_gib':stats([r['memory']['post_gc_retained_pss_bytes']/2**30 for r in rr]),
                               'gc_cpu_seconds':stats([r['gc_cpu_seconds'] for r in rr]),
                               'gc_cpu_per_million':stats([r['gc_cpu_seconds']*1e6/r['input_equivalent_records'] for r in rr]),
                               'stw_union_seconds':stats([r['gc_pause_union_seconds'] for r in rr]),
                               'initialization_seconds':stats([r['coverage_seconds_including_submission'] for r in rr])}
            if all(k in by for k in ['heap','forl0']):
                h={r['repeat']:r for r in by['heap']};f={r['repeat']:r for r in by['forl0']};pairs=sorted(set(h)&set(f))
                logs=[math.log(f[k]['throughput_input_records_s']/h[k]['throughput_input_records_s']) for k in pairs]
                z=stats(logs);cell['paired_speedup']={'geomean':math.exp(z['mean']),'ci95':[math.exp(x) for x in z['ci95']] if z['ci95'] else None,
                                                  'raw':[math.exp(x) for x in logs],'n':len(logs)}
            if by:cells.append(cell)
    return cells

def savefig(fig,p):
    fig.savefig(str(p)+'.png',dpi=300,bbox_inches='tight');fig.savefig(str(p)+'.svg',bbox_inches='tight');fig.savefig(str(p)+'.pdf',bbox_inches='tight');plt.close(fig)

def figures(cells,profiles,out):
    colors={'heap':'#adc2da','forl0':'#205c85'}
    for metric,filename,ylabel in [('throughput','throughput','Input-equivalent throughput (records/s)'),('pss_mean_gib','memory-pss','Two-TM mean PSS (GiB)'),('rss_peak_gib','memory-rss','Two-TM peak RSS (GiB)'),('retained_pss_gib','memory-retained','Two-TM post-GC retained PSS (GiB)'),('gc_cpu_per_million','gc-memory','GC CPU seconds / million input-equivalent records')]:
        fig,axes=plt.subplots(2,4,figsize=(7.2,4.3));axes=axes.ravel()
        for ax,w in zip(axes,W):
            for backend in ['heap','forl0']:
                cc=[c for c in cells if c['workload']==w and backend in c];xs=[c['memory_gib'] for c in cc];ys=[c[backend][metric]['mean'] for c in cc]
                low=[max(0,c[backend][metric]['ci95'][0]) if c[backend][metric]['ci95'] else y for c,y in zip(cc,ys)]
                high=[c[backend][metric]['ci95'][1] if c[backend][metric]['ci95'] else y for c,y in zip(cc,ys)]
                ax.plot(xs,ys,'o-',label='Heap' if backend=='heap' else 'ForL0',color=colors[backend]);ax.fill_between(xs,low,high,color=colors[backend],alpha=.18)
            ax.set_title(w.upper());ax.set_xticks(M);ax.grid(axis='y',alpha=.25);ax.set_xlabel('GiB / TM')
        axes[-1].axis('off');handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='upper center',ncol=2)
        fig.supylabel(ylabel,fontsize=9);fig.suptitle('Shading: 95% t interval across independent JVM runs',y=.94,fontsize=7);fig.tight_layout(rect=(.025,0,1,.90));savefig(fig,out/filename)
    for memory in [32,64]:
        pp=[p for p in profiles if p['memory_gib']==memory]
        if not pp:continue
        denominators={w:statistics.mean(p['cpu_seconds_per_million_inputs']['total'] for p in pp if p['workload']==w and p['backend']=='heap') for w in W if any(p['workload']==w and p['backend']=='heap' for p in pp)}
        for style,stem,ylabel in [('per_million','cpu-decomposition','CPU s / million\ninput-equivalent records'),('raw','cpu-time','CPU seconds (120 s window)'),('relative','cpu-relative','CPU work / input (% of Heap)')]:
            def value(p,component):
                if style=='raw':return p[{'gc':'gc_cpu_seconds','state':'state_cpu_seconds_estimated','other':'other_cpu_seconds_estimated','total':'cpu_seconds'}[component]]
                v=p['cpu_seconds_per_million_inputs'][component]
                return v/denominators[p['workload']]*100 if style=='relative' else v
            fig,ax=plt.subplots(figsize=(7.2,2.9));x=np.arange(len(W));width=.34
            for offset,backend in [(-width/2,'heap'),(width/2,'forl0')]:
                base=np.zeros(len(W))
                for component,color in [('gc','#d9937a'),('state','#457aa5'),('other','#c8c8c8')]:
                    values=[]
                    for w in W:
                        rr=[value(p,component) for p in pp if p['workload']==w and p['backend']==backend]
                        values.append(statistics.mean(rr) if rr else np.nan)
                    ax.bar(x+offset,values,width,bottom=base,color=color,edgecolor='white',linewidth=.4,label={'gc':'GC workers','state':'State access','other':'Other'}[component] if backend=='heap' else None,hatch='//' if backend=='forl0' else '')
                    base+=np.array(values)
                low=[];high=[]
                for j,w in enumerate(W):
                    totals=[value(p,'total') for p in pp if p['workload']==w and p['backend']==backend]
                    low.append(max(0,base[j]-min(totals)) if totals else 0);high.append(max(0,max(totals)-base[j]) if totals else 0)
                ax.errorbar(x+offset,base,yerr=[low,high],fmt='none',ecolor='black',capsize=2,linewidth=.7)
            ax.set_xticks(x,[w.upper() for w in W]);ax.set_ylabel(ylabel);ax.set_title(f'{memory} GiB/TM; plain: Heap, hatched: ForL0\nn=2; caps: fork range; state CPU is estimated',fontsize=8)
            handles,labels=ax.get_legend_handles_labels()
            fig.legend(handles,labels,loc='upper center',bbox_to_anchor=(.5,1),ncol=3,frameon=False)
            ax.set_axisbelow(True);ax.grid(axis='y',alpha=.2);fig.tight_layout(rect=(0,0,1,.86));savefig(fig,out/f'{stem}-m{memory}')

def main():
    p=argparse.ArgumentParser();p.add_argument('--category',default='all');p.add_argument('--no-figures',action='store_true');a=p.parse_args()
    out=B/'analysis';out.mkdir(exist_ok=True);allrows={}
    for category in (['formal','profiles'] if a.category=='all' else [a.category]):
        result=[]
        for path in sorted((B/category).glob('*/measurement.json')):
            folder=path.parent;cached=folder/'analysis.json'
            previous=load(cached) if cached.exists() else None
            r=previous if previous and previous.get('analysis_schema')==ANALYSIS_SCHEMA else summarize_run(folder)
            if r is not previous:dump(cached,r)
            result.append(r)
        allrows[category]=result;dump(out/(category+'-runs.json'),result)
    formal=allrows.get('formal',[]);profiles=allrows.get('profiles',[]);cells=aggregate(formal)
    dump(out/'conditions.json',cells)
    flat=[]
    for r in formal+profiles:
        flat.append({k:r[k] for k in ['workload','memory_gib','backend','repeat','profiled','throughput_input_records_s','measurement_seconds','coverage_seconds_including_submission','cpu_seconds','gc_cpu_seconds','state_cpu_seconds_estimated','other_cpu_seconds_estimated','gc_pause_union_seconds','average_cores','last_first_30s_ratio','throughput_30s_window_cv']}|
                    {'rss_mean_gib':r['memory']['VmRSS']['mean']/2**30,'rss_peak_gib':r['memory']['VmRSS']['peak']/2**30,'pss_mean_gib':r['memory']['Pss']['mean']/2**30,'pss_peak_gib':r['memory']['Pss']['peak']/2**30,'directory':str(Path(r['directory']).relative_to(Path('/root/briskstate-setup/paper-experiments-20261104')))})
    write_csv(out/'runs.csv',flat)
    if not a.no_figures and formal:figures(cells,profiles,out)
    print(json.dumps({'formal':len(formal),'profiles':len(profiles),'conditions':len(cells)}),flush=True)

if __name__=='__main__':main()
