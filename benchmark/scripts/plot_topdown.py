#!/usr/bin/env python3
"""Two publication-ready stacked charts from real Kunpeng DevKit ALL reports.

Input manifest: {"runs":[{"workload":"wc", "backend":"heap", "report":"raw.txt"}, ...]}.
Each raw report must aggregate both TaskManagers, use the same capture window,
and retain hardware counts. No synthetic substitution or missing-value zero fill.
"""
import argparse,hashlib,json,math,re
from pathlib import Path
WORKLOADS=['wc','fd','sd','tm','lg','vs','lr']
FIELDS=['Retiring','Bad Speculation','Frontend Bound','Backend Bound','Core Bound','Memory Bound','L1 Bound','L2 Bound','L3 or DRAM Bound','Store Bound']

def parse_report(path):
    text=path.read_text(errors='replace')
    # Ignore per-interval/per-thread tables. Require a unique vendor aggregate.
    marker='TOP-DOWN Summary Report-ALL'
    if text.count(marker)!=1:raise ValueError(f'{path}: expected one aggregate ALL report')
    section=text.split(marker,1)[1].split('Optimization Suggestions',1)[0]
    values={}
    for field in FIELDS:
        hits=re.findall(r'^\s*[|+`\- ]*'+re.escape(field)+r'\s+(\d+(?:\.\d+)?)\s+',section,re.M)
        if len(hits)!=1:raise ValueError(f'{path}: missing/ambiguous {field}')
        values[field]=float(hits[0])
    for name in ['Cycles','Instructions']:
        m=re.search(r'^\s*'+name+r'\s+([\d,]+)\s*$',section,re.M)
        if not m or int(m[1].replace(',',''))<=0:raise ValueError(f'{path}: invalid {name}')
        values[name]=int(m[1].replace(',',''))
    pmu=dict(re.findall(r'^\s*(r[0-9a-fA-F]+)\s+([\d,]+)\s*$',section,re.M))
    if len(pmu)<4:raise ValueError(f'{path}: hardware raw-event counts not retained')
    l1=sum(values[k] for k in FIELDS[:4]); mem=sum(values[k] for k in FIELDS[6:]); be=values['Core Bound']+values['Memory Bound']
    if abs(l1-100)>.6 or abs(be-values['Backend Bound'])>.6 or abs(mem-values['Memory Bound'])>.6:
        raise ValueError(f'{path}: hierarchy does not close: total={l1}, backend={be}, memory={mem}')
    if not all(math.isfinite(v) and v>=0 for v in values.values()):raise ValueError(f'{path}: invalid metric')
    values['pmu_raw_counts']={k:int(v.replace(',','')) for k,v in pmu.items()}
    values['report_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    return values

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('manifest',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    manifest=json.loads(a.manifest.read_text());rows={}
    for row in manifest['runs']:
        key=(row['workload'],row['backend'])
        if key in rows:raise ValueError('Duplicate workload/backend; aggregate raw counters before plotting')
        rows[key]={**row,'metrics':parse_report(a.manifest.parent/row['report'])}
    expected={(w,b) for w in WORKLOADS for b in ['heap','forl0']}
    if set(rows)!=expected:raise ValueError(f'Need all 14 hardware reports; missing {expected-set(rows)}')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'pdf.fonttype':42,'ps.fonttype':42,'axes.spines.top':False,'axes.spines.right':False})
    a.output.mkdir(parents=True,exist_ok=True)
    palette=['#4c78a8','#f2cf5b','#f58518','#54a24b','#b279a2']
    for mode in ['topdown','backend']:
        fig,ax=plt.subplots(figsize=(12.0,4.8),layout='constrained')
        categories=FIELDS[:4] if mode=='topdown' else ['Core Bound','L1 Bound','L2 Bound','L3 or DRAM Bound','Store Bound']
        for bi,backend in enumerate(['heap','forl0']):
            x=np.arange(7)+(bi-.5)*.34;bottom=np.zeros(7)
            for ci,category in enumerate(categories):
                heights=np.array([rows[(w,backend)]['metrics'][category] for w in WORKLOADS])
                if mode=='backend':
                    denominator=np.array([rows[(w,backend)]['metrics']['Backend Bound'] for w in WORKLOADS])
                    if np.any(denominator<=0):raise ValueError('Backend share cannot normalize zero denominator')
                    heights=heights/denominator*100
                ax.bar(x,heights,.31,bottom=bottom,color=palette[ci],edgecolor='white',linewidth=.4,label=category if bi==0 else None)
                bottom+=heights
            for xi in x:ax.text(xi,-3,'H' if bi==0 else 'F',ha='center',va='top',fontsize=9)
            if mode=='backend':
                for wi,w in enumerate(WORKLOADS):
                    m=rows[(w,backend)]['metrics'];ax.text(x[wi],101,f"{100*m['Memory Bound']/m['Backend Bound']:.0f}%",ha='center',va='bottom',fontsize=8,color='#444444')
        ax.set_xticks(range(7),[w.upper() for w in WORKLOADS]);ax.tick_params(axis='x',pad=17)
        ax.set_ylim(0,109 if mode=='backend' else 101);ax.set_yticks([0,20,40,60,80,100])
        ax.set_ylabel('Pipeline slots (%)' if mode=='topdown' else 'Share of Backend Bound (%)')
        ax.set_axisbelow(True);ax.grid(axis='y',color='#dddddd',linewidth=.6)
        ax.legend(loc='upper center',bbox_to_anchor=(.5,1.2),ncol=len(categories),frameon=False,fontsize=9)
        note='H: Heap   F: ForL0   |   Both TaskManagers; PMU aggregate'
        if mode=='backend':note+='   |   Labels above bars: total Memory Bound share'
        fig.supxlabel(note,fontsize=9)
        for suffix in ['png','pdf','svg']:fig.savefig(a.output/(mode+'.'+suffix),dpi=220)
        plt.close(fig)
    (a.output/'plotted-data.json').write_text(json.dumps(list(rows.values()),indent=2)+'\n')
if __name__=='__main__':main()
