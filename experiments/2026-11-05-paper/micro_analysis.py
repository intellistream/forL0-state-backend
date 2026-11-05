from pathlib import Path
import csv,hashlib,json,shutil,statistics
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams.update({'font.family':'DejaVu Serif','font.size':8,'axes.labelsize':8,'axes.titlesize':10,
                     'xtick.labelsize':7,'ytick.labelsize':7,'legend.fontsize':8,'pdf.fonttype':42,'svg.fonttype':'none',
                     'axes.spines.top':False,'axes.spines.right':False})

B=Path(__file__).resolve().parent
old=Path('/root/briskstate-setup/micro-opt-20261104') if str(B).startswith('/root/') else B.parent/'micro-opt-20261104'
out=B/'analysis';out.mkdir(exist_ok=True)
reused=B/'micro-reused';reused.mkdir(exist_ok=True)
for name in ['analysis/results.json','analysis/state-microbenchmarks.csv','formal-r30','FINAL_SELECTION.json']:
    p=old/name;q=reused/name;q.parent.mkdir(parents=True,exist_ok=True)
    if p.is_dir():shutil.copytree(p,q,dirs_exist_ok=True)
    else:shutil.copy2(p,q)
baseline=json.loads((reused/'analysis/results.json').read_text());rows=[];raw=[]
for entry in baseline:
    op=entry['op'];row={'operation':op}
    for backend in ['heap','forl0']:
        z=entry[backend];row[backend]={'score':z['score'],'fork_scores':z['fork_scores'],'range':[min(z['fork_scores']),max(z['fork_scores'])],'alloc_B_op':z['alloc'],'gc_ms_per_fork':z['gc_ms'],'rss_peak_gib':z['rss_gib'],'files':['micro-reused/'+p for p in z['files']]}
        for score,p in zip(z['fork_scores'],z['files']):raw.append({'operation':op,'backend':backend,'fork_score_ops_ms':score,'path':'micro-reused/'+p,'source':'reused-r30'})
    found=[]
    source_dir=B/('micro-rocksdb-long-map-read' if op=='mapGet' else 'micro-rocksdb' if op in ['valueGet','listGet','listIterate'] else 'micro-rocksdb-long-writes')
    for p in sorted(source_dir.glob(op+'-*-ROCKSDB.json')):
        try:z=json.loads(p.read_text())[0]
        except (ValueError,IndexError):continue
        proof=json.loads(p.with_name(p.stem+'-processes.summary.json').read_text())
        assert proof['java_masks_bound'] and proof['observed_jmh_forks']==1
        assert any('librocksdbjni' in path for path in proof['native_mappings']),proof
        process=json.loads(p.with_name(p.stem+'-processes.json').read_text())
        found.append((p,z,process));raw.append({'operation':op,'backend':'rocksdb','fork_score_ops_ms':z['primaryMetric']['score'],'path':str(p.relative_to(B)),'source':'new-rocksdb'})
    if found:
        scores=[z['primaryMetric']['score'] for _,z,_ in found]
        row['rocksdb']={'score':statistics.mean(scores),'fork_scores':scores,'range':[min(scores),max(scores)],
                        'alloc_B_op':statistics.mean(z['secondaryMetrics']['gc.alloc.rate.norm']['score'] for _,z,_ in found),
                        'gc_ms_per_fork':statistics.mean(z['secondaryMetrics'].get('gc.time',{}).get('score',0) for _,z,_ in found),
                        'rss_peak_gib':statistics.mean(max(v['rss_peak_bytes'] for v in process.values())/2**30 for _,_,process in found),
                        'files':[str(p.relative_to(B)) for p,_,_ in found]}
    rows.append(row)
(out/'micro-results.json').write_text(json.dumps(rows,indent=2)+'\n')
with (out/'micro-forks.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(raw[0]));w.writeheader();w.writerows(raw)
with (out/'micro-summary.csv').open('w',newline='') as f:
    w=csv.writer(f);w.writerow(['operation','heap_ops_ms','forl0_ops_ms','rocksdb_ops_ms','forl0_over_heap','forl0_over_rocksdb'])
    for r in rows:w.writerow([r['operation'],r['heap']['score'],r['forl0']['score'],r.get('rocksdb',{}).get('score'),r['forl0']['score']/r['heap']['score'],r['forl0']['score']/r['rocksdb']['score'] if 'rocksdb' in r else None])
colors={'heap':'#adc2da','forl0':'#205c85','rocksdb':'#c1814b'}
for logarithmic in [False,True]:
    fig,axes=plt.subplots(1,3,figsize=(7.2,3.05));width=.24
    for ax,prefix,title in zip(axes,['value','list','map'],['ValueState','ListState','MapState']):
        rr=[r for r in rows if r['operation'].startswith(prefix)];x=np.arange(len(rr))
        for i,backend in enumerate(['heap','forl0','rocksdb']):
            ys=[r[backend]['score'] if backend in r else np.nan for r in rr]
            lo=[y-r[backend]['range'][0] if backend in r else 0 for y,r in zip(ys,rr)]
            hi=[r[backend]['range'][1]-y if backend in r else 0 for y,r in zip(ys,rr)]
            ax.bar(x+(i-1)*width,ys,width,color=colors[backend],edgecolor='black',linewidth=.5,label={'heap':'Heap','forl0':'ForL0','rocksdb':'RocksDB'}[backend],yerr=np.array([lo,hi]),capsize=2)
        ax.set_xticks(x,[r['operation'][len(prefix):] for r in rr],rotation=30,ha='right')
        ax.set_title(title);ax.set_axisbelow(True);ax.grid(axis='y',alpha=.25)
        ax.set_ylabel('Throughput (ops/ms)'+(' — log scale' if logarithmic else ''))
        if logarithmic:ax.set_yscale('log')
        else:ax.set_ylim(bottom=0)
    handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='upper center',ncol=3)
    fig.suptitle('Error bars: range of two fork means; Heap/ForL0 reused from R30',y=.91,fontsize=7)
    fig.tight_layout(rect=(0,0,1,.87));name='micro-three-backends'+('-log' if logarithmic else '')
    for ext in ['png','svg','pdf']:fig.savefig(out/(name+'.'+ext),dpi=300,bbox_inches='tight')
    plt.close(fig)
print('MICRO_ANALYSIS',len(raw),'forks',sum('rocksdb' in r and len(r['rocksdb']['fork_scores'])==2 for r in rows),'complete operations',flush=True)
