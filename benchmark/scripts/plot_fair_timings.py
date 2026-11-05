#!/usr/bin/env python3
import json,statistics,sys
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
base=Path(sys.argv[1]);rows=json.loads((base/'summary.json').read_text());names=['wc','fd','sd','tm','lg','vs','lr']
fig,axes=plt.subplots(1,2,figsize=(12,4.2),layout='constrained');med={}
for i,backend in enumerate(['heap','forl0']):
 samples=[[r['seconds'] for r in rows if r['workload']==w and r['backend']==backend and not r['profiled']] for w in names]
 med[backend]=[statistics.median(v) for v in samples];x=np.arange(7)+(i-.5)*.36
 axes[0].bar(x,med[backend],.34,label=backend,color=['#ed9d3b','#2670ac'][i])
 for xx,v in zip(x,samples):axes[0].scatter([xx]*len(v),v,c='black',s=13,zorder=3,marker='_')
 axes[0].legend()
speed=np.array(med['heap'])/np.array(med['forl0']);bars=axes[1].bar(names,speed,color='#2670ac')
axes[1].bar_label(bars,labels=[f'{v:.2f}x' for v in speed],padding=3)
for ax in axes:ax.spines[['top','right']].set_visible(False);ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True);ax.set_xticks(range(7),[w.upper() for w in names])
axes[0].set_ylabel('Unprofiled Flink job duration (seconds)');axes[1].set_ylabel('Heap / ForL0 median duration');axes[1].set_ylim(0,max(speed)*1.2)
fig.suptitle('Identical standard-API job, same workload per backend\nBars: medians; black marks: all unprofiled samples',fontsize=12)
out=base/'analysis';out.mkdir(exist_ok=True);fig.savefig(out/'unprofiled-timings.png',dpi=180);fig.savefig(out/'unprofiled-timings.svg')
