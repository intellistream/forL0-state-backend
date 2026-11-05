#!/usr/bin/env python3
"""Plot measured GC CPU and sample-estimated state CPU; not PMU top-down."""
import argparse,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
p=argparse.ArgumentParser();p.add_argument('input',type=Path);p.add_argument('out',type=Path);a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
rows=json.loads(a.input.read_text())['runs'];names=['wc','fd','sd','tm','lg','vs','lr']
fig,axes=plt.subplots(1,2,figsize=(14,5),layout='constrained')
colors=['#2670ac','#ed9d3b','#87939d'];labels=['State path (sample estimate)','GC workers (thread CPU)','Other (remainder)']
for panel,ax in enumerate(axes):
 for bi,backend in enumerate(['heap','forl0']):
  x=np.arange(7)+(bi-.5)*.36;bottom=np.zeros(7)
  selected=[next(r for r in rows if r['workload']==w and r['backend']==backend) for w in names]
  for k,color,label in zip(['state_estimated','gc_workers_measured','other_estimated'],colors,labels):
   value=np.array([r['cpu'][k]/(r['cpu']['total'] if panel else 1) for r in selected])*(100 if panel else 1)
   ax.bar(x,value,.34,bottom=bottom,color=color,label=label if bi==0 else None,edgecolor='white',linewidth=.25);bottom+=value
  for xx,yy in zip(x,bottom):ax.annotate('H' if bi==0 else 'F',(xx,yy),ha='center',va='bottom',fontsize=8)
 ax.set_xticks(range(7),[w.upper() for w in names]);ax.spines[['top','right']].set_visible(False)
 ax.set_ylabel('TM CPU work (core seconds)' if panel==0 else 'Share of TM CPU work (%)');ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True)
axes[1].set_ylim(0,109);axes[0].legend(loc='upper left',fontsize=8)
fig.suptitle('Same standard Flink API job: Heap (H) and ForL0 (F)\nCPU accounting, not hardware top-down or a serial critical-path decomposition',fontsize=12)
fig.savefig(a.out/'cpu-work.png',dpi=180);fig.savefig(a.out/'cpu-work.svg');plt.close(fig)
