"""Plot stage-separated Secondary fault statistics; missing denominators stay N/A."""
import argparse,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('statistics',type=Path)
    parser.add_argument('--stage',choices=['pilot','formal'],required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    rows=[r for r in json.loads(args.statistics.read_text()) if r['stage']==args.stage]
    if not rows:raise SystemExit('No completed runs for requested stage')
    lookup={}
    for row in rows:
        key=(row['workload'],row['scenario'],row['config'])
        if key in lookup:raise SystemExit('Multiple runs per cell; select one protocol batch explicitly')
        lookup[key]=row
    args.output.mkdir(parents=True,exist_ok=True)
    scenarios=[('secondary-stop','S2: one Secondary stopped'),('two-secondary-stop','S3: immediate'),
               ('two-secondary-stop-settled','S3: after Primary stepdown')]
    for metric,title in [('w1_availability','Acknowledged first writes / attempted writes'),
                         ('violation_rate','Confirmed violations / evaluable trials')]:
        fig,axes=plt.subplots(2,3,figsize=(11,6),sharey=True,layout='constrained')
        for i,model in enumerate(['mw','ryw']):
            for j,(scenario,label) in enumerate(scenarios):
                ax=axes[i,j]
                for k,(cid,color) in enumerate([('C1','#D97706'),('C4','#2563EB')]):
                    row=lookup.get((model,scenario,cid));value=row.get(metric) if row else None
                    if value is None:
                        ax.text(k,5,'N/A' if row else 'Pending',ha='center',color='#555555')
                    else:
                        ax.bar(k,100*value,color=color,width=.55)
                        numerator=row['w1_success'] if metric=='w1_availability' else row['confirmed_violations']
                        denominator=row['w1_attempted'] if metric=='w1_availability' else row['evaluable']
                        ax.text(k,min(100*value+3,106),f'{numerator}/{denominator}',ha='center',fontsize=10)
                ax.set_xticks([0,1],['C1','C4']);ax.set_xlim(-.65,1.65);ax.set_ylim(0,115)
                ax.set_yticks([0,25,50,75,100]);ax.grid(axis='y',alpha=.18);ax.set_axisbelow(True)
                ax.spines[['top','right']].set_visible(False)
                if i==0:ax.set_title(label,fontsize=10)
                if j==0:ax.set_ylabel(model.upper()+' (%)')
        fig.suptitle(args.stage.upper()+': '+title+'\nN/A = zero denominator; Pending = unfinished cell',fontsize=13)
        for extension in ['png','svg']:
            fig.savefig(args.output/f'{args.stage}-{metric}.{extension}',dpi=180)
        plt.close(fig)
    print(json.dumps({'stage':args.stage,'completed_cells':len(rows),'matplotlib':matplotlib.__version__}))

if __name__=='__main__':main()
