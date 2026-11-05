"""Build a common measurement harness; keep the seven task graphs and state algorithms."""
from pathlib import Path
import hashlib,json,shutil,subprocess,zipfile

B=Path('/root/briskstate-setup/paper-experiments-20261104')
OLD=Path('/root/briskstate-setup/deep-opt-20261103/reproduction/benchset-source')
out=B/'steady-job';src=out/'src/main/java/org/apache/flink/benchmark/benchset'
src.mkdir(parents=True,exist_ok=True)
for p in (OLD/'src/main/java/org/apache/flink/benchmark/benchset').glob('*.java'):shutil.copy2(p,src/p.name)
main=(src/'BenchsetMain.java').read_text()
main=main.replace('private final String output;', 'private final String output;\n    private final String control;')
main=main.replace('output=args.getRequired("outputDirectory");','output=args.getRequired("outputDirectory");\n        control=args.get("controlDirectory","");')
main=main.replace('private final String control;', 'private final String control;\n    private final boolean materializeHistory;')
main=main.replace('control=args.get("controlDirectory","");', 'control=args.get("controlDirectory","");\n        materializeHistory=!control.isEmpty() || args.getBoolean("materializeHistory",false);')
main=main.replace('new BenchsetSink(output,name)', 'new BenchsetSink(output,name,control)')
main=main.replace('new BenchsetSource(keys,records)', 'new BenchsetSource(keys,records,control)')
main=main.replace('new BenchsetStage(name,width,touches,window)', 'new BenchsetStage(name,width,touches,window,materializeHistory)')
main=main.replace('new BenchsetStage("Global ACD",4,1,window)', 'new BenchsetStage("Global ACD",4,1,window,materializeHistory)')
(src/'BenchsetMain.java').write_text(main)
stage=(src/'BenchsetStage.java').read_text()
stage=stage.replace('private final int width,touches,window;', 'private final int width,touches,window;\n    private final boolean materializeHistory;')
stage=stage.replace('this.role=role;this.width=width;this.touches=touches;this.window=window;', 'this(role,width,touches,window,false);\n    }\n    public BenchsetStage(String role,int width,int touches,int window,boolean materializeHistory) {\n        this.role=role;this.width=width;this.touches=touches;this.window=window;this.materializeHistory=materializeHistory;')
stage=stage.replace('long n=count.add(1);', '''long n=count.add(1);
        // Equivalent zero values materialize bounded role dictionaries in the
        // common key-coverage phase. Missing history was read as zero before.
        if(materializeHistory && n==1) {
            int slots=role.equals("Predict")?272:role.equals("Moving Average")?window:0;
            for(long k=0;k<slots;k++)history.put(k,0L);
        }''')
(src/'BenchsetStage.java').write_text(stage)
source='''package org.apache.flink.benchmark.benchset;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** Shared key-coverage barrier, then the original seeded uniform access stream. */
public class BenchsetSource extends RichParallelSourceFunction<BenchsetEvent> {
    private final int keys; private final long records; private final String control;
    private volatile boolean running=true;
    public BenchsetSource(int keys,long records){this(keys,records,"");}
    public BenchsetSource(int keys,long records,String control){this.keys=keys;this.records=records;this.control=control;}
    @Override public void run(SourceContext<BenchsetEvent> ctx)throws Exception {
        int index=getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int parallelism=getRuntimeContext().getTaskInfo().getNumberOfSubtasks();
        SplittableRandom random=new SplittableRandom(20261102L+index);
        Path base=control.isEmpty()?null:Paths.get(control);
        long produced=0,signature=0,sequence=index;
        try {
            for(;running && (base!=null || sequence<records);sequence+=parallelism) {
                if(base!=null && sequence>=keys && produced==keys/parallelism+(index<keys%parallelism?1:0)) {
                    Files.write(base.resolve("ready-"+index),Long.toString(produced).getBytes(StandardCharsets.UTF_8));
                    while(running && !Files.exists(base.resolve("release")))Thread.sleep(10);
                }
                if(base!=null && (produced&1023)==0) {
                    if(Files.exists(base.resolve("stop")))break;
                    if(Files.exists(base.resolve("pause"))) {
                        Map<String,Object> paused=new LinkedHashMap<>();paused.put("records",produced);paused.put("signature",signature);
                        Path temp=base.resolve("paused-"+index+".json.tmp");
                        Files.write(temp,new Gson().toJson(paused).getBytes(StandardCharsets.UTF_8));
                        Files.move(temp,base.resolve("paused-"+index+".json"));
                        while(running && Files.exists(base.resolve("pause")) && !Files.exists(base.resolve("stop")))Thread.sleep(10);
                        if(Files.exists(base.resolve("stop")))break;
                    }
                }
                BenchsetEvent e=new BenchsetEvent();
                e.key=sequence<keys?sequence:random.nextInt(keys);e.peer=(e.key+1)%keys;
                e.sequence=sequence;e.sample=1+(BenchsetEvent.mix(sequence)&1023);e.metric=e.sample;e.kind=(int)(sequence&15);
                synchronized(ctx.getCheckpointLock()){ctx.collect(e);}
                produced++;signature+=BenchsetEvent.mix(sequence);
            }
        }finally {
            if(base!=null){
                Map<String,Object> row=new LinkedHashMap<>();row.put("subtask",index);row.put("records",produced);row.put("signature",signature);row.put("next_sequence",sequence);
                Files.write(base.resolve("source-"+index+".json"),new Gson().toJson(row).getBytes(StandardCharsets.UTF_8));
            }
        }
    }
    @Override public void cancel(){running=false;}
}
'''
source=source.replace('getNumberOfSubtasks()', 'getNumberOfParallelSubtasks()')
(src/'BenchsetSource.java').write_text(source)
sink=(src/'BenchsetSink.java').read_text()
sink=sink.replace('private final String directory,name;', 'private final String directory,name,control;\n    private transient long measuredRecords,measuredProjection,measuredSignature,startMs,endMs,nextCheck;\n    private transient long[] secondCounts;')
sink=sink.replace('public BenchsetSink(String directory,String name) {this.directory=directory;this.name=name;}', '''public BenchsetSink(String directory,String name) {this(directory,name,"");}
    public BenchsetSink(String directory,String name,String control) {this.directory=directory;this.name=name;this.control=control;}''')
sink=sink.replace('public void invoke(BenchsetEvent e,Context c) {', 'public void invoke(BenchsetEvent e,Context c) throws Exception {')
sink=sink.replace('records++;projection+=e.features;signature+=BenchsetEvent.mix(e.sequence);', '''records++;projection+=e.features;signature+=BenchsetEvent.mix(e.sequence);
        if(!control.isEmpty()) {
            long now=System.currentTimeMillis();
            if(endMs==0 && now>=nextCheck) {
                nextCheck=now+250;
                Path window=Paths.get(control,"window.json");
                if(Files.exists(window)) {
                    long[] times=new Gson().fromJson(new String(Files.readAllBytes(window),StandardCharsets.UTF_8),long[].class);
                    startMs=times[0];endMs=times[1];secondCounts=new long[(int)((endMs-startMs+999)/1000)];
                }
            }
            if(now>=startMs && now<endMs) {
                measuredRecords++;measuredProjection+=e.features;measuredSignature+=BenchsetEvent.mix(e.sequence);
                secondCounts[(int)((now-startMs)/1000)]++;
            }
        }''')
sink=sink.replace('result.put("records",records);', '''result.put("records",records);
        result.put("measured_records",measuredRecords);result.put("measured_projection",measuredProjection);
        result.put("measured_signature",measuredSignature);result.put("measurement_start_ms",startMs);result.put("measurement_end_ms",endMs);result.put("second_counts",secondCounts);''')
(src/'BenchsetSink.java').write_text(sink)
classes=out/'classes';classes.mkdir(exist_ok=True)
cp=':'.join(map(str,Path('/root/opt/flink-1.20.3/lib').glob('*.jar')))+':'+str(B/'reproduction/benchset-1.0-SNAPSHOT.jar')
subprocess.run(['/usr/lib/jvm/java-11-openjdk/bin/javac','--release','8','-cp',cp,'-d',str(classes),*[str(p) for p in src.glob('*.java')]],check=True)
frozen=B/'reproduction/benchset-1.0-SNAPSHOT.jar';jar=out/'paper-steady-benchset.jar'
with zipfile.ZipFile(frozen) as original,zipfile.ZipFile(jar,'w',zipfile.ZIP_DEFLATED) as target:
    generated={str(p.relative_to(classes)):p for p in classes.rglob('*.class')}
    # Preserve the unchanged state API loop and event representation byte-for-byte.
    for info in original.infolist():
        if info.filename in generated:
            stem=Path(info.filename).name
            if stem.startswith(('LongStateAccess','BenchsetEvent')):
                target.writestr(info,original.read(info.filename));generated.pop(info.filename)
                continue
            target.writestr(info,generated.pop(info.filename).read_bytes())
        else:target.writestr(info,original.read(info.filename))
    for name,p in generated.items():target.write(p,name)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
proof={'frozen_job_sha256':sha(frozen),'steady_job_sha256':sha(jar),'standard_state_api_loop_unchanged':True,
       'changes':['source coverage/release/stop barriers','sink fixed-window counters','zero materialization of bounded Predict(272) and Moving Average(window) histories on first entity visit'],
       'unchanged_classes':{}}
with zipfile.ZipFile(frozen) as a,zipfile.ZipFile(jar) as z:
    for n in a.namelist():
        if n.startswith('org/apache/flink/benchmark/benchset/') and Path(n).name.startswith(('LongStateAccess','BenchsetEvent')):
            assert a.read(n)==z.read(n);proof['unchanged_classes'][n]=hashlib.sha256(a.read(n)).hexdigest()
(out/'provenance.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof),flush=True)
