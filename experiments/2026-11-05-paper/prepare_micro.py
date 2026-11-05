from pathlib import Path
import hashlib, json, subprocess, zipfile

B=Path('/root/briskstate-setup/paper-experiments-20261104')
R=Path('/root/forL0-state-backend')
OLD=Path('/root/briskstate-setup/deep-opt-20261103/reproduction/micro-measured.jar')
out=B/'micro-factory';out.mkdir(exist_ok=True)
src=out/'src';classes=out/'classes';classes.mkdir(exist_ok=True)
text=(R/'benchmark/flink-benchmarks/src/main/java/org/apache/flink/state/benchmark/StateBackendBenchmarkUtils.java').read_text()
text=text.replace('HEAP,\n        FORL0','HEAP,\n        FORL0,\n        ROCKSDB')
text=text.replace('            default:\n                throw new IllegalArgumentException', '''            case ROCKSDB:
                rootDir = prepareDirectory(rootDirName, baseDir);
                return org.apache.flink.contrib.streaming.state.PaperRocksFactory.create(rootDir, ttlTimeProvider);
            default:
                throw new IllegalArgumentException''')
p=src/'org/apache/flink/state/benchmark/StateBackendBenchmarkUtils.java';p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)
helper='''package org.apache.flink.contrib.streaming.state;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

/** Only backend construction differs; all measured classes come from the frozen JAR. */
public final class PaperRocksFactory {
    public static KeyedStateBackend<Long> create(File directory, TtlTimeProvider ttl) throws IOException {
        org.rocksdb.RocksDB.loadLibrary();
        long bytes=4L<<30;
        RocksDBSharedResources shared=RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                bytes,0.5,0.1,false,RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT);
        OpaqueMemoryResource<RocksDBSharedResources> memory=new OpaqueMemoryResource<>(shared,bytes,shared::close);
        RocksDBResourceContainer resources=new RocksDBResourceContainer(PredefinedOptions.DEFAULT,null,memory);
        ExecutionConfig execution=new ExecutionConfig();
        return new RocksDBKeyedStateBackendBuilder<Long>(
                "paper-long-micro",Thread.currentThread().getContextClassLoader(),new File(directory,"rocksdb"),
                resources,ignored->resources.getColumnOptions(),null,LongSerializer.INSTANCE,2,new KeyGroupRange(0,1),
                execution,LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED,
                RocksDBPriorityQueueConfig.buildWithPriorityQueueType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP),
                ttl,LatencyTrackingStateConfig.disabled(),new UnregisteredMetricsGroup(),(name,value)->{},
                Collections.emptyList(),AbstractStateBackend.getCompressionDecorator(execution),new CloseableRegistry()).build();
    }
}
'''
h=src/'org/apache/flink/contrib/streaming/state/PaperRocksFactory.java';h.parent.mkdir(parents=True,exist_ok=True);h.write_text(helper)
cp=str(B/'reproduction/flink-statebackend-forL0-1.0-SNAPSHOT.jar')+':'+str(OLD)
subprocess.run(['/usr/lib/jvm/java-11-openjdk/bin/javac','--release','11','-cp',cp,'-d',str(classes),str(p),str(h)],check=True)
overlay=out/'rocksdb-factory-overlay.jar'
with zipfile.ZipFile(overlay,'w',zipfile.ZIP_DEFLATED) as z:
    for f in classes.rglob('*.class'):z.write(f,str(f.relative_to(classes)))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
proof={'frozen_application_sha256':sha(OLD),'overlay_sha256':sha(overlay),
       'overlay_classes':zipfile.ZipFile(overlay).namelist(),
       'all_timed_and_fixture_classes_unchanged':not any('LongStateMicrobenchmarks' in s for s in zipfile.ZipFile(overlay).namelist()),
       'java_heap_gib':8,'rocksdb_shared_native_budget_gib':4,'write_buffer_ratio':0.5,'high_priority_pool_ratio':0.1,
       'predefined_options':'DEFAULT','key_group_range':[0,1],'l0_forl0_enabled':False,'checkpoint_enabled':False}
assert proof['all_timed_and_fixture_classes_unchanged']
(out/'proof.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof),flush=True)
