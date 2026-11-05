import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.*;
import java.util.zip.GZIPOutputStream;
import com.google.gson.*;
import org.apache.flink.benchmark.benchset.BenchsetEvent;

/** Replays only the source outside Flink and checks the closed-form feature sum. */
public final class PaperSourceOracle {
 public static void main(String[] args)throws Exception {
  Path control=Paths.get(args[0]);int keys=Integer.parseInt(args[1]);String workload=args[2];
  long[] counts=new long[keys];long total=0,sourceSignature=0;
  for(int index=0;index<8;index++) {
   JsonObject source=JsonParser.parseString(new String(Files.readAllBytes(control.resolve("source-"+index+".json")),StandardCharsets.UTF_8)).getAsJsonObject();
   long records=source.get("records").getAsLong();long signature=0;
   SplittableRandom random=new SplittableRandom(20261102L+index);
   for(long n=0,sequence=index;n<records;n++,sequence+=8) {
    int key=sequence<keys?(int)sequence:random.nextInt(keys);counts[key]++;signature+=BenchsetEvent.mix(sequence);
   }
   if(source.has("signature") && signature!=source.get("signature").getAsLong())throw new AssertionError("Source signature mismatch: "+index);
   if(source.has("next_sequence") && source.get("next_sequence").getAsLong()!=index+8*records)throw new AssertionError("Sequence-prefix mismatch");
   total+=records;sourceSignature+=signature;
  }
  long squares=0,scoreSquares=0;
  for(int k=0;k<keys;k++) {
   long n=counts[k];squares+=n*n;
   long score=7*n+counts[(k+keys-1)%keys];scoreSquares+=score*score;
  }
  long coefficient;
  if(workload.equals("lg"))coefficient=6*squares+3*total;
  else if(workload.equals("lr"))coefficient=92*squares+12*total;
  else if(workload.equals("vs"))coefficient=2*scoreSquares+8*total;
  else coefficient=2*squares+total;
  long expected=64L*1000000007L*coefficient;
  Path histogram=control.getParent().resolve("source-key-histogram.bin.gz");
  try(DataOutputStream stream=new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(histogram)),65536))) {
   stream.writeInt(keys);for(long n:counts)stream.writeLong(n);
  }
  Map<String,Object> result=new LinkedHashMap<>();result.put("source_records",total);result.put("source_signature",sourceSignature);
  result.put("sum_key_counts_squared",squares);result.put("score_key_counts_squared",scoreSquares);
  result.put("expected_feature_projection",expected);result.put("keys",keys);result.put("workload",workload);
  System.out.println(new Gson().toJson(result));
 }
}
