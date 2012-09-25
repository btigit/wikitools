package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WikiSeqFile2TermVectorMapper extends MapReduceBase 
  implements Mapper<LongWritable, MapWritable, Text, Text>{

  public void map(LongWritable key, MapWritable value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    output.collect(new Text(key.toString()), (Text) value.get(new Text("text")));
  }
  
}
