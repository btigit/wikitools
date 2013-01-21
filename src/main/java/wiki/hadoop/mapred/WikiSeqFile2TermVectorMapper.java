package wiki.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import wiki.hadoop.io.WikiWritable;

public class WikiSeqFile2TermVectorMapper extends MapReduceBase 
  implements Mapper<LongWritable, WikiWritable, Text, Text>{

  public void map(LongWritable key, WikiWritable value,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    output.collect(new Text(key.toString()), value.getText());
  }
  
}
