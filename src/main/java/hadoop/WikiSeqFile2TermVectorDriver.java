package hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikiSeqFile2TermVectorDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("WikiSeqFile2TermVector");
    
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    
    FileSystem fs = FileSystem.get(outputPath.toUri(), getConf());
    fs.delete(outputPath, true);
    
    FileInputFormat.addInputPath(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(WikiSeqFile2TermVectorMapper.class);
    conf.setJarByClass(WikiSeqFile2TermVectorDriver.class);
    
    conf.setNumReduceTasks(0);
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    args = new String[2];
    args[0] = "wiki/wiki.seq";
    args[1] = "wiki/wiki-text";
    int exitCode = ToolRunner.run(new WikiSeqFile2TermVectorDriver(), args);
    System.exit(exitCode);
  }
  
}
