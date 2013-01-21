package wiki.hadoop.mapreduce.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.mapreduce.CategoryCount;

import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class CategoryCount2MongoDriver {

  private static final Logger log = LoggerFactory.getLogger(CategoryCount2MongoDriver.class);
  private static String inputPath;
  private static String outputPath;
  
  private static Configuration init(String[] args) throws IOException, OptionException{
    GroupBuilder gbuilder = new GroupBuilder();
    
    Option dirInputPathOpt = DefaultOptionCreator.inputOption().create();
    Option dirOutputPathOpt = DefaultOptionCreator.outputOption().create();
    
    Option helpOpt = DefaultOptionCreator.helpOption();
    
    Group group = gbuilder.withName("Options").withOption(dirInputPathOpt)
        .withOption(dirOutputPathOpt).withOption(helpOpt).create();
    
    Parser parser = new Parser();
    parser.setGroup(group);
    parser.setHelpOption(helpOpt);
    try {
      CommandLine cmdLine = parser.parse(args);
      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return null;
      }
      inputPath = (String) cmdLine.getValue(dirInputPathOpt);
      outputPath = (String) cmdLine.getValue(dirOutputPathOpt);
      
      Configuration conf = new Configuration();
      MongoConfigUtil.setOutputURI(conf, new MongoURI(outputPath));
      
      return conf;
    } catch (OptionException e) {
      log.error("Parse error.",e);
      CommandLineUtil.printHelp(group);
      throw e;
    }
  }
  
  private static Job createJob(Configuration conf) throws IOException{
    
    final Job job = new Job( conf, "category count" );
    job.setJarByClass( CategoryCount2MongoDriver.class );
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(CategoryCount.Map.class);
    job.setReducerClass(CategoryCount.Reduce.class);
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(MongoOutputFormat.class);
    
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    return job;
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = init(args);
    List<Job> jobs = new ArrayList<>();
    if(conf != null){
      jobs.add(createJob(conf));
    }
    
    for (Job job : jobs) {
      if(!job.waitForCompletion( true ))
        return;
    }
  }
  
}