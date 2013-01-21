package wiki.hadoop.mapreduce.driver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.mapreduce.TitleCount;

import com.mongodb.MongoURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class TitleCount2MongoDriver {

  private static final Logger log = LoggerFactory.getLogger(TitleCount2MongoDriver.class);
  private static String inputPath;
  private static String outputPath;
  
  private static Configuration init(String[] args) throws IOException, OptionException{
    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();
    
    Option dirInputPathOpt = DefaultOptionCreator.inputOption().create();
    Option dirOutputPathOpt = DefaultOptionCreator.outputOption().create();
    Option categoriesOpt = obuilder.withLongName("categories").withArgument(
        abuilder.withName("categories").withMinimum(1).withMaximum(1).create()).withDescription(
        "Location of the categories file.  One entry per line. "
            + "Will be used to make a string match in Wikipedia Category field").withShortName("c").create();
    
    Option helpOpt = DefaultOptionCreator.helpOption();
    
    Group group = gbuilder.withName("Options").withOption(dirInputPathOpt)
        .withOption(dirOutputPathOpt).withOption(categoriesOpt).withOption(helpOpt).create();
    
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
      
      String catFile = "";
      if (cmdLine.hasOption(categoriesOpt)) {
        catFile = (String) cmdLine.getValue(categoriesOpt);
      }
      
      Configuration conf = new Configuration();
      conf.set("io.serializations",
          "org.apache.hadoop.io.serializer.JavaSerialization,"
          + "org.apache.hadoop.io.serializer.WritableSerialization");
      
      Set<String> categories = new HashSet<String>();
      if (!catFile.isEmpty()) {
        for (String line : new FileLineIterable(new File(catFile))) {
          categories.add(line.trim().toLowerCase(Locale.ENGLISH));
        }
      }
      
      DefaultStringifier<Set<String>> setStringifier =
          new DefaultStringifier<Set<String>>(conf, GenericsUtil.getClass(categories));
      
      String categoriesStr = setStringifier.toString(categories);
      conf.set("wikipedia.categories", categoriesStr);
      
      MongoConfigUtil.setOutputURI(conf, new MongoURI(outputPath));
      return conf;
    } catch (OptionException e) {
      log.error("Parse error.",e);
      CommandLineUtil.printHelp(group);
      throw e;
    }
  }
  
  private static Job createJob(Configuration conf) throws IOException{
    
    final Job job = new Job( conf, "title count" );
    job.setJarByClass( TitleCount2MongoDriver.class );
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(TitleCount.Map.class);
    job.setReducerClass(TitleCount.Reduce.class);
    
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