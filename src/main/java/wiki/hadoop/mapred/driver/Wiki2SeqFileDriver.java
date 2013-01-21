package wiki.hadoop.mapred.driver;

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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.io.WikiWritable;
import wiki.hadoop.mapred.WikipediaDumper;
import wiki.hadoop.mapred.lib.input.StreamWikiDumpInputFormat;

public class Wiki2SeqFileDriver extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(Wiki2SeqFileDriver.class);
  private String inputPath;
  private String outputPath;
  private String catFile;
  private boolean all;
  private boolean redirect;
  private boolean stub;
  private boolean disambiguation;
  
  @Override
  public int run(String[] args) throws Exception {
    init(args);
    
    List<JobConf> jobs = new ArrayList<>();
    jobs.add(createDump());
    
    for (JobConf jobConf : jobs) {
      RunningJob status = JobClient.runJob(jobConf);
      status.waitForCompletion();
      if (!status.isSuccessful()) {
        return -1;
      }
    }
    return 0;
  }
  
  public JobConf createDump() throws IOException{
    JobConf conf = new JobConf(Wiki2SeqFileDriver.class);
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");
    conf.setBoolean("all.files", all);
    conf.setBoolean("redirect.pages", redirect);
    conf.setBoolean("stub.pages", stub);
    conf.setBoolean("disambiguation.pages", disambiguation);
    conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization,"
             + "org.apache.hadoop.io.serializer.WritableSerialization");
    
    conf.setJobName("wiki2seq");
    conf.setMapperClass(WikipediaDumper.Map.class);
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(WikiWritable.class);
    
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(WikiWritable.class);
    
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(StreamWikiDumpInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    if (log.isInfoEnabled()) {
      log.info("Input: {} Out: {} Categories: {} All Files: {} Redirect: {} Stub: {} Disambiguation: {}", new Object[] {inputPath, outputPath, catFile, all, redirect, stub, disambiguation});
    }
    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    
    MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
        LongWritable.class, Text.class);
    
    conf.set("mapred.compress.map.output", "true"); 
    conf.set("mapred.map.output.compression.type","BLOCK"); 
    conf.set("mapred.output.compress", "true"); 
    conf.set("mapred.output.compression.type","BLOCK"); 
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
    
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
    conf.setBoolean("org.wikimedia.wikihadoop.previousRevision", false);
    
    HadoopUtil.delete(conf, new Path(outputPath));
    return conf;
  }
  
  private void init(String[] args) throws OptionException{
    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();
    
    Option dirInputPathOpt = DefaultOptionCreator.inputOption().create();
    
    Option dirOutputPathOpt = DefaultOptionCreator.outputOption().create();
    
    Option categoriesOpt = obuilder.withLongName("categories").withArgument(
      abuilder.withName("categories").withMinimum(1).withMaximum(1).create()).withDescription(
      "Location of the categories file.  One entry per line. "
          + "Will be used to make a string match in Wikipedia Category field").withShortName("c").create();
    
    Option allOpt = obuilder.withLongName("all")
        .withDescription("If set, Select all files. Default is false").withShortName("all").create();
    
    Option redirectOpt = obuilder.withLongName("redirect")
        .withDescription("If set, Select redirect pages. Default is false").withShortName("redirect").create();
    
    Option stubOpt = obuilder.withLongName("stub")
        .withDescription("If set, Select stub pages. Default is false").withShortName("stub").create();
    
    Option disambiguationOpt = obuilder.withLongName("disambiguation")
        .withDescription("If set, Select disambiguation pages. Default is false").withShortName("disambiguation").create();
    
    Option helpOpt = DefaultOptionCreator.helpOption();
    
    Group group = gbuilder.withName("Options").withOption(categoriesOpt).withOption(dirInputPathOpt)
        .withOption(dirOutputPathOpt).withOption(allOpt).withOption(helpOpt)
        .withOption(redirectOpt).withOption(stubOpt).withOption(disambiguationOpt).create();
    
    Parser parser = new Parser();
    parser.setGroup(group);
    parser.setHelpOption(helpOpt);
    try {
      CommandLine cmdLine = parser.parse(args);
      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return;
      }
      
      inputPath = (String) cmdLine.getValue(dirInputPathOpt);
      outputPath = (String) cmdLine.getValue(dirOutputPathOpt);
      
      catFile = "";
      if (cmdLine.hasOption(categoriesOpt)) {
        catFile = (String) cmdLine.getValue(categoriesOpt);
      }
      
      all = false;
      if (cmdLine.hasOption(allOpt)) {
        all = true;
      }
      redirect = false;
      if (cmdLine.hasOption(redirectOpt)) {
        redirect = true;
      }
      stub = false;
      if (cmdLine.hasOption(stubOpt)) {
        stub = true;
      }
      disambiguation = false;
      if (cmdLine.hasOption(disambiguationOpt)) {
        disambiguation = true;
      }
    } catch (OptionException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
      throw e;
    }
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Wiki2SeqFileDriver(), args);
    System.exit(res);
  }
  
}