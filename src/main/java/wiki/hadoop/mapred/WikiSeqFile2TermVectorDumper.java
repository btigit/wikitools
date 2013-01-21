package wiki.hadoop.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.io.TextArrayWritable;
import wiki.hadoop.io.WikiWritable;

public class WikiSeqFile2TermVectorDumper {
  
  public static class Map extends MapReduceBase implements Mapper<LongWritable, WikiWritable, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(Map.class);
    
    private Set<String> inputCategories;
    private boolean exactMatchOnly;
    private boolean all;
    private boolean redirect;
    private boolean stub;
    private boolean disambiguation;

    @Override
    public void map(LongWritable key, WikiWritable value,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
      if (!redirect && value.getRedirect().get()){
        reporter.getCounter("Wikipedia", "Redirect pages").increment(1);
        return;
      }
      
      if (!stub && value.getStub().get()){
        reporter.getCounter("Wikipedia", "Stub pages").increment(1);
        return;
      }
      
      if (!disambiguation && value.getDisambiguation().get()){
        reporter.getCounter("Wikipedia", "Disambiguation pages").increment(1);
        return;
      }
      
      if (!all) {
        if (!findMatchingCategory(value.getCategories())) {
          return;
        }
      }
      reporter.getCounter("Wikipedia", "Normal pages").increment(1);
      output.collect(new Text(key.toString()), value.getText());
    }
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      if (inputCategories == null) {
        Set<String> newCategories = new HashSet<String>();

        DefaultStringifier<Set<String>> setStringifier =
            new DefaultStringifier<Set<String>>(job, GenericsUtil.getClass(newCategories));

        String categoriesStr;
        try {
          categoriesStr = job.get("wikipedia.categories", setStringifier.toString(newCategories));
          inputCategories = setStringifier.fromString(categoriesStr);
        } catch (IOException e) {
        }
      }
      exactMatchOnly = job.getBoolean("exact.match.only", false);
      all = job.getBoolean("all.files", true);
      redirect = job.getBoolean("redirect.pages", false);
      stub = job.getBoolean("stub.pages", false);
      disambiguation = job.getBoolean("disambiguation.pages", false);
      log.info("Configure: Input Categories size: {} All: {} Exact Match: {} Redirect: {} Stub: {} Disambiguation: {}",
               new Object[] {inputCategories.size(), all, exactMatchOnly, redirect, stub, disambiguation});
    }

    private boolean findMatchingCategory(TextArrayWritable textArrayWritable) {
      for (Writable category : textArrayWritable.get()) {
        if (exactMatchOnly && inputCategories.contains(category.toString())) {
          return true;
        }
        if (!exactMatchOnly) {
          for (String inputCategory : inputCategories) {
            if (category.toString().contains(inputCategory)) { // we have an inexact match
              return true;
            }
          }
        }
      }
      return false;
    }
  }
}
