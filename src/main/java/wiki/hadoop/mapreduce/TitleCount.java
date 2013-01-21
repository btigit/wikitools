package wiki.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.hadoop.io.KeyTextOrderIntWritable;
import wiki.hadoop.io.WikiWritable;

public class TitleCount {
  
  public static class Map extends Mapper<LongWritable, WikiWritable, Text, IntWritable> {
    private static final Logger log = LoggerFactory.getLogger(Map.class);
    private final static IntWritable one = new IntWritable(1);
    private Set<String> inputCategories;
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      Configuration job = context.getConfiguration();
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
      log.info("Configure: Input Categories size: {}",
               new Object[] {inputCategories.size()});
    }
    
    @Override
    public void map(LongWritable key, WikiWritable value, Context context) throws IOException, InterruptedException {
        if(value.getCategories() != null && !findMatchingCategory(value.getCategories().get())){
          return;
        }
        context.getCounter("Wikipedia", "Pages").increment(1);
        context.write(value.getTitle(), one);
    }
    
    private boolean findMatchingCategory(Writable[] writables) {
      for (Writable category : writables) {
        if (inputCategories.contains(category.toString())) {
          return true;
        }
        for (String inputCategory : inputCategories) {
          if (category.toString().contains(inputCategory)) { // we have an inexact match
            return true;
          }
        }
      }
      return false;
    }
  }
  
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
  
  public static class SortMap extends Mapper<Text, IntWritable, KeyTextOrderIntWritable, KeyTextOrderIntWritable> {
    private final static Text fixKey = new Text("key");
    
    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
      context.write(new KeyTextOrderIntWritable(fixKey, value), new KeyTextOrderIntWritable(key, value));
    }
  }
  
  public static class SortReduce extends Reducer<KeyTextOrderIntWritable, KeyTextOrderIntWritable, Text, IntWritable> {
    @Override
    public void reduce(KeyTextOrderIntWritable key, Iterable<KeyTextOrderIntWritable> values,
        Context context)
        throws IOException, InterruptedException {
      for (KeyTextOrderIntWritable value : values) {
        context.getCounter("Wikipedia", "Titles").increment(1);
        context.write(value.getKey(), value.getOrder());
      }
    }
  }
  
}
