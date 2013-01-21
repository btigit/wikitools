package wiki.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import wiki.hadoop.io.KeyTextOrderIntWritable;
import wiki.hadoop.io.TextArrayWritable;
import wiki.hadoop.io.WikiWritable;

public class CategoryCount {
  
  public static class Map extends Mapper<LongWritable, WikiWritable, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, WikiWritable value, Context context) throws IOException, InterruptedException {
      TextArrayWritable categories = value.getCategories();
      if(categories != null && categories.get() != null){
        context.getCounter("Wikipedia", "Pages").increment(1);
        for (Writable category : categories.get()) {
          word.set(category.toString());
          context.write(word, one);
        }
      }
    }
  }
  
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
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
        context.getCounter("Wikipedia", "Categories").increment(1);
        context.write(value.getKey(), value.getOrder());
      }
    }
  }
}
