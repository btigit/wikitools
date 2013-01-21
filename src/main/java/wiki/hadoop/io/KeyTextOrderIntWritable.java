package wiki.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyTextOrderIntWritable implements WritableComparable<KeyTextOrderIntWritable> {

  private Text key;
  private IntWritable order;
  
  public KeyTextOrderIntWritable(){
    this.key = new Text();
    this.order = new IntWritable();
  }
  
  public KeyTextOrderIntWritable(Text key, IntWritable order){
    this.key = key;
    this.order = order;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    order.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    order.readFields(in);
  }

  public Text getKey() {
    return key;
  }
  
  public IntWritable getOrder() {
    return order;
  }

  @Override
  public int compareTo(KeyTextOrderIntWritable other) {
    return this.key.compareTo(other.getKey());
  }

}