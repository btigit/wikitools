package wiki.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyTextOrderIntGroupingComparator extends WritableComparator {
  
  public KeyTextOrderIntGroupingComparator(){
    super(KeyTextOrderIntWritable.class, true);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    Comparable one = KeyTextOrderIntWritable.class.cast(a).getKey();
    Comparable another = KeyTextOrderIntWritable.class.cast(b).getKey();
    return one.compareTo(another);
  }

}