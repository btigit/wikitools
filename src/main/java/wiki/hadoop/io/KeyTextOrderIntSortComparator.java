package wiki.hadoop.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyTextOrderIntSortComparator extends WritableComparator {
  
  public KeyTextOrderIntSortComparator(){
    super(KeyTextOrderIntWritable.class, true);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    Comparable oneKey = KeyTextOrderIntWritable.class.cast(a).getKey();
    Comparable anotherKey = KeyTextOrderIntWritable.class.cast(b).getKey();
    
    int keyCompare = oneKey.compareTo(anotherKey);
    if (keyCompare != 0) {
      return keyCompare;
    }
    
    Comparable oneOrder = KeyTextOrderIntWritable.class.cast(a).getOrder();
    Comparable anotherOrder = KeyTextOrderIntWritable.class.cast(b).getOrder();
    return -oneOrder.compareTo(anotherOrder);
  }

}