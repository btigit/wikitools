package wiki.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import wiki.hadoop.io.TextArrayWritable;
import wiki.hadoop.io.WikiWritable;

public class WikiWritableSerDe implements SerDe{

  private ObjectInspector cachedInspector;
  private final List<Object> deserializeCache = new ArrayList<Object>(11);
  
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    String columnProperty = tbl.getProperty("columns");
    List<String> columnList = Arrays.asList(columnProperty.split(","));
    cachedInspector = new WikiInspector(columnList);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedInspector;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    WikiWritable ww = (WikiWritable) blob;
    deserializeCache.clear();
    deserializeCache.add(ww.getDocid());
    deserializeCache.add(ww.getTitle());
    deserializeCache.add(ww.getTitleAnnotation());
    deserializeCache.add(ww.getTitleKana());
    deserializeCache.add(ww.getText());
    deserializeCache.add(ww.getRedirectText());
    {
      List<Writable> array = new ArrayList<>();
      TextArrayWritable arrayWritable = ww.getCategories();
      if(arrayWritable != null && arrayWritable.get() != null){
        for (Writable ele : arrayWritable.get()) {
          array.add(ele);
        }
      }
      deserializeCache.add(array);
    }
    {
      List<Writable> array = new ArrayList<>();
      TextArrayWritable arrayWritable = ww.getLinks();
      if(arrayWritable != null && arrayWritable.get() != null){
        for (Writable ele : arrayWritable.get()) {
          array.add(ele);
        }
      }
      deserializeCache.add(array);
    }
    deserializeCache.add(ww.getRedirect());
    deserializeCache.add(ww.getStub());
    deserializeCache.add(ww.getDisambiguation());
    return deserializeCache;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return WikiWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    throw new UnsupportedOperationException("No serialize operation for " + this.getClass().getName());
  }
  
  static class WikiInspector extends StandardStructObjectInspector {
    protected WikiInspector(List<String> names) {
      super(names, getFieldObjectInspectors());
    }
    
    static List<ObjectInspector> getFieldObjectInspectors() {
      List<ObjectInspector> r = new ArrayList<ObjectInspector>(11);
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.LONG));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING));
      r.add(ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING)));
      r.add(ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING)));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BOOLEAN));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BOOLEAN));
      r.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BOOLEAN));
      return r;
    }
  }
  
}
