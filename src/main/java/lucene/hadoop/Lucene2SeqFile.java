package lucene.hadoop;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class Lucene2SeqFile {
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String path = "/Users/noburton88/Developer/eclipse_workspace/neuron-dev/neuron/neuron/module/solr/neuron/multicore/module/data/index";
    
    Path output = new Path("lucene/lucene.seq");
    FileSystem outputfs = FileSystem.get(output.toUri(), conf);
    
    if(outputfs.exists(output)){
      //clean up output directory
      outputfs.delete(output,true);
    }
    parseLucene(outputfs, conf, output, path);
    
    FileStatus status = outputfs.getFileStatus(output.getParent());
    ls(outputfs, status);
    
  }
  
  private static void ls(FileSystem fs, FileStatus stat) throws IOException {
    print(stat);
    if (stat.isDir()) {
      FileStatus[] stats = fs.listStatus(stat.getPath());
      for (FileStatus fileStatus : stats) {
        ls(fs, fileStatus);
      }
    }
  }
  
  private static void print(FileStatus stat){
    System.out.println(stat.getPath().toUri().getPath());
  }
  
  private static void parseLucene(FileSystem outputfs, Configuration conf, Path output, String path) throws Exception{
    LongWritable key = new LongWritable();
    MapWritable value = new MapWritable();
    SequenceFile.Writer writer = SequenceFile.createWriter(outputfs, conf, output, key.getClass(), value.getClass());
    

    try {
      Directory dir = FSDirectory.open(Paths.get(path).toFile());
      DirectoryReader dreader = DirectoryReader.open(dir);
      AtomicReader reader = SlowCompositeReaderWrapper.wrap(dreader);
      
      Fields fields = reader.fields();
      TermsEnum termIds = fields.terms("id").iterator(null);
      BytesRef id;
      int doc;
      while ((id = termIds.next())!=null) {
        DocsEnum td = reader.termDocsEnum(new Term("id",id.utf8ToString()));
        if((doc = td.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
          Document document = dreader.document(doc);
          writeDocument(writer, key, value, document, doc);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeStream(writer);
    }
  }
  
  private static void writeDocument(SequenceFile.Writer writer, LongWritable key, 
      MapWritable value, Document document, int doc) throws IOException{
    key.set(doc);
    value.put(new Text("title"), getText(document.get("title")));
    value.put(new Text("text"), getText(document.get("text")));
    writer.append(key, value);
  }
  
  private static Text getText(String str){
    if(str == null)
      return new Text("");
    else
      return new Text(str);
  }
  
}
