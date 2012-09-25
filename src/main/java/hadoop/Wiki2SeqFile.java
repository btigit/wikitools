package hadoop;

import hadoop.io.TextArrayWritable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import tools.WikiTextParser;
import tools.WikipediaModel;

public class Wiki2SeqFile {
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path input = new Path("file:///Users/noburton88/Developer/eclipse_workspace/neuron-dev/neuron/wikitools/input/jawiki-20120806-pages-articles.xml.bz2");
    Path output = new Path("wiki/wiki.seq");
    String[] categoriesStr = {"日本のミュージシャン","アクション映画"};
    ArrayList<String> categories = new ArrayList<String>();
    for (String category : categoriesStr) {
      categories.add(category);
    }
    
    FileSystem inputfs = FileSystem.get(input.toUri(), conf);
    FileSystem outputfs = FileSystem.get(output.toUri(), conf);
    
    if(!inputfs.exists(input)){
      System.out.println("input error");
      System.exit(1);
    }
    
    if(outputfs.exists(output)){
      //clean up output directory
      outputfs.delete(output,true);
    }
    parseWiki(inputfs, outputfs, categories, conf, input, output);
    
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
  
  private static void parseWiki(FileSystem intputfs, FileSystem outputfs, ArrayList<String> categories, Configuration conf, Path input, Path output) throws Exception{
    InputStream is = getCodecInputStream(intputfs, conf, input);
    if(is == null){
      System.out.println("bye");
      return;
    }
    BufferedInputStream bis = new BufferedInputStream(is);
    
    LongWritable key = new LongWritable();
    MapWritable value = new MapWritable();
    SequenceFile.Writer writer = SequenceFile.createWriter(outputfs, conf, output, key.getClass(), value.getClass());
    
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLEventReader reader = factory.createXMLEventReader(bis);

    try {
      long count = 0;
      while (reader.hasNext()) {
        XMLEvent event = reader.nextEvent();
        if (WikiTextParser.isStartElem(event, "page")) {
          WikipediaModel model = WikiTextParser.pageParse(reader);
          if (model != null && !model.isEmpty() && !model.isRedirect() && 
              !model.isStub() && !model.isDisambiguation() && model.getText().length() > 0){
            if(categories.size() > 0){
              boolean found = false;
              ArrayList<String> modelCategories = model.getCategories();
              for (String modelCategory : modelCategories) {
                if(categories.contains(modelCategory)){
                  found=true;
                  break;
                }
              }
              if(found){
                writeWikiDocument(writer, key, value, model);
                count++;
              }
            }else{
              writeWikiDocument(writer, key, value, model);
              count++;
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeStream(writer);
    }
  }
  
  private static void writeWikiDocument(SequenceFile.Writer writer, LongWritable key, 
      MapWritable value, WikipediaModel model) throws IOException{
    key.set(model.getId());
    value.put(new Text("title"), getText(model.getTitle()));
    value.put(new Text("titleAnnotation"), getText(model.getTitleAnnotation()));
    value.put(new Text("text"), getText(model.getText()));
    value.put(new Text("redirect"), new BooleanWritable(model.isRedirect()));
    value.put(new Text("redirectText"), getText(model.getRedirectText()));
    value.put(new Text("stub"), new BooleanWritable(model.isStub()));
    value.put(new Text("disambiguation"), new BooleanWritable(model.isDisambiguation()));
    value.put(new Text("categories"),getTextArray(model.getCategories()));
    value.put(new Text("links"), getTextArray(model.getLinks()));
    value.put(new Text("lastModified"), new LongWritable(model.getLastModified().getTime()));
    writer.append(key, value);
  }
  
  private static InputStream getCodecInputStream(FileSystem fs, Configuration conf, Path input) throws IOException{
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(input);
    if (codec == null) {
      return null;
    }else{
      return codec.createInputStream(fs.open(input));
    }
  }
  
  private static Text getText(String str){
    if(str == null)
      return new Text("");
    else
      return new Text(str);
  }
  
  private static ArrayWritable getTextArray(ArrayList<String> strArray){
    TextArrayWritable textArray = new TextArrayWritable();
    ArrayList<Text> textArrayList = new ArrayList<Text>();
    Text text = new Text();
    for (String str : strArray) {
      text.set(str);
      textArrayList.add(text);
    }
    
    textArray.set(textArrayList.toArray(new Text[0]));
    return textArray;
  }
  
}
