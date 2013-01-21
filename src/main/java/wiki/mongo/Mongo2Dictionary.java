package wiki.mongo;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;
import org.atilika.kuromoji.Tokenizer.Mode;

import wiki.mongo.dao.WikipediaModelDAO;
import wiki.mongo.db.MongoMorphiaManager;
import wiki.mongo.entity.WikipediaModel;

import com.google.code.morphia.Datastore;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Wikipediaのjawiki-latest-pages-articles.xmlを解析する
 */
public class Mongo2Dictionary {
  
  public void run() throws Exception{
    long start = System.currentTimeMillis();
    
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setEncoding("UTF-8");
    config.load("conf/mongo.properties");
    
    MongoMorphiaManager.initialize(config);
    Datastore ds = MongoMorphiaManager.getDatastore();
    WikipediaModelDAO dao = WikipediaModelDAO.getInstance();
    int readerNumber = 10;
    
    List<WikipediaModel> lastIdList = ds.find(WikipediaModel.class).order("-docid").limit(1).asList();
    long lastId = lastIdList.get(0).getDocId();
    long increment = lastId/readerNumber;
    
    ExecutorService exec = Executors.newFixedThreadPool(readerNumber);
    for (int i = 0; i < readerNumber; i++) {
      long fromId = i * increment;
      long toId = readerNumber - 1 == i ? lastId : (i+1) * increment - 1;
      Path dictPath = Paths.get("data/dictionary-"+i+".csv");
      Files.deleteIfExists(dictPath);
      Files.createFile(dictPath);
      System.out.println(dictPath+" "+fromId+"-"+toId);
      exec.execute(new ReaderService(ds, dao, dictPath, fromId, toId));
    }
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.DAYS);
    Path dictPath = Paths.get("src/main/resources/mahout/vectorizer/dictionary.csv");
    Files.deleteIfExists(dictPath);
    Files.createFile(dictPath);
    
    Path dictDir = Paths.get("data");
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dictDir,"dictionary-*.csv");
        OutputStream os = Files.newOutputStream(dictPath, StandardOpenOption.APPEND)) {
      for (Path part: stream) {
        Files.copy(part, os);
//        Files.delete(part);
      }
    }
    
    System.out.println((System.currentTimeMillis() - start) + "msec");
  }
  
  private class ReaderService extends Thread {
    
    private Datastore ds;
    private WikipediaModelDAO dao;
    private Path dictPath;
    private long fromId;
    private long toId;
    private Tokenizer tokenizer = Tokenizer.builder().mode(Mode.NORMAL).build();
    
    private ReaderService(Datastore ds, WikipediaModelDAO dao, Path dictPath, long fromId, long toId){
      this.ds = ds;
      this.dao = dao;
      this.dictPath = dictPath;
      this.fromId = fromId;
      this.toId = toId;
    }
    
    @Override
    public void run() {
      try (BufferedWriter bw = 
          Files.newBufferedWriter(dictPath, Charset.forName("UTF-8"), StandardOpenOption.WRITE)){
        AggregationOutput res = dao.distinctTitle(fromId, toId);
        
        for (DBObject result : res.results()) {
          BasicDBObject id = (BasicDBObject) result.get("_id");
          String title = id.get("title").toString().replaceAll("[\\s,.!/]", "");
          String kana = result.get("titleKana").toString().replaceAll("[\\s,.!/]", "");
          List<Token> tokenList = tokenizer.tokenize(title);
          if(tokenList.size() == 1 && tokenList.get(0).isKnown()){
            continue;
          }
          if(tokenList.size() > 5 || filter(title).length() == 0 
              || title.length() > 15 || filter(kana).length() == 0
              || kana.replaceAll("[ァ-ヶ]","").length() == kana.length()){
            continue;
          }
          bw.write(title+","+title+","+kana+","
                  +"カスタム名詞"+System.lineSeparator());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    private String filter(String in){
      //句読文字: !"#$%&'()*+,-./:;<=>?@[\]^_`{｜}~ のいずれか
      return in.replaceAll("[\\p{Punct}0-9・]", "");
    }
  }

  /** main */
  public static void main(String[] args) throws Exception {
    Mongo2Dictionary runner = new Mongo2Dictionary();
    runner.run();
  }
  
}
