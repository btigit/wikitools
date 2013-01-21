package wiki.mongo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tools.bzip2.CBZip2InputStream;

import wiki.mongo.dao.WikipediaModelDAO;
import wiki.mongo.db.MongoMorphiaManager;
import wiki.mongo.entity.WikipediaModel;
import wiki.parser.WikiTextParser;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;

/**
 * Wikipediaのjawiki-latest-pages-articles.xmlを解析する
 */
public class Wiki2Mongo {
  
  public void run() throws Exception {
    String wikiXMLFile = "data/jawiki-latest-pages-articles.xml.bz2";
    long start = System.currentTimeMillis();
    
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setEncoding("UTF-8");
    config.load("conf/mongo.properties");
    
    MongoMorphiaManager.initialize(config);
    Datastore ds = MongoMorphiaManager.getDatastore();
    WikipediaModelDAO dao = WikipediaModelDAO.getInstance();
    
    ExecutorService exec = Executors.newFixedThreadPool(10);
    
    BufferedInputStream bis = null;
    
    if (wikiXMLFile.endsWith(".gz")) {
      bis = new BufferedInputStream(new GZIPInputStream(new FileInputStream(
          wikiXMLFile)));
    } else if (wikiXMLFile.endsWith(".bz2")) {
      FileInputStream fis = new FileInputStream(wikiXMLFile);
      byte[] ignoreBytes = new byte[2];
      fis.read(ignoreBytes); // "B", "Z" bytes from commandline tools
      bis = new BufferedInputStream(new CBZip2InputStream(fis), 1024 * 1024);
    } else {
      bis = new BufferedInputStream(new FileInputStream(wikiXMLFile),
          1024 * 1024);
    }
    
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLEventReader reader = factory.createXMLEventReader(bis);
    
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (WikiTextParser.isStartElem(event, "page")) {
        WikipediaModel model = WikiTextParser.pageParse(reader);
        if (model != null) {
          exec.execute(new IngestService(ds, dao, model));
        }
      }
    }
    
    reader.close();
    exec.shutdown();
    exec.awaitTermination(1, TimeUnit.DAYS);
    System.out.println((System.currentTimeMillis() - start) + "msec");
  }
  
  private class IngestService extends Thread {
    
    private Datastore ds;
    private WikipediaModelDAO dao;
    private WikipediaModel model;
    
    private IngestService(Datastore ds, WikipediaModelDAO dao,
        WikipediaModel model) {
      this.ds = ds;
      this.dao = dao;
      this.model = model;
    }
    
    @Override
    public void run() {
      try {
        WikiTextParser parser = new WikiTextParser(model.getTitle(),model.getOrgText());
        model.setText(parser.getPlainText());
        model.setTitleKana(parser.getTitleKana());
        model.setRedirect(parser.isRedirect());
        model.setRedirectText(parser.getRedirectText());
        model.setStub(parser.isStub());
        model.setDisambiguation(parser.isDisambiguationPage());
        model.setCategories(parser.getCategories());
        model.setLinks(parser.getLinks());
        
        Query<WikipediaModel> query = ds.createQuery(WikipediaModel.class);
        query.field("docid").equal(model.getDocId());
        
        WikipediaModel doc = dao.findOne(query);
        if (doc != null
            && doc.getLastModified().compareTo(model.getLastModified()) <= 0) {
          return;
        }
        
        //TODO
        query = ds.createQuery(WikipediaModel.class);
        query.field("title").equal(model.getTitle());
        
        doc = dao.findOne(query);
        if (doc != null) {
          System.out.printf("duplicate entry : %s %s"+System.lineSeparator(), model.getDocId(), model.getTitle());
          return;
        }
        
        dao.save(model);
      } catch (Exception e) {
      }
    }
  }
  
  /** main */
  public static void main(String[] args) throws Exception {
    Wiki2Mongo runner = new Wiki2Mongo();
    runner.run();
  }
  
}
