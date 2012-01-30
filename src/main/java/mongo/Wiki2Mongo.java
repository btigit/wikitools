package mongo;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;

import org.apache.tools.bzip2.CBZip2InputStream;

import tools.WikiTextParser;
import tools.WikipediaModel;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * Wikipediaのjawiki-latest-pages-articles.xmlを解析する
 */
public class Wiki2Mongo {

    /** main */
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

    	String wikiXMLFile = "jawiki-latest-pages-articles.xml.xml.bz2";
        long start = System.currentTimeMillis();

        Mongo m = new Mongo( "10.211.55.11" , 27017 );
        DB db = m.getDB( "wiki" );
        DBCollection coll = db.getCollection("docs");
        coll.ensureIndex("docid");
        coll.ensureIndex("title");
        
        BufferedInputStream bis = null;
        
        if(wikiXMLFile.endsWith(".gz")) {                
        	bis = new BufferedInputStream(new GZIPInputStream(new FileInputStream(wikiXMLFile)));
        } else if(wikiXMLFile.endsWith(".bz2")) {
            FileInputStream fis = new FileInputStream(wikiXMLFile);
            byte [] ignoreBytes = new byte[2];
            fis.read(ignoreBytes); //"B", "Z" bytes from commandline tools
            bis = new BufferedInputStream(new CBZip2InputStream(fis));
        } else {
        	bis = new BufferedInputStream(new FileInputStream(wikiXMLFile));
        }
        
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader reader = factory.createXMLEventReader(bis);

        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (WikiTextParser.isStartElem(event, "page")) {
                WikipediaModel model = WikiTextParser.pageParse(reader);
                if (model != null){
                	BasicDBObject query = new BasicDBObject();
                	query.put("docid", model.getId());
                	DBCursor cur = coll.find(query);
                	if(cur.hasNext()){
                		DBObject obj = cur.next();
                		WikipediaModel dbmodel = new WikipediaModel();
                		dbmodel.setId((Long) obj.get("docid"));
                		dbmodel.setTitle((String) obj.get("title"));
                		dbmodel.setTitleAnnotation((String) obj.get("titleAnnotation"));
                		dbmodel.setText((String) obj.get("text"));
                		dbmodel.setRedirect((Boolean) obj.get("redirect"));
                		dbmodel.setRedirectText((String) obj.get("redirectText"));
                		dbmodel.setStub((Boolean) obj.get("stub"));
                		dbmodel.setDisambiguation((Boolean) obj.get("disambiguation"));
                		dbmodel.setCategories((ArrayList<String>) obj.get("categories"));
                		dbmodel.setLinks((ArrayList<String>) obj.get("links"));
                		dbmodel.setLastModified((Date) obj.get("lastModified"));

                		if(dbmodel.getLastModified().compareTo(model.getLastModified())<=0){
                			//ignore
                		}else{
                        	coll.update(obj,getDocument(model));
                		}
                	}else{
                    	coll.insert(getDocument(model));
                	}
                }
            }
        }

        reader.close();
        System.out.println((System.currentTimeMillis() - start) + "msec");
    }
    
    private static BasicDBObject getDocument(WikipediaModel model){
        BasicDBObject doc = new BasicDBObject();
    	doc.put("docid", model.getId());
    	doc.put("title", model.getTitle());
    	doc.put("titleAnnotation", model.getTitleAnnotation());
    	doc.put("text", model.getText());
    	doc.put("redirect", model.isRedirect());
    	doc.put("redirectText", model.getRedirectText());
    	doc.put("stub", model.isStub());
    	doc.put("disambiguation", model.isDisambiguation());
    	doc.put("categories",model.getCategories());
    	doc.put("links",model.getLinks());    	
    	doc.put("lastModified", model.getLastModified());
    	return doc;
    }
}
