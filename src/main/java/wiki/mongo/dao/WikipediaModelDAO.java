package wiki.mongo.dao;

import org.bson.types.ObjectId;

import wiki.mongo.db.MongoMorphiaManager;
import wiki.mongo.entity.WikipediaModel;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.dao.BasicDAO;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;

public class WikipediaModelDAO extends BasicDAO<WikipediaModel, ObjectId> {
  
  private static WikipediaModelDAO dao;
  
  static{
    dao = new WikipediaModelDAO(MongoMorphiaManager.getDatastore());
  }
  
  private WikipediaModelDAO(Datastore ds) {
    super(ds);
  }
  
  public static WikipediaModelDAO getInstance(){
    return dao;
  }
  
  public AggregationOutput distinctTitle(long fromId, long toId){
    BasicDBObject project;
    BasicDBObject group;
    BasicDBObject match;
    BasicDBObject matchDocId;
    BasicDBObject sort;
    
    match = new BasicDBObject();
    match.append("$match", 
      new BasicDBObject()
        .append("titleAnnotation", null)
        .append("redirect", false)
        .append("stub", false)
        .append("disambiguation", false)
    );
    
    project = new BasicDBObject();
    project.append("$project", 
      new BasicDBObject()
        .append("title", 1)
        .append("titleKana", 1)
        .append("docid", 1)
    );
    group = new BasicDBObject();
    group.append("$group", 
      new BasicDBObject()
        .append("_id", new BasicDBObject("title", "$title"))
        .append("titleKana", new BasicDBObject("$first", "$titleKana"))
        .append("docid", new BasicDBObject("$first", "$docid"))
    );
    
    matchDocId = new BasicDBObject();
    matchDocId.append("$match", 
      new BasicDBObject()
        .append("docid", new BasicDBObject().append("$gte", fromId).append("$lte", toId))
    );
    
    sort = new BasicDBObject();
    sort.append("$sort", new BasicDBObject("docid", 1));
    
    return ds.getCollection(WikipediaModel.class).aggregate(match, project, group, matchDocId, sort);
  }
  
}