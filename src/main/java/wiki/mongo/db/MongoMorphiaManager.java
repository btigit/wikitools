package wiki.mongo.db;

import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wiki.mongo.entity.WikipediaModel;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class MongoMorphiaManager {
  protected static final Logger logger = LoggerFactory.getLogger(MongoMorphiaManager.class);
  
  public static final String PROPERTY_HOST = "mongo.host";
  public static final String PROPERTY_PORT = "mongo.port";
  public static final String PROPERTY_DB = "mongo.db";
  public static final String PROPERTY_AUTO_CONNECT_RETRY = "mongo.auto.retry";
  public static final String PROPERTY_MAX_AUTO_CONNECT_RETRY_TIME = "mongo.max.retry.time";
  public static final String PROPERTY_CONNECTIONS = "mongo.connections";
  
  private static Morphia morphia;
  private static Mongo mongo;
  private static Datastore ds;
  
  public static void initialize(Configuration config) throws ConfigurationException {
    // Queue
    String host = config.getString(PROPERTY_HOST, "localhost");
    int port = config.getInt(PROPERTY_PORT , 27017);
    String dbName = config.getString(PROPERTY_DB);
    if (StringUtils.isEmpty(dbName))
      throw new ConfigurationException("'" + PROPERTY_DB + "' is not defined.");
    boolean autoConnectRetry = config.getBoolean(PROPERTY_AUTO_CONNECT_RETRY , true);
    int maxAutoConnectRetryTime = config.getInt(PROPERTY_MAX_AUTO_CONNECT_RETRY_TIME , 0);
    int connections = config.getInt(PROPERTY_CONNECTIONS , -1);

    MongoOptions opts = new MongoOptions();
    opts.safe = true;
    opts.autoConnectRetry = autoConnectRetry;
    opts.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
    if (connections > 0) {
      int defConns = opts.getConnectionsPerHost();
      int defBlock = opts.getThreadsAllowedToBlockForConnectionMultiplier();
      opts.setConnectionsPerHost(connections);
      opts.setThreadsAllowedToBlockForConnectionMultiplier(((int) Math.ceil((double) connections / (double) defConns))
          * defBlock);
    }
    
    try {
      morphia = new Morphia();
      //TODO 外部定義化
      morphia.map(WikipediaModel.class);
      
      mongo = new Mongo(new ServerAddress(host, port), opts);
      ds = morphia.createDatastore(mongo, dbName);
      ds.ensureIndexes();
      ds.ensureCaps();
    } catch (UnknownHostException e) {
      throw new ConfigurationException(e);
    }
  }
  
  public static Morphia getMorphia(){
    return morphia;
  }
  
  public static Datastore getDatastore(){
    return ds;
  }
  
  public static void close() {
    ds.getDB().getMongo().close();
  }

}
