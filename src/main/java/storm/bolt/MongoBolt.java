package storm.bolt;

import java.util.Map;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MongoBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private DB mongoDB;
	
	private final String mongoHost;
	private final int mongoPort;
	private final String mongoDbName;
	private final String mongoCollectionName;

	/**
	 * @param mongoHost The host on which Mongo is running.
	 * @param mongoPort The port on which Mongo is running.
	 * @param mongoDbName The Mongo database containing all collections being
	 * written to.
	 */
	public MongoBolt(String mongoHost, int mongoPort, String mongoDbName,
			String mongoCollectionName) {
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.mongoDbName = mongoDbName;
		this.mongoCollectionName = mongoCollectionName;
	}
	
	
	public void prepare(
		@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.mongoDB = new Mongo(mongoHost, mongoPort).getDB(mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void execute(Tuple input) {
		if (shouldActOnInput(input)) {
			String collectionName = getMongoCollectionForInput(input);
			DBObject query = getDBObjectForQuery(input);
			DBObject updateObj = getDBObjectForInput(input);
			
			if (query != null) {
				try {
					mongoDB.getCollection(collectionName).update(query, updateObj,true,false,new WriteConcern(1));
					collector.ack(input);
				} catch (MongoException me) {
					collector.fail(input);
				}
			}
		} else {
			collector.ack(input);
		}
	}
	
	public DBObject getDBObjectForQuery(Tuple input) {
		BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
		dbObjectBuilder.append("campaign_id", 1);
		dbObjectBuilder.append("word", input.getString(0));
		return dbObjectBuilder.get();
	}
	
	public DBObject getDBObjectForInput(Tuple input) {
		BasicDBObjectBuilder inc = new BasicDBObjectBuilder();
		BasicDBObjectBuilder update = new BasicDBObjectBuilder();
		inc.append("count", 1);
		update.append("$inc", inc.get());
		return update.get();
	}
	
	public void cleanup() {
		this.mongoDB.getMongo().close();
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public String getMongoCollectionForInput(Tuple input) {
		return mongoCollectionName;
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean shouldActOnInput(Tuple input) {
		return true;
	}
}
