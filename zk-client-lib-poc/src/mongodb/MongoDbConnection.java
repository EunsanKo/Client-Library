package mongodb;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import entity.ZooEntity;

public class MongoDbConnection {

	static private MongoDbConnection instance;
	private ZooEntity ze;
	private MongoDbConnection(){
		
	}
	
	static public MongoDbConnection getInstance(){
		if(instance==null){
			instance = new MongoDbConnection();
		}
		return instance;
	}
	
	public void refreshMongosServer(ZooEntity ze){
		this.ze = ze;
		this.refreshConnectionPool();
	}
	
	
	/**
	 * Set connection pool based on redis shard rule
	 * 
	 */
	private void refreshConnectionPool(){
		String mstSvr = ze.getMongosServer();
		String[] mstIpPort = mstSvr.split(":");
		String mstIp = mstIpPort[0];
		int mstPort = Integer.parseInt(mstIpPort[1]);
		
		
		try {
			Mongo mongo = new Mongo(mstIp, mstPort );

			DB db = mongo.getDB("PlayDB");
			DBCollection collection = db.getCollection("user");
			System.out.println("ÀüÃ¼ users °¹¼ö : " + collection.getCount()); 
			DBObject doc = new BasicDBObject(); 
			DBCursor cursor = collection.find(); 
			while (cursor.hasNext()) { 
				DBObject smith = cursor.next(); 
				System.out.println("id : " + smith.get("id")); 
			} 


			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	

	
	
}
