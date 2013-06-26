package entity;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;

public class ZooEntity {

	private String serverIp;
	private String mongosServer;
	
	


	private ZooKeeper zk;

	private int redisMaxActive;
	private int redisMaxIdle;
	

	/**
	 * db node path
	 */
	private static String REDIS_Z_NODE = "/db/redis"; 
	private static String REDIS_SHARD_RULE = "/shard-rule"; 
	private static String REDIS_SHARDS = "/shards"; 
	private static String REDIS_SHARD_MASTER = "/master"; 

	private static String MONGODB_Z_NODE = "/db/mongodb"; 
	private static String MONGODB_MONGOS = "/mongos"; 
	private static String MONGODB_SHARDS = "/shards"; 
	private static String MONGODB_CONFIGS = "/configs";
	
	/**
	 * redis-shard-rule
	 */
	private String redisShardRule[];

	/**
	 * mongos
	 */
	private List<String> mongoConfig = new ArrayList<String>();
	

	
	public int getRedisMaxActive() {
		return redisMaxActive;
	}
	public void setRedisMaxActive(int redisMaxActive) {
		this.redisMaxActive = redisMaxActive;
	}
	public int getRedisMaxIdle() {
		return redisMaxIdle;
	}
	public void setRedisMaxIdle(int redisMaxIdle) {
		this.redisMaxIdle = redisMaxIdle;
	}
	public String[] getRedisShardRule() {
		return redisShardRule;
	}
	public void setRedisShardRule(String[] redisShardRule) {
		this.redisShardRule = redisShardRule;
	}
	public List<String> getMongoConfig() {
		return mongoConfig;
	}
	public void setMongoConfig(List<String> mongoConfig) {
		this.mongoConfig = mongoConfig;
	}
	public String getServerIp() {
		return serverIp;
	}
	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}
	public ZooKeeper getZk() {
		return zk;
	}
	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
	public String getMongosServer() {
		return mongosServer;
	}
	public void setMongosServer(String mongosServer) {
		this.mongosServer = mongosServer;
	}
	public String getRedisNodePath(){
		return REDIS_Z_NODE;
	}

	public String getRedisShardRuleNodePath(){
		return REDIS_Z_NODE+REDIS_SHARD_RULE;
	}

	public String getRedisShardsNodePath(){
		return REDIS_Z_NODE+REDIS_SHARDS;
	}

	public String getRedisShardMasterPath(){
		return REDIS_SHARD_MASTER;
	}
	
	public String getMongodbNodePath(){
		return MONGODB_Z_NODE;
	}
	
	public String getMongodbMongosPath(){
		return MONGODB_Z_NODE+MONGODB_MONGOS;
	}
	
	
	public String getMongodbConfigsPath(){
		return MONGODB_Z_NODE+MONGODB_CONFIGS;
	}


	
	
}
