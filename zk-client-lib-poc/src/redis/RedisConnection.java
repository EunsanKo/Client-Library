package redis;

import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.ConsistentHash;
import util.HashFunction;
import connector.ZookeeperConnector;
import entity.ZooEntity;

public class RedisConnection {

	static private RedisConnection instance;
	private String redisShardRule[];
	private ZooEntity ze;
	private HashMap<String, JedisPool> ConnectionMap = new HashMap<>();
	private HashFunction hashFunction;
	private ConsistentHash<String> ch;
	private int redisMaxActive;
	private int redisMaxIdle;
	private RedisConnection(){
		
	}
	
	static public RedisConnection getInstance(){
		if(instance==null){
			instance = new RedisConnection();
		}
		return instance;
	}
	
	public void refreshShardRule(ZooEntity ze){
		this.redisShardRule = ze.getRedisShardRule();
		this.ze = ze;
		this.redisMaxActive = ze.getRedisMaxActive();
		this.redisMaxIdle = ze.getRedisMaxIdle();
		this.refreshConnectionPool();
	}
	
	private void makeConsistentHashing(){
		hashFunction  = new HashFunction();
		ch = new ConsistentHash<>(hashFunction, 10, ConnectionMap.keySet());
	}
	
	/**
	 * Set connection pool based on redis shard rule
	 * 
	 */
	private void refreshConnectionPool(){
		for(int inx=0;inx<redisShardRule.length;inx++){
			String shard = redisShardRule[inx];
			
			ZookeeperConnector zc = new ZookeeperConnector();
			try {
				List<String> mstList = zc.getChildren(ze.getZk(), ze.getRedisShardsNodePath()+"/"+shard+ze.getRedisShardMasterPath());
				
				/**
				 * Only one master ip
				 */
				String mstSvr = mstList.get(0);
				String[] mstIpPort = mstSvr.split(":");
				String mstIp = mstIpPort[0];
				int mstPort = Integer.parseInt(mstIpPort[1]);
				
				
				JedisPoolConfig config = new JedisPoolConfig();
				config.setMaxActive(redisMaxActive);
				config.setMaxIdle(redisMaxIdle);
				JedisPool pool = new JedisPool(config,mstIp,mstPort);
				
				ConnectionMap.put(shard, pool);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		this.makeConsistentHashing();
	}
	

	
	
	/**
	 * jedis set mehtod
	 * @param key
	 * @param value
	 */
	public void set(String key, String value){
		JedisPool pool = ConnectionMap.get(ch.get(key));
		Jedis resource=pool.getResource();
		resource.set(key, value);
		pool.returnResource(resource);
		
	}

	/**
	 * jedis get method
	 * @param key
	 * @return
	 */
	public String get(String key){
		
		JedisPool pool = ConnectionMap.get(ch.get(key));
		Jedis resource=pool.getResource();
		String value = resource.get(key);
		pool.returnResource(resource);
		return value;
	}
	
	
}
