package redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.ConsistentHash;
import util.HashFunction;
import watcher.ZNodeMonitor;
import connector.ZookeeperConnector;
import entity.ZooEntity;
import event.RedisShardMasterListenerImpl;

public class RedisConnection {

	static private RedisConnection instance;
	private String redisShardRule[];
	private ZooEntity ze;
	private HashMap<String, JedisPool> ConnectionMap = new HashMap<>();
	private HashFunction hashFunction;
	private ConsistentHash<String> ch;
	private int redisMaxActive;
	private int redisMaxIdle;
	static private HashMap<String, String> poolEnable = new HashMap<>();
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
		this.setShardMasterNodeWathcer();
	}
	
	public void setShardMasterNodeWathcer(){
		
		try {
			
			for(int inx=0;inx<redisShardRule.length;inx++){
				String shard = redisShardRule[inx];
				String masterNodePath = ze.getRedisShardsNodePath()+"/"+shard+ze.getRedisShardMasterPath();
				ZNodeMonitor redisShardMasterWatcher = new ZNodeMonitor(ze.getZk(), masterNodePath);
				
				RedisShardMasterListenerImpl redisListener = new RedisShardMasterListenerImpl(ze,shard);
				redisShardMasterWatcher.setListener(redisListener);
				
				ze.getZk().getChildren(masterNodePath, redisShardMasterWatcher);
				
			}
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setRedisPoolDisable(String shard){
		poolEnable.remove(shard);
		poolEnable.put(shard, "N");
	}
	
	private void makeConsistentHashing(){
		hashFunction  = new HashFunction();
		ch = new ConsistentHash<>(hashFunction, 10, ConnectionMap.keySet());
	}
	
	/**
	 * Set connection pool based on redis shard rule
	 * 
	 */
	public void refreshConnectionPool(){
		
		String[] usableShard = null;
		List<String> usableShardList = new ArrayList<>(); 
		
		for(int inx=0;inx<redisShardRule.length;inx++){
			String shard = redisShardRule[inx];
			
			ZookeeperConnector zc = new ZookeeperConnector();
			try {
				List<String> mstList = zc.getChildren(ze.getZk(), ze.getRedisShardsNodePath()+"/"+shard+ze.getRedisShardMasterPath());
				
				/**
				 * Only one master ip
				 */
				String mstSvr = mstList.get(0);
				
				if(mstSvr!=null&&!"".equals(mstSvr)){
					String[] mstIpPort = mstSvr.split(":");
					String mstIp = mstIpPort[0];
					int mstPort = Integer.parseInt(mstIpPort[1]);
					
					
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxActive(redisMaxActive);
					config.setMaxIdle(redisMaxIdle);
					JedisPool pool = new JedisPool(config,mstIp,mstPort);
					
					ConnectionMap.put(shard, pool);
					
					/**
					 * test
					 */
					/*Jedis resource =  pool.getResource();
					resource.auth("hermes");
					resource.set("foo", "bar");
					System.out.println(resource.get("foo"));
					pool.returnResource(resource);*/
					
					poolEnable.put(shard, "Y");
					usableShardList.add(shard);
				}
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		usableShard = new String[usableShardList.size()];
		for(int inx=0;inx<usableShardList.size();inx++){
			usableShard[inx] = usableShardList.get(inx);
		}
		redisShardRule = usableShard;
		this.makeConsistentHashing();
	}
	

	
	
	/**
	 * jedis set mehtod
	 * @param key
	 * @param value
	 * @throws Exception 
	 */
	public void set(String key, String value) throws Exception{
		
		
		String shard = ch.get(key);
		int connectionTryNum=0;
		
		while(poolEnable.get(shard).equals("N")){			

			connectionTryNum++;
			if(connectionTryNum > 300){
				break;
			}
		}
		
		//커넥션 맺으려 할때 master가 disable 상태인지 판단 해야 함
		if(poolEnable.get(shard).equals("Y")){
			JedisPool pool = ConnectionMap.get(ch.get(key));
			Jedis resource=pool.getResource();
			resource.auth("hermes");
			resource.set(key, value);
			
			
			pool.returnResource(resource);
		}else{
			throw new Exception("Can not connect redis server. Please Contact Administrator");
			//System.out.println("Exception:::::커넥션을 맺을 수 없습니다. 관리자에게 연락하세요");
		}
		
		
	}

	/**
	 * jedis get method
	 * @param key
	 * @return
	 * @throws Exception 
	 */
	public String get(String key) throws Exception{
		
		String value = null;
		String shard = ch.get(key);
		int connectionTryNum=0;
		
		while(poolEnable.get(shard).equals("N")){			

			connectionTryNum++;
			if(connectionTryNum > 300){
				break;
			}
		}
		
		//커넥션 맺으려 할때 master가 disable 상태인지 판단 해야 함
		if(poolEnable.get(shard).equals("Y")){
			JedisPool pool = ConnectionMap.get(ch.get(key));
			Jedis resource=pool.getResource();
			resource.auth("hermes");
			value = resource.get(key);
			pool.returnResource(resource);
		}else{
			throw new Exception("Can not connect redis server. Please Contact Administrator");
		}
		
		return value;
	}
	
	
}
