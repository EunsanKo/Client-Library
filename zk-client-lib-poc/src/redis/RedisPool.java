package redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPool {

	private JedisPool jedisPool;
	
	
	
	public RedisPool(JedisPool jedisPool){
		this.jedisPool = jedisPool;
	}
	
	
	public Jedis getConn(){
		return jedisPool.getResource();
	}

	public void returnConn(Jedis conn){
		this.jedisPool.returnResource(conn);
	}
}
