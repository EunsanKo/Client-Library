package connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import redis.RedisConnection;
import redis.RedisPool;
import redis.clients.jedis.Jedis;
import watcher.ZNodeMonitor;
import entity.ZooEntity;
import event.RedisListenerImpl;

public class ZookeeperConnectListener implements ServletContextListener {
	
	private static final Log LOG = LogFactory.getLog(ZookeeperConnectListener.class);
	
	private static Map<String, ZooKeeper> zookeepers = new HashMap<String, ZooKeeper>();
	public static Object zkConnMonitor = new Object();
	private static RedisConnection conn;
	
	/**
	 * zookeeper server info
	 */
	private static String zkips = "";
	
	/**
	 * use db type
	 */
	private static boolean redisUse = false;
	private static boolean mongodbUse = false;
	
	private static int redisMaxActive;
	private static int redisMaxIdle;
	
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		zkips = "172.22.9.130:2184,172.22.9.130:2185,172.22.9.130:2186";
		
		redisUse = true;
		mongodbUse = false;
		
		redisMaxActive = 10;
		redisMaxIdle = 10;
		
		ZooKeeper zk = getZooKeeper(zkips);
		/*List<String> children = new ArrayList<String>();
		children = zk.getChildren(REDIS_Z_NODE, false);*/
		
		
		ZooEntity zet = new ZooEntity();
		zet.setZk(zk);
		zet.setServerIp(zkips);
		zet.setRedisMaxActive(redisMaxActive);
		zet.setRedisMaxIdle(redisMaxIdle);
		
		conn = RedisConnection.getInstance();
		
		if(redisUse){
			byte[] nodeData = zk.getData(zet.getRedisShardRuleNodePath(), false, null);
			zet.setRedisShardRule(new String(nodeData).split(","));
			
			conn.refreshShardRule(zet);
			
			/**
			 * shard-rule watcher
			 * check shard-rule and then get changed data
			 */
			ZNodeMonitor redisShardRuleWatcher = new ZNodeMonitor(zet.getZk(), zet.getRedisShardRuleNodePath());
			
			
			RedisListenerImpl redisListener = new RedisListenerImpl(zet);
			redisShardRuleWatcher.setListener(redisListener);
			
			zk.exists(zet.getRedisShardRuleNodePath(), redisShardRuleWatcher);
			
			
		}
		
		if(mongodbUse){
			zet.setMongoConfig(zk.getChildren(zet.getMongodbConfigsPath(), false));
		}


		for(int inx=0;inx<zet.getRedisShardRule().length;inx++){
			System.out.println(zet.getRedisShardRule()[inx]);
		}
		System.out.println(zet.getMongoConfig());
		
		
		
		conn.set("foo","bar");

		String value = conn.get("foo");
		System.out.println(value);
		
		
		/*synchronized (zkConnMonitor) {
			zkConnMonitor.wait();
		}*/
		
		
		/*//event type 에 맞는 watcher 설정
		if(event.equals("NodeChildrenChanged")){
			//children watcher
			zk.getChildren(nodePath, watcher);
		}else{
			//data watcher
			zk.exists(nodePath, watcher);
		}
		System.out.println(children);*/
	}

	public static ZooKeeper getZooKeeper(final String zkservers) throws Exception {
		synchronized (zookeepers) {
			if (zookeepers.containsKey(zkservers)) {
				return zookeepers.get(zkservers);
			}
			
			ZookeeperConnectListener zc = new ZookeeperConnectListener();
			
			Watcher watcher = zc.new ZKMonitorWatcher(zkservers);

			final ZooKeeper zk = new ZooKeeper(zkservers, 30 * 1000, watcher);
			synchronized (zkConnMonitor) {
				zkConnMonitor.wait();
			}
			zookeepers.put(zkservers, zk);
			System.out.println("Create new ZK conneciton");
			return zk;
		}
	}
	
	class ZKMonitorWatcher implements Watcher {
		private String zkservers;

		public ZKMonitorWatcher(String zkservers) {
			this.zkservers = zkservers;
		}

		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();

			if (event.getType() == Event.EventType.None) {
				switch (event.getState()) {
				case SyncConnected:
					LOG.info("ZK Connected:" + zkservers);
					synchronized (zkConnMonitor) {
						zkConnMonitor.notifyAll();
					}
					break;
				case Disconnected:
					LOG.info("ZK Disconnected:" + zkservers);
					break;
				case Expired:
					LOG.info("ZK Expired:" + zkservers);
					synchronized (zookeepers) {
						zookeepers.remove(zkservers);
					}
					break;
				}
			}else{
				if(event.getType() == Event.EventType.NodeCreated){
					System.out.println("NodeCreated : " + path);
				} else if(event.getType() == Event.EventType.NodeDeleted){
					System.out.println("NodeDeleted : " + path);
				} else if(event.getType() == Event.EventType.NodeDataChanged){
					System.out.println("NodeDataChanged : " + path);
				} else if(event.getType() == Event.EventType.NodeChildrenChanged){
					System.out.println("NodeChildrenChanged : " + path);

					try {
						//children watcher re work
						zookeepers.get(zkservers).getChildren(path, true);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
				
			} 
		}
	}
		
	
	/**
	 * Children Node List 조회
	 * 
	 * @param nodePath
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public List<String> getChildrenList(String ip, String nodePath) throws Exception,
			InterruptedException {

		List<String> children = new ArrayList<String>();
		try {
			ZooKeeper zk = getZooKeeper(ip);
			
			children = zk.getChildren(nodePath, false);
			if (children.isEmpty()) {

			}

		} catch (KeeperException.NoNodeException e) {
			System.out.printf("node %s does not exist\n", nodePath);
		}

		return children;
	}

	
	
}
