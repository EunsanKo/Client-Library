package connector;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import mongodb.MongoDbConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import redis.RedisConnection;
import watcher.ZNodeMonitor;
import entity.ZooEntity;
import event.RedisListenerImpl;

public class ZookeeperConnectListener implements ServletContextListener {
	
	private static final Log LOG = LogFactory.getLog(ZookeeperConnectListener.class);
	
	private static Map<String, ZooKeeper> zookeepers = new HashMap<String, ZooKeeper>();
	public static Object zkConnMonitor = new Object();
	private static RedisConnection conn;
	private static MongoDbConnection mconn;
	
	/**
	 * zookeeper server info
	 */
	private static String zkips = "";
	
	/**
	 * use db type
	 */
	private static String redisUse = "N";
	private static String mongodbUse = "N";
	private static String mongosStartCmd = "";
	private static String mongosServer = "127.0.0.1:27017";
	
	private static int redisMaxActive;
	private static int redisMaxIdle;
	
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		
		zkips = arg0.getServletContext().getInitParameter("zookeeperCluster");
		redisUse = arg0.getServletContext().getInitParameter("redisUse");
		mongodbUse = arg0.getServletContext().getInitParameter("mongodbUse");
		mongosStartCmd = arg0.getServletContext().getInitParameter("mongosStartCmd");
		mongosServer = arg0.getServletContext().getInitParameter("mongosServer");
		redisMaxActive = Integer.parseInt(arg0.getServletContext().getInitParameter("redisMaxActive"));
		redisMaxIdle = Integer.parseInt(arg0.getServletContext().getInitParameter("redisMaxIdle"));
		
		
		ZooKeeper zk;
		try {
			zk = getZooKeeper(zkips);
			
			ZooEntity zet = new ZooEntity();
			zet.setZk(zk);
			zet.setServerIp(zkips);
			zet.setRedisMaxActive(redisMaxActive);
			zet.setRedisMaxIdle(redisMaxIdle);
			zet.setMongosServer(mongosServer);
			
			
			if(redisUse.equals("Y")){
				conn = RedisConnection.getInstance();
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
			
			if(mongodbUse.equals("Y")){
				zet.setMongoConfig(zk.getChildren(zet.getMongodbConfigsPath(), false));
				
				List mconfigs = zet.getMongoConfig();
				
				StringBuffer configs = new StringBuffer(" ");
				for(int inx=0;inx<mconfigs.size();inx++){
					configs.append(mconfigs.get(inx));
					if(inx<mconfigs.size()-1){
						configs.append(",");
					}
				}
				mongosStartCmd += configs.toString();
				//D:\mongodb-win32-i386-2.4.4\bin\mongos.exe --port 27017 --logpath D:\mongodb-win32-i386-2.4.4\logs\mongo_mongos.log --configdb 172.22.9.210:30000
				try {
				    String ls_str;

				    Process ls_proc = Runtime.getRuntime().exec(mongosStartCmd);

				    // get its output (your input) stream

				    DataInputStream ls_in = new DataInputStream(
			                                          ls_proc.getInputStream());

				    try {
					while ((ls_str = ls_in.readLine()) != null) {
					    System.out.println(ls_str);
					}
				    } catch (IOException e) {
					System.exit(0);
				    }
				} catch (IOException e1) {
				    System.err.println(e1);
				    System.exit(1);
				}
				 
				mconn = MongoDbConnection.getInstance();
				
				mconn.refreshMongosServer(zet);
			}


			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
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
	 * Children Node List Á¶È¸
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
