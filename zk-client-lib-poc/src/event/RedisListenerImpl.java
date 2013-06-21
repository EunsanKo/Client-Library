package event;

import org.apache.zookeeper.KeeperException;

import redis.RedisConnection;
import connector.ZookeeperConnectListener;
import entity.ZooEntity;


public class RedisListenerImpl implements ZNodeMonitorListener {
	
	private ZooEntity zet;
	
	public RedisListenerImpl(ZooEntity zet) {
		this.zet = zet;
	}
	
	@Override
	public void nodeDataChangeProcess(){
		byte[] nodeData = null;
		try {
			nodeData = zet.getZk().getData(zet.getRedisShardRuleNodePath(), false, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		zet.setRedisShardRule(new String(nodeData).split(","));
		
		System.out.println("why so serius :"+zet.getRedisShardRule()[0] );
		synchronized (ZookeeperConnectListener.zkConnMonitor) {
			ZookeeperConnectListener.zkConnMonitor.notifyAll();
		}
		
		RedisConnection.getInstance().refreshShardRule(zet);
	}
	
	@Override
	public void nodeDeletedProcess(){
		
	}

	@Override
	public void nodeChildrenChangedProcess(){
		
	}
	
}
