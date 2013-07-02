package event;

import java.util.List;

import org.apache.zookeeper.KeeperException;

import redis.RedisConnection;
import connector.ZookeeperConnectListener;
import entity.ZooEntity;


public class RedisShardMasterListenerImpl implements ZNodeMonitorListener {
	
	private ZooEntity zet;
	private String masterNodePath;
	private String shard;
	public RedisShardMasterListenerImpl(ZooEntity zet,String shard) {
		this.zet = zet;
		this.shard = shard;
		this.masterNodePath = zet.getRedisShardsNodePath()+"/"+shard+zet.getRedisShardMasterPath();
	}
	
	@Override
	public void nodeDataChangeProcess(){

	}
	
	@Override
	public void nodeDeletedProcess(){
		
	}

	@Override
	public void nodeChildrenChangedProcess(){
		
		List<String> mstList = null;
		
		try {
			mstList = zet.getZk().getChildren(masterNodePath,false);
			if(mstList.size()<1){
				RedisConnection.getInstance().setRedisPoolDisable(shard);
			}else{
				RedisConnection.getInstance().refreshConnectionPool();
				RedisConnection.getInstance().setShardMasterNodeWathcer();
			}
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
