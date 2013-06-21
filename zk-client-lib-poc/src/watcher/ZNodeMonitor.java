package watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import connector.ZookeeperConnectListener;
import event.ZNodeMonitorListener;

public class ZNodeMonitor implements Watcher{

	private ZooKeeper zk;
	private String nodePath;
	private ZNodeMonitorListener listener;
	
	public ZNodeMonitor(ZooKeeper zk, String nodePath) {
		this.zk = zk;
		this.nodePath = nodePath;
		
	}
	
	public void setListener(ZNodeMonitorListener listener){
		this.listener = listener;
		
	}
	
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
	
		if (event.getType() != Event.EventType.None) {				
			if(path !=null && path.equals(nodePath)){
				if(event.getType() == Event.EventType.NodeDeleted){
					//do-something
					listener.nodeDeletedProcess();
					try {
						zk.exists(nodePath, this);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				} else if(event.getType() == Event.EventType.NodeDataChanged){
					//do-something
					
					listener.nodeDataChangeProcess();
					
					try {
						zk.exists(nodePath, this);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					
				} else if(event.getType() == Event.EventType.NodeChildrenChanged){
					//do-something
					listener.nodeChildrenChangedProcess();
					
					try {
						zk.exists(nodePath, this);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
				
			} 
				
		}
	}
}
