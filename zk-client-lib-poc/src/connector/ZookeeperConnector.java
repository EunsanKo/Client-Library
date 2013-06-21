package connector;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;

public class ZookeeperConnector{
	
	
	public String getNodeData(ZooKeeper zk, String nodePath) throws Exception {
		return new String(zk.getData(nodePath, false, null));
	}
	
	public List<String> getChildren(ZooKeeper zk, String nodePath) throws Exception {
		return zk.getChildren(nodePath, false);
	}
}
