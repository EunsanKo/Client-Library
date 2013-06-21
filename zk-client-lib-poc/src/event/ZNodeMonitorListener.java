package event;


public interface ZNodeMonitorListener {

	public void nodeDataChangeProcess();
	public void nodeDeletedProcess();
	public void nodeChildrenChangedProcess();
}
