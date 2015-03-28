package mp2;

/*
 * Thread to read commands from command line and perform necessary
 * actions in response
 */
public class Coordinator extends Thread {
	
	//The Chord system that this Coordinator belongs to
	protected PeerToPeerLookupService p2p;

	//Indicates whether the current command has finished executing
	protected volatile boolean cmdComplete = false;
	
	
	protected Coordinator (PeerToPeerLookupService p2p) {
		this.p2p = p2p;
		
		new Thread(this, "CommandInput").start();
	}
	
	
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
}
