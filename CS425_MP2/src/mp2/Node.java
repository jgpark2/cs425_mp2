package mp2;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Represents a node in the Chord system
 */
public class Node extends Thread {
	
	//Indicates whether this is an active node
	//Used in full-capacity ArrayList in PeerToPeerLookupService
	protected boolean valid;
	private int id;
	
	private boolean initialnode = false; //indicates first node in system
	
	//The Chord system that this Node belongs to
	protected PeerToPeerLookupService p2p;
	
	//Each key is stored in a Node's ConcurrentHashMap, because threads
	//    inside the Node may try to simultaneously access it
	//The <Integer,Boolean> pairs represent a key and whether this Node has it
	//To increase efficiency just a little, when a key is moved from one Node
	//    to another, it can just be marked false in the first Node's hashmap
	ConcurrentHashMap<Integer,Boolean> keys;
	
	
	protected Node () {
		this.valid = false;
		this.id = -1;
	}
	
	protected Node (int id, PeerToPeerLookupService p2p) {
		this.valid = true;
		this.id = id;
		this.p2p = p2p;
		
		//TODO: declare other class member objects
		
		new Thread(this, "Node"+id).start();
	}
	
	/*
	 * Special constructor for the first node in the Chord system
	 */
	protected Node(int id, PeerToPeerLookupService p2p, String string) {
		this.valid = true;
		this.id = id;
		this.p2p = p2p;
		this.initialnode = true; //check this in run()
		
		keys = new ConcurrentHashMap<Integer,Boolean>();
		
		//Fill keys because this is first node to join system
		for (int i=0; i<256; i++) {
			Integer key = new Integer(i);
			keys.put(key, true);
		}
		
		//TODO: initialize finger table
		
		new Thread(this, "Nodeprime"+id).start();
	}

	protected int getNodeId() {
		return id;
	}
	

	public void run() {
		//TODO: connect to other nodes (using methods in PeerToPeerLookupService)
		//TODO: acquire correct keys from other nodes (special case: node 0's first state)
		
	}

}
