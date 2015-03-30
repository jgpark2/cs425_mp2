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
	
	public void run() {
		//TODO: connect to other nodes (using methods in PeerToPeerLookupService)
		//TODO: acquire correct keys from other nodes (special case: node 0's first state)
		
	}

}
