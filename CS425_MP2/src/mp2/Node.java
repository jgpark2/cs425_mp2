package mp2;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Represents a node in the Chord system
 */
public class Node extends Thread {
	
	//Used in full-capacity ArrayList in PeerToPeerLookupService
	private int id;
	
	private final int BOUND = 256;
	private final int TABLE_SIZE = 8; //lg(256)
	
	//The Chord system that this Node belongs to
	protected PeerToPeerLookupService p2p;
	
	//Each key is stored in a Node's ConcurrentHashMap, because threads
	//    inside the Node may try to simultaneously access it
	//The <Integer,Boolean> pairs represent a key and whether this Node has it
	//To increase efficiency just a little, when a key is moved from one Node
	//    to another, it can just be marked false in the first Node's hashmap
	ConcurrentHashMap<Integer,Boolean> keys;
	
	ArrayList<TableEntry> finger_table = new ArrayList<TableEntry>(TABLE_SIZE);
	public int predecessor = -1;
	
	
	protected Node (int id, PeerToPeerLookupService p2p) {
		this.id = id;
		this.p2p = p2p;
		
		//ID 0 is initialized with the system and never "joins", so we can initialize all its keys to true
		for(int i=0; i<BOUND; ++i) {
			keys.put(i, (id==0));
		}
		
		//TODO: declare other class member objects
		
		new Thread(this, "Node"+id).start();
			
	}
	
	public void run() {
		//Join Algorithm
		this.onJoin(0);
		
		//TODO: connect to other nodes (using methods in PeerToPeerLookupService)
		//TODO: acquire correct keys from other nodes (special case: node 0's first state)
		
	}

	private void onJoin(int introducer) {
		if(id == introducer) {
			for(int i=0; i<TABLE_SIZE; ++i)
				finger_table.add(new TableEntry(0));
			predecessor = 0;
		}
		else {
			init_finger_table(0);
			update_others();
			//Move Keys...
		}
		
		int predecessor = find_predecessor(id);
	}
	
	private void init_finger_table() {
		
	}
	
	private void update_others() {
		for(int i=0; i<BOUND; ++i) {
			//find last node p whose ith finger might be n
			int p_id = find_predecessor(id-2^(i-1));
			p_id. update_finger_table(id,i); //Call to socket
		}
		
	}
	
	//if s is ith finger of n, update n's finger table with s
	private void update_finger_table(int s, int i) {
		if(s in [id, finger_table.get(i).id]) {
			finger_table.get(i).id = s;
			int p = predecessor;
			p .update_finger_table(s, i); //Call to socket
		}
	
	}
	
	private int find_successor(int id) {
		
	}
	
	private int find_predecessor(int id) {
		
	}
	
}
