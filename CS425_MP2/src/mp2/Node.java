package mp2;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
	
	//The socket that the Node listens on
	private Server server;
	
	//successor is implicitly the first entry in the finger table
	private NodeConnection [] finger_table;
	
	private NodeConnection predecessor;
	
	
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
		
		new Thread(this, "Nodeprime"+id).start();
	}

	protected int getNodeId() {
		return id;
	}
	

	public void run() {
		
		initializeSockets();
		
		//TODO: connect to other nodes (using methods in PeerToPeerLookupService)
		//TODO: acquire correct keys from other nodes (special case: node 0's first state)
		
	}
	

	private void initializeSockets() {
		
		server = new Server(this);		
		finger_table = new NodeConnection[8];
		
		if (initialnode) { //this is the first node in the network
			//Logically, all connections are to ourself here
			for (int i=0; i<8; i++) {
				finger_table[i] = null;
			}
			predecessor = null;
		}
		else {
			
			//At this point, we only know node 0 exists; make a socket connection with 0
			Socket nprime = null;
			try {
				nprime = new Socket("127.0.0.1", 7500);
				NodeConnection nprimep = new NodeConnection(this, nprime, 0);
			} catch (Exception e) {
				System.out.println("Failed to connect to n\'");
				e.printStackTrace();
				return;
			}

			
			//init_finger_table:
				//finger[1].node = 0.find_successor(finger[1].start);
				//predecessor = successor.predecessor = this;
				//successor.predecessor = this;
				//for i=1 to m-1 (7)
					//if (finger[i+1].start >= this.id && finger[i+1].start < finger[i].node.id)
						//finger[i+1].node = finger[i].node;
					//else
						//finger[i+1].node = 0.find_successor(finger[i+1].start);
			
			//update_others:
				//for i=1 to m (8)
					//p = find_predecessor(this.id - 2^(i-1));
					//p.update_finger_table(this,i);
		}
		
	}
	
	
	/*
	 * Receive and process a message from node with parameter recvId
	 * Keep a reference to the NodeConnection it came from in case
	 * a reply is required
	 */
	protected void receive(NodeConnection recvNode, int recvId, String msg) {
		
	}

}
