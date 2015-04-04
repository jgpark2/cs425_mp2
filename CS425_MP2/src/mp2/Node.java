package mp2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Represents a node in the Chord system
 * 
 * MESSAGE FORMATS:
 * find_successor:
 * 		req: "req find_successor <reqcnt> <parameter_id> <sendId>"
 * 		ack: "ack find_successor <reqcnt> <parameter_id> <return_value> <sendId>"
 * successor:
 * 		req: "req successor <reqcnt> <placeholder> <sendId>"
 * 		ack: "ack successor <reqcnt> <placeholder> <return_value> <sendId>"
 * closest_preceding_finger:
 * 		req: "req closest_preceding_finger <reqcnt> <parameter_id> <sendId>"
 * 		ack: "ack closest_preceding_finger <reqcnt> <parameter_id> <return_value> <sendId>"
 * predecessor:
 * 		req: "req predecessor <reqcnt> <placeholder> <sendId>"
 * 		ack: "ack predecessor <reqcnt> <placeholder> <return_value> <sendId>"
 * set_predecessor: we don't need an ack for this
 * 		req: "req set_predecessor <reqcnt> <parameter_pred> <sendId>"
 * update_finger_table: we don't need an ack for this
 * 		req: "req update_finger_table <parameter_s> <parameter_i> <sendId>"
 * move:
 * 		req: "req transfer_keys <reqcnt> <placeholder> <senderID>"
 * 		ack: "ack transfer_keys <reqcnt> <placeholder> <list of keys: comma split> <senderID>"
 */
public class Node extends Thread {
	
	protected final int m = 8;
	protected final int bound = 256;
	
	private final boolean DEBUG = true;
	
	//Used in full-capacity ArrayList in PeerToPeerLookupService
	private int id;

	private boolean initialnode; //indicates first node in system
	
	//The Chord system that this Node belongs to
	protected PeerToPeerLookupService p2p;
	
	//Each key is stored in a Node's ConcurrentHashMap, because threads
	//    inside the Node may try to simultaneously access it
	//The <Integer,Boolean> pairs represent a key and whether this Node has it
	//To increase efficiency just a little, when a key is moved from one Node
	//    to another, it can just be marked false in the first Node's hashmap
	ConcurrentHashMap<Integer,Boolean> keys;
	
	//successor is implicitly the first entry in the finger table
	private Finger [] finger_table;
	protected int predecessor;
	
	//The socket that the Node listens on
	private Server server; 
	
	//Map that tracks how many acks we have received/are yet to receive for a message
	protected ConcurrentHashMap<String, AckTracker> recvacks;
	
	//Number of operation requests made by this Node
	protected volatile int reqcnt = 0;
	
	
	protected Node (int id, PeerToPeerLookupService p2p) {
		this.id = id;
		this.initialnode = (id==0);
		this.p2p = p2p;		
		
		keys = new ConcurrentHashMap<Integer,Boolean>();
		recvacks = new ConcurrentHashMap<String, AckTracker>();
		
		//ID 0 is initialized with the system and never "joins", so we can initialize all its keys to true
		for(int i=0; i<bound; ++i) {
			keys.put(i, (id==0));
		}
		
		new Thread(this, "Node"+id).start();
	}


	protected int getNodeId() {
		return this.id;
	}
	
	protected int getSuccessor() {
		return finger_table[0].node;
	}
	
	protected int getPredecessor() {
		return predecessor;
	}
	

	/*
	 * The main purpose of this thread is to represent a node in the
	 * Chord system, executing join, leave, find, and show commands
	 */
	public void run() {
		server = new Server(this);
		//Join Algorithm
		this.onJoin();
	}
	
	
	/*
	 * Join algorithm for Chord system
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	private void onJoin() {
		initializeFingerTable();
		
		if (!initialnode) {
			updateOthers();
			
			//move keys in (predecessor, this.id] from successor
			reqcnt++;
			String key_req = "transfer_keys " + reqcnt + " " + this.predecessor;
			AckTracker move_reply = new AckTracker(1);
			recvacks.put(key_req, move_reply); //wait for a single reply
			p2p.send("req " + key_req + " " + this.id, this.id, finger_table[0].node);
			
			//wait on reply
			while (move_reply.toreceive > 0) {}
			
			//System.out.println("HI:"+);
			List<String> receivedKeys = Arrays.asList((move_reply.validacks.get(0).split(" ")[4]).split(","));
			for(int i=0; i<receivedKeys.size(); ++i)
				keys.put(Integer.parseInt(receivedKeys.get(i)), true);
			
			if (DEBUG)
				System.out.println("DB: "+id+" Added Keys: "+receivedKeys.toString());
			
		}
		
		//join algorithm finished, mark cmdComplete in Coordinator
		p2p.coord.cmdComplete = true;
	}
	

	/*
	 * Initializes the finger table structure and calculates the start
	 * of each finger's interval
	 * Send requests to fill in correct finger_table.node fields
	 */
	private void initializeFingerTable() {
		
		finger_table = new Finger[m];
		for (int i=0; i<m; i++) {
			finger_table[i] = new Finger();
			finger_table[i].start = Finger.calculateStart(this.id, i, m);
		}
		
		if (initialnode) { //this is the first node in the network
			//Logically, all connections are to ourself here
			for (int i=0; i<m; i++) {
				finger_table[i].node = 0;
			}
			predecessor = 0;
		}
		else {
			
			//At this point, we only know node 0 exists
			
			//finger[1].node = 0.find_successor(finger[1].start);
			reqcnt++;
			String req = "find_successor "+reqcnt+" " + finger_table[0].start;
			AckTracker find_successor_reply = new AckTracker(1);
			recvacks.put(req, find_successor_reply); //wait for a single reply
			p2p.send("req " + req + " " + this.id, this.id, 0);
			
			//wait on reply
			while (find_successor_reply.toreceive > 0) {}
			
			String reply_id = find_successor_reply.validacks.get(0);
			finger_table[0].node = Integer.parseInt(reply_id);
			
			//predecessor = successor.predecessor;
			reqcnt++;
			String pred_req = "predecessor "+reqcnt+" " + this.id;
			AckTracker predecessor_reply = new AckTracker(1);
			recvacks.put(pred_req, predecessor_reply); //wait for a single reply
			p2p.send("req " + pred_req + " " + this.id, this.id, finger_table[0].node);
			
			//wait on reply
			while (predecessor_reply.toreceive > 0) {}
			
			String pred_reply_id = predecessor_reply.validacks.get(0);
			this.predecessor = Integer.parseInt(pred_reply_id);
			
			//successor.predecessor = this; //we don't need to wait for a return value (ack)
			String set_pred_req = "set_predecessor "+this.id+" "+this.id;
			p2p.send("req " + set_pred_req + " " + this.id, this.id, finger_table[0].node);
			
			
			for (int i=0; i<m-1; i++) {
				
				if (p2p.insideInterval(finger_table[i+1].start, this.id, finger_table[i].node)
						|| finger_table[i+1].start == this.id) {
					finger_table[i+1].node = finger_table[i].node;
				}
				
				else {
					//finger[i+1].node = 0.find_successor(finger[i+1].start);
					reqcnt++;
					String finger_suc_req = "find_successor "+reqcnt+" " + finger_table[i+1].start;
					AckTracker finger_suc_reply = new AckTracker(1);
					recvacks.put(finger_suc_req, finger_suc_reply); //wait for a single reply
					p2p.send("req " + finger_suc_req + " " + this.id, this.id, 0);
					
					//wait on reply
					while (finger_suc_reply.toreceive > 0) {}
					
					String finger_suc_reply_id = finger_suc_reply.validacks.get(0);
					finger_table[i+1].node = Integer.parseInt(finger_suc_reply_id);
				}
				
			}

		}
		
	}
	
	
	/*
	 * Update all nodes whose finger tables should refer to this Node
	 */
	private void updateOthers() {
		
		//for i=1 to m (8)
		for (int i=1; i<=m; i++) {
			
			//find last node p whose ith finger might be n
			//p = find_predecessor(n-2^(i-1));
			int p = findPredecessor(this.id - (int)Math.pow(2,i-1));
			
			//p_id.updateFingerTable(id,i);
			if (p == this.id) { //call our method
				this.updateFingerTable(this.id, i);
			}
			else { //don't need to wait for a reply
				String update_req = "update_finger_table "+this.id+" "+i;
				p2p.send("req " + update_req + " " + this.id, this.id, p);
			}
			
		}
		
	}
	
	
	/*
	 * If s is ith finger of this Node, update our finger table with s
	 * The parameter i is 1-indexed, and finger_table is 0-indexed
	 */
	private void updateFingerTable(int s, int i) {
		
		//if (s in [this.id, finger[i].node))
		if(p2p.insideInterval(s,this.id,finger_table[i-1].node) || s == this.id) {
			finger_table[i-1].node = s;
			int p = predecessor;
			//p.update_finger_table(s, i);
			//don't need to wait for a reply
			String update_req = "update_finger_table "+s+" "+i;
			p2p.send("req " + update_req + " " + this.id, this.id, p);
		}
		
	}
	
	
	/*
	 * This node has been asked to locate key
	 * Send query to largest successor/finger entry <= key
	 * (if none exist, send query to successor(this))
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void find(int key) {
		System.out.println("Node "+findSuccessor(key)+" has key "+key);
		p2p.coord.cmdComplete = true;
	}
	
	
	/*
	 * Ask this Node (node n) to find id's successor
	 */
	protected int findSuccessor(int id) {
		
		int nprime = findPredecessor(id);
		
		if (nprime == this.id) {
			return this.getSuccessor();
		}
		
		else { //ask nprime for its successor
			
			reqcnt++;
			String successorreq = "successor " +reqcnt+" " + nprime;
			AckTracker successor_reply = new AckTracker(1);
			recvacks.put(successorreq, successor_reply); //wait for a single reply
			p2p.send("req " + successorreq + " " + this.id, this.id, nprime);
			
			//wait on reply
			while (successor_reply.toreceive > 0) {}
			
			String reply_id = successor_reply.validacks.get(0);
			return Integer.parseInt(reply_id);
			
		}

	}
	
	
	/*
	 * Ask this Node (node n) to find id's predecessor
	 */
	private int findPredecessor(int id) {

		int nprime = this.id;
		int nprimesuccessor = this.getSuccessor();
		
		while (!p2p.insideHalfInclusiveInterval(id, nprime, nprimesuccessor)) {
			
			if (nprime == this.id) { //call our method
				nprime = this.closestPrecedingFinger(id);
				
				//set nprimesuccessor
				if (nprime == this.id) {
					nprimesuccessor = this.getSuccessor();
				}
				else { //ask nprime for its successor
					
					reqcnt++;
					String successorreq = "successor " +reqcnt+" " + nprime;
					AckTracker successor_reply = new AckTracker(1);
					recvacks.put(successorreq, successor_reply); //wait for a single reply
					p2p.send("req " + successorreq + " " + this.id, this.id, nprime);
					
					//wait on reply
					while (successor_reply.toreceive > 0) {}
					
					String reply_id = successor_reply.validacks.get(0);
					nprimesuccessor = Integer.parseInt(reply_id);
					
				}
			}
			
			else { //send a message to nprime
				
				reqcnt++;
				String req = "closest_preceding_finger " +reqcnt+" " + id;
				AckTracker closest_preceding_finger_reply = new AckTracker(1);
				recvacks.put(req, closest_preceding_finger_reply); //wait for a single reply
				p2p.send("req " + req + " " + this.id, this.id, nprime);
				
				//wait on reply
				while (closest_preceding_finger_reply.toreceive > 0) {}
				
				String reply_finger = closest_preceding_finger_reply.validacks.get(0);
				nprime = Integer.parseInt(reply_finger);
				
				//set nprimesuccessor
				if (nprime == this.id) {
					nprimesuccessor = this.getSuccessor();
				}
				else { //ask nprime for its successor
					
					reqcnt++;
					String successorreq = "successor " +reqcnt+" " + nprime;
					AckTracker successor_reply = new AckTracker(1);
					recvacks.put(successorreq, successor_reply); //wait for a single reply
					p2p.send("req " + successorreq + " " + this.id, this.id, nprime);
					
					//wait on reply
					while (successor_reply.toreceive > 0) {}
					
					String reply_id = successor_reply.validacks.get(0);
					nprimesuccessor = Integer.parseInt(reply_id);
					
				}
				
			}

		}

		return nprime;
	}
	
	
	/*
	 * Return closest finger preceding id
	 */
	protected int closestPrecedingFinger(int id) {

		for (int i=m-1; i>=0; i--) {
			if (p2p.insideInterval(finger_table[i].node, this.id, id))
				return finger_table[i].node;
		}
		
		return this.id;
	}

	public String moveKeysTo(int sendId) {
		
		String ret = "";
		
		//iteration can start at index = id since node 0 never goes away
		for(int i=id; i<bound; ++i) {
			if(keys.get(i) && i>id && i<=sendId) {
				//Remove keys by setting flag to false
				keys.put(i,false);
				ret += String.valueOf(i) + ",";
			}	
		}
		
		//Remove extra comma at the end
		if (ret.charAt(ret.length()-1)==',')
			ret.substring(0, ret.length()-1);
		
		if (DEBUG)
			System.out.println("DB: "+id+" Removed Keys: "+ret);
		
		return ret;
	}
	
}
