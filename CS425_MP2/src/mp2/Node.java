package mp2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
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
 * 		req: "req find_predecessor <reqcnt> <parameter_id> <sendId>"
 * 		ack: "ack find_predecessor <reqcnt> <parameter_id> <return_value> <sendId>"
 * set_predecessor: we don't need an ack for this
 * 		req: "req set_predecessor <reqcnt> <parameter_pred> <sendId>"
 * set_successor: we don't need an ack for this
 * 		req: "req set_successor <reqcnt> <parameter_pred> <sendId>"
 * update_finger_table: we don't need an ack for this
 * 		req: "req update_finger_table <parameter_s> <parameter_i> <sendId>"
 * move:
 * 		req: "req transfer_keys <reqcnt> <placeholder> <senderID>"
 * 		ack: "ack transfer_keys <reqcnt> <placeholder> <list of keys: comma split> <senderID>"
 * force_transfer: we don't need an ack for this
 * 		
 */
public class Node extends Thread {
	
	protected static final int m = 8;
	protected static final int bound = 256;
	
	private final boolean DEBUG = false;
	
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
	protected Finger [] finger_table;
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
		if (DEBUG)
			System.out.println("Initd");
		if (!initialnode) {
			updateOthers();
//			System.out.println("done with updateOthers");
			
			//move keys in (predecessor, this.id] from successor
			reqcnt++;
			String key_req = "transfer_keys " + reqcnt + " " + this.predecessor;
			AckTracker move_reply = new AckTracker(1);
			recvacks.put(key_req, move_reply); //wait for a single reply
			p2p.send("req " + key_req + " " + this.id, this.id,  getSuccessor());
			
//			if (DEBUG)
//				System.out.println("Going to wait on a find_successor reply: "+key_req);
			//wait on reply
			while (move_reply.toreceive > 0) {}
//			if (DEBUG)
//				System.out.println("Done waiting on a find_successor reply");
			
			String[] reply = move_reply.validacks.get(0).split("\\s+");
			
			ArrayList<Integer> key_str = new ArrayList<Integer>();
			
			//Add transferred keys to hashmap
			for (int i=4; i<reply.length-1; i++) {
				Integer key = new Integer(reply[i]);
				keys.put(key, true);
				key_str.add(key);
			}
			
			Collections.sort(key_str); //TODO: why is this here?
			if (DEBUG)
				System.out.println("DB: "+id+" Added Keys: "+key_str.toString());
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
			if (DEBUG)
				System.out.println("Initializing finger table for inital node "+id);
			//Logically, all connections are to ourself here
			for (int i=0; i<m; i++) {
				finger_table[i].node = 0;
			}
			predecessor = 0;
		}
		else {
			//DEFINING PREDECESSOR
			//Ask Introducer 0 to pass along the message to find my predecessor
			reqcnt++;
			String pred_req = "find_predecessor "+reqcnt+" " + this.id;
			AckTracker predecessor_reply = new AckTracker(1);
			recvacks.put(pred_req, predecessor_reply); //wait for a single reply
			p2p.send("req " + pred_req + " " + this.id, this.id, 0);
			
			//wait on reply
			while (predecessor_reply.toreceive > 0) {}
			
			String pred_reply_id = predecessor_reply.validacks.get(0);
			this.predecessor = Integer.parseInt(pred_reply_id);
			if (DEBUG)
				System.out.println("Node "+this.id+"'s predecessor just set as "+pred_reply_id);
			
			
			//DEFINING SUCCESSOR
			//Ask my predecessor about his successor
			reqcnt++;
			String req = "successor "+reqcnt+" " + "-";
			AckTracker successor_reply = new AckTracker(1);
			recvacks.put(req, successor_reply); //wait for a single reply
			p2p.send("req " + req + " " + id, id, predecessor);
			
			//wait on reply
			while (successor_reply.toreceive > 0) {}
			
			String reply_id = successor_reply.validacks.get(0);
			finger_table[0].node = Integer.parseInt(reply_id); //Set Successor
			if (DEBUG)
				System.out.println("Node "+this.id+"'s successor just set as "+finger_table[0].node);
			
			
			//UPDATING SUCCESOR AND PREDECESSOR
			//we don't need to wait for a return value (ack)
			String set_pred_req = "set_predecessor "+this.id+" "+this.id;
			p2p.send("req " + set_pred_req + " " + this.id, this.id, getSuccessor());			
			String set_succ_req = "set_successor "+this.id+" "+this.id;
			p2p.send("req " + set_succ_req + " " + this.id, this.id, predecessor);	
			
			for (int i=1; i<m; i++) {
				
				if (p2p.insideInterval(finger_table[i].start, this.id, finger_table[i-1].node)
						|| finger_table[i].start == this.id) {
					finger_table[i].node = finger_table[i-1].node;
				}
				
				else {
					//finger[i].node = 0.find_successor(finger[i].start);
					reqcnt++;
					String finger_suc_req = "find_successor "+reqcnt+" " + finger_table[i].start;
					AckTracker finger_suc_reply = new AckTracker(1);
					recvacks.put(finger_suc_req, finger_suc_reply); //wait for a single reply
					p2p.send("req " + finger_suc_req + " " + this.id, this.id, 0);

//					System.out.println("Node "+this.id+" going to wait on find_successor reply: "+finger_suc_req);
					//wait on reply
//					System.out.println("Athasth"+finger_suc_req);
					while (finger_suc_reply.toreceive > 0) {}
//					System.out.println("Node "+this.id+" done waiting on find_successor reply");
					
					String finger_suc_reply_id = finger_suc_reply.validacks.get(0);
					finger_table[i].node = Integer.parseInt(finger_suc_reply_id);
				}
			}
			
			if (DEBUG) {
				for(int i=1; i<=m; i++) {
					System.out.println("Node "+this.id+"'s finger_table["+i+"] set as "+finger_table[i-1].start+"->"+finger_table[i-1].node);
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
			int p = findFingerPredecessor(this.id - (int)Math.pow(2,i-1));

			if (!(p == this.id)) { //we shouldn't update our own joining finger table
				//don't need to wait for a reply
				String update_req = "update_finger_table "+this.id+" "+i;
//				System.out.println("Node "+this.id+" sending update_finger_table request to "+p+": "+"req " + update_req + " " + this.id);
				p2p.send("req " + update_req + " " + this.id, this.id, p); // i is 1-based
			}
			
//			System.out.println("Finished with i="+i+" round of for loop in updateOthers");
			
		}

	}
	
	
	/*
	 * If s is ith finger of this Node, update our finger table with s
	 * The parameter i is 1-indexed, and finger_table is 0-indexed
	 */
	void updateFingerTable(int s, int i) {
		
		if (s == this.id)
			return; //this node started this method chain in its join call, we should do nothing
		
		//if (s in [this.id, finger[i].node))
		if(p2p.insideInterval(s,this.id,finger_table[i-1].node) || s == this.id) {
			if (DEBUG)
				System.out.println("Node "+this.id+" updating finger["+i+"] to "+finger_table[i-1].start+"->"+s);
			finger_table[i-1].node = s;
			int p = predecessor;
			//p.update_finger_table(s, i);
			//don't need to wait for a reply
			String update_req = "update_finger_table "+s+" "+i;
			p2p.send("req " + update_req + " " + this.id, this.id, p);
		}
		
	}
	
	/*
	 * Routine to execute when this node is requested to leave
	 */
	public void onLeave() {
		
		//Link back my predecessor and successor to each other again
		//UPDATING SUCCESOR AND PREDECESSOR
		String set_pred_req = "set_predecessor "+"-"+" "+predecessor;
		p2p.send("req " + set_pred_req + " " + predecessor, this.id, getSuccessor());			
		String set_succ_req = "set_successor "+"-"+" "+getSuccessor();
		p2p.send("req " + set_succ_req + " " + getSuccessor(), this.id, predecessor);	
		
		//UPDATE OTHERS
		updateOthersOnLeave();
		
		//Transfer my keys to successor
		//move keys in (predecessor, this.id] to successor
		String returnValue = "";
		
		Set<Integer> keyset = keys.keySet();
		Iterator<Integer> it = keyset.iterator();
		while (it.hasNext()) {
			Integer key = it.next();
			if (!keys.get(key)) continue; //do not move key if it's not ours
			//if (p2p.insideHalfInclusiveInterval(key.intValue(), range_start, getSuccessor()-1)) {
				//take it out of our keys (doesn't really matter)
				keys.put(key, false);
				//add it to keysReturnValue
				returnValue = returnValue + key.toString() + " ";
			//}
		}
		
		if (returnValue.compareTo("") != 0)
			returnValue = returnValue.substring(0, returnValue.length()-1);
		
		String update_req = "force_transfer "+"-"+" "+this.id;
		if (DEBUG)
			System.out.println("DB: GIVE:"+returnValue);
		p2p.send("req " + update_req + " " + returnValue, this.id, getSuccessor());
		
		//kill this thread and close socket
		try {
			server.join();
			try {
				server.server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (DEBUG)
			System.out.println("socket closed");
	}
	
	
	/*
	 * Whoever had me as a finger table successor should set it to my successor since I'll be gone
	 */
	private void updateOthersOnLeave() {
		for (int i=1; i<=m; i++) {
			int lookUpNode = (this.id - (int)Math.pow(2,i-1) + (int)Math.pow(2,m)) % (int)Math.pow(2,m);
			
			int p = 0;
			
			if (p == this.id) { //call our method
				//System.out.println("wat");
				this.updateFingerTableOnLeave(getSuccessor(), i, this.id);
			}
			else { //don't need to wait for a reply
				if (DEBUG)
					System.out.println(p+"! PLEASE UPDATE "+i+" TO "+getSuccessor());
				String update_req = "leaving_update_finger_table "+getSuccessor()+" "+i;
				p2p.send("req " + update_req + " " + this.id, this.id, p); // i is 1-based
			}
			
		}		
	}


	/*
	 * Special case of finding the predecessor of a finger.start
	 * in updateOthers(); we want to include nprime in the interval
	 * ex. 128 - 2^7 = 0; this finger should update 0's 7th finger to 128!
	 * In other words, if we find a valid node that is equal to id, that is
	 * what we should return
	 */
	protected int findFingerPredecessor(int id) {
		
		int nprime = this.id;
		int nprimesuccessor = this.getSuccessor();
		
		//id != nprime is changed from findPredecessor code
		while (id != nprime && !p2p.insideHalfInclusiveInterval(id, nprime, nprimesuccessor)) {
			
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
		
		//This if statement is changed from findPredecessor code
		if (id == nprimesuccessor)
			return nprimesuccessor;

		return nprime;
	}
	
	
	/*
	 * This node has been asked to locate key
	 * Send query to largest successor/finger entry <= key
	 * (if none exist, send query to successor(this))
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void find(int key) {
		int nodeId = this.id;
		if (!keys.get(key)) { //go through algorithm to find key
			nodeId = findSuccessor(key);
		}
		System.out.println("Node "+nodeId+" has key "+key);
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


	/*
	 * Used in show commands
	 * format is node-identifier key1 key2 … key_last
	 */
	public void printKeys() {
		
		StringBuilder keys_str = new StringBuilder();
		keys_str.append(this.id); 
		
		ArrayList<Integer> keys_list = new ArrayList<Integer>();
		
		for(int i=0; i<bound; i++) {
			if(keys.get(i)) {
				keys_str.append(" " + i);
				keys_list.add(i);
			}
		}
		
		try {
			p2p.out.write(keys_str.toString()+"\n");
			p2p.out.flush();
		} catch (IOException e) {
			System.out.println("Could not print keys to out stream");
			e.printStackTrace();
		}
	}


	public void showFingerTable() {
		System.out.println("Node "+this.id+"'s finger table:");
		for (int i=0; i<m; i++) {
			System.out.println("Start: "+finger_table[i].start+", Node: "+finger_table[i].node);
		}
	}


	public void updateFingerTableOnLeave(int s, int i, int senderID) {
			//System.out.println(id+ " "+s+" "+i+" "+senderID+"..."+finger_table[i-1].node);
			if(finger_table[i-1].node == senderID || p2p.insideInterval(s,this.id,finger_table[i-1].node)){// || s == this.id) { //Even if u get ur message back, you need to relay it backawrds again!
					
				//System.out.println("taken");
				for(int j=0; j<m; ++j) {
					if(finger_table[j].node==senderID) {
						finger_table[j].node = s;
					}
				}
				int p = predecessor;
				
				if (DEBUG)
					System.out.println(id+":" + p+"! PLEASE UPDATE "+i+" TO "+s);
				
				String update_req = "leaving_update_finger_table "+s+" "+i;
				p2p.send("req " + update_req + " " + senderID, senderID, p);
			}
			else if (i>=0 && i<m && finger_table[i].node == senderID) {
				//...potential edge case??
				//System.out.println(id+"boohoo");
				finger_table[i].node = s;
			}
			/*if(finger_table[i-1].node == senderID) {
				if (DEBUG)
					System.out.println("Node "+this.id+" updating finger["+i+"] to "+finger_table[i-1].start+"->"+s);
				finger_table[i-1].node = s;
				int p = predecessor;
				
				String update_req = "update_finger_table "+s+" "+i;
				p2p.send("req " + update_req + " " + senderID, this.id, p);
			}
			else if(p2p.insideInterval(s,this.id,finger_table[i-1].node) || s == this.id) {
				if (DEBUG)
					System.out.println("Node "+this.id+" updating finger["+i+"] to "+finger_table[i-1].start+"->"+s);
				finger_table[i-1].node = s;
				int p = predecessor;
				
				String update_req = "update_finger_table "+s+" "+i;
				p2p.send("req " + update_req + " " + senderID, this.id, p);
			}*/
			
		
	}
	
}
