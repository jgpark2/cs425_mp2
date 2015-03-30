package mp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
 * Thread to read commands from command line and perform necessary
 * actions in response
 */
public class Coordinator extends Thread {
	
	//The Chord system that this Coordinator belongs to
	protected PeerToPeerLookupService p2p;

	//Indicates whether the current command has finished executing
	protected volatile boolean cmdComplete = false;
	
	//BufferedReader coming from command line
	private BufferedReader cmdin;
	
	
	protected Coordinator (PeerToPeerLookupService p2p) {
		this.p2p = p2p;
		cmdin = new BufferedReader(new InputStreamReader(System.in));
		
		new Thread(this, "CommandInput").start();
	}
	

	/*
	 * The main purpose of this thread is to read commands from the
	 * command line and do the administrative work for them
	 */
	public void run() {

		String cmd = "";
		
		try {
			
			while ((cmd = cmdin.readLine()) != null) {
				
				cmdComplete = false;
				
				if (cmd.lastIndexOf("join ") == 0) { //join p
					try {
						int p = Integer.parseInt(cmd.substring(5));
						join(p);
					} catch (NumberFormatException e1) {
						System.out.println("Command was not correctly fomatted; try again");
						continue;
					}
				}
				
				else if (cmd.lastIndexOf("find ") == 0) { //find p k
					try {
						String[] elems = cmd.split("\\s+");
						int p = Integer.parseInt(elems[1]);
						int k = Integer.parseInt(elems[2]);
						find(p,k);
					} catch (Exception e1) {
						System.out.println("Command was not correctly fomatted; try again");
						continue;
					}
				}
				
				else if (cmd.lastIndexOf("leave ") == 0) { //leave p
					try {
						int p = Integer.parseInt(cmd.substring(6));
						leave(p);
					} catch (NumberFormatException e1) {
						System.out.println("Command was not correctly fomatted; try again");
						continue;
					}
				}
				
				else if (cmd.lastIndexOf("show all") == 0) { //show all
					showall();
				}
				
				else if (cmd.lastIndexOf("show ") == 0) { //show p
					try {
						int p = Integer.parseInt(cmd.substring(5));
						show(p);
					} catch (NumberFormatException e1) {
						System.out.println("Command was not correctly fomatted; try again");
						continue;
					}
				}
				
				else {
					System.out.println("Command was not correctly fomatted; try again");
					continue;
				}
				
				//ensure that two commands will not be processed simultaneously
				while (!cmdComplete) {}
				
			}
			
		} catch (IOException e) {
			System.out.println("Failed to get command");
			e.printStackTrace();
			try {
				cmdin.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			return;
		}
		
	}
	
	
	/*
	 * Coordinator has been asked to join node with parameter id; the coordinator
	 * creates a new Node thread, which then takes steps to add itself
	 * TODO:
	 * Once the proper join procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void join(int id) {
		//Error checking
		if (id < 0 || id > 255) {
			System.out.println("Id is assumed to be in the range 0 to 255; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: according to Piazza post @345, this may have to involve messages
		//Check to see that node id doesn't already exist in system
		boolean exists = false;
		for (int i=0; i<p2p.nodes.size(); i++) {
			Node n = p2p.nodes.get(i);
			int nodeid = n.getNodeId();
			if (id == nodeid)
				exists = true;
		}
		if (exists) {
			System.out.println("Node with id "+id+" already exists in the system; try again");
			cmdComplete = true;
			return;
		}
		
		//Create a thread that will simulate behavior of node with requested id
		Node p = new Node (id, p2p);
		
		//add the new Node to p2p's ArrayList for administrative purposes (?)
		p2p.nodes.add(p);
	}
	
	
	/*
	 * Coordinator has been asked to ask node with parameter id to locate
	 * key; if node id does not exist in system, then command is invalid
	 * TODO:
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void find(int id, int key) {
		//Error checking
		if (id < 0 || id > 255) {
			System.out.println("Id is assumed to be in the range 0 to 255; try again");
			cmdComplete = true;
			return;
		}
		
		if (key < 0 || key > 255) {
			System.out.println("Key is assumed to be in the range 0 to 255; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: according to Piazza post @345, this may have to involve messages
		//Check to see that node id exists in system
		boolean exists = false;
		for (int i=0; i<p2p.nodes.size(); i++) {
			Node n = p2p.nodes.get(i);
			int nodeid = n.getNodeId();
			if (id == nodeid)
				exists = true;
		}
		if (!exists) {
			System.out.println("Node with id "+id+" does not exist in the system; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: send find message to node id
		
	}
	
	
	/*
	 * Coordinator has been asked to ask node with parameter id to leave the
	 * system; all transfer of keys and administrative work is done by Node
	 * TODO:
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void leave(int id) {
		//Error checking
		if (id < 0 || id > 255) {
			System.out.println("Id is assumed to be in the range 0 to 255; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: according to Piazza post @345, this may have to involve messages
		//Check to see that node id exists in system
		boolean exists = false;
		for (int i=0; i<p2p.nodes.size(); i++) {
			Node n = p2p.nodes.get(i);
			int nodeid = n.getNodeId();
			if (id == nodeid)
				exists = true;
		}
		if (!exists) {
			System.out.println("Node with id "+id+" does not exist in the system; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: send leave message to node id
		
	}
	
	
	/*
	 * Coordinator has been asked to start an output that lists the keys
	 * stored locally at each of the nodes, in increasing order of the node
	 * ids (start at node 0, send messages to successors)
	 * TODO:
	 * Once the proper procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void showall() {
		//TODO: send show all message to node 0
	}
	
	
	/*
	 * Coordinator has been asked to start an output that lists the keys
	 * stored locally at node with parameter id, provided that it exists
	 * TODO:
	 * Once the proper join procedure has finished, Node must mark cmdComplete
	 * as true
	 */
	protected void show(int id) {
		
		//TODO: according to Piazza post @345, this may have to involve messages
		//Check to see that node id exists in system
		boolean exists = false;
		for (int i=0; i<p2p.nodes.size(); i++) {
			Node n = p2p.nodes.get(i);
			int nodeid = n.getNodeId();
			if (id == nodeid)
				exists = true;
		}
		if (!exists) {
			System.out.println("Node with id "+id+" does not exist in the system; try again");
			cmdComplete = true;
			return;
		}
		
		//TODO: send show message to node id
		
	}
	
}
