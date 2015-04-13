package mp2;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

/*
 * A class representing the starting point for a Chord-like
 * peer-to-peer system of nodes and file pointers
 */
public class PeerToPeerLookupService {
	
	//This is where all output from "show" commands goes
	protected BufferedWriter out;
	
	//Each node in the Chord system is represented as a Node thread in this
	//    ArrayList, being removed from the ArrayList when it leaves, and
	//    being added to the ArrayList when it joins
	protected ArrayList<Node> nodes;
	
	//Coordinator thread to get and execute commands
	protected Coordinator coord;
	
	//Stores the number of messages sent in the system
	//Used for performance analysis, can be displayed with
	//    "message count" utility method
	public volatile int messageCount;


	public static void main(String[] args) {
		PeerToPeerLookupService p2p = new PeerToPeerLookupService(args);
		p2p.start();
	}


	/*
	 * PeerToPeerLookupService constructor
	 * A filename to write show command output to may be in args parameter
	 */
	public PeerToPeerLookupService(String[] args) {
		
		this.out = new BufferedWriter(new OutputStreamWriter(System.out));
		
		if (args.length == 2 && args[0].compareTo("-g")==0) { //program is passed "-g filename"

			try {
				out = new BufferedWriter(new PrintWriter(args[1]));
			} catch (FileNotFoundException e) {
				System.out.println("Couldn't create output file");
				e.printStackTrace();
				out = new BufferedWriter(new OutputStreamWriter(System.out));
			}
			
		}
		
		this.nodes = new ArrayList<Node>();
		this.messageCount = 0;

	}
	
	
	/*
	 * Start the system running (initialize node 0) after PeerToPeer
	 * member variables have been initialized
	 */
	public void start() {

		Node nprime = new Node(0, this);
		nodes.add(nprime);
		
		//Start Coordinator thread after node 0 is created
		coord = new Coordinator(this);
		
	}
	
	
	/*
	 * Method used by any Node to send a message through sockets
	 * Eliminates confusion about sockets closing on one or both ends
	 * sendId = sender's ID/Source, recvID = receipient/Destination  
	 */
	protected synchronized int send(String msg, int sendId, int recvId) {
		
		int ret = -1;
		
		//Check to see that node id exists in system
		int nodeIdx = -1;
		for (int i=0; i<nodes.size(); i++) {
			Node n = nodes.get(i);
			int nodeid = n.getNodeId();
			if (recvId == nodeid) {
				nodeIdx = i;
			}
		}
		if (nodeIdx == -1) {
			return -1;
		}
		
		Socket socket = null;
		PrintWriter outs = null;
		
		try {
			socket = new Socket("127.0.0.1",7500+recvId);
			outs = new PrintWriter(socket.getOutputStream(), true);
			outs.println(msg);
			ret = 0;
			messageCount++;

		} catch (Exception e) {
			ret = -1;
		}
				
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {}
		}
		if (outs != null)
			outs.close();
				
		return ret;
	}
	
	
	/*
	 * Determines if x is on the interval (a,b) (exclusive) mod 2^m
	 * Consider the case where the interval is (0,0); we are conceptualizing
	 * this to mean that the interval is (0,256)
	 */
	protected boolean insideInterval(int x, int a, int b) {
		if (a == b)
			return (x != a); //if x is on the boundary
		
		if (b < a) { //a < 2^m && b >= 0
			
			if (x < a)
				x += (int)Math.pow(2, Node.m);
			
			b += (int)Math.pow(2, Node.m);
		}
		return (x > a) && (x < b);
	}
	
	/*
	 * Determines if x is on the interval (a,b] mod 2^m
	 * This case is necessary in findPredecessor when (nprime,nprimesuccessor] == (0,0]
	 */
	protected boolean insideHalfInclusiveInterval(int x, int a, int b) {
		if (b == a)
			return true;
		if (b < a) { //a <= 2^m && b >= 0
			
			if (x < a)
				x += (int)Math.pow(2, Node.m);
			
			b += (int)Math.pow(2, Node.m);
		}
		return ((x > a) && (x < b)) || (x==b);
	}

}
