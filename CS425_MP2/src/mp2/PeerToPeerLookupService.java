package mp2;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

/*
 * TODO: give instructions on how to run this code in cs425_mp2/instructions.txt
 */
public class PeerToPeerLookupService {
	
	//This is where all output from "show" commands goes
	//To write to: out.write(strtowrite+"\n"); //it needs this newline!
	//then call: out.flush();
	protected BufferedWriter out;
	
	//While node identifiers & keys can only be 0-255, we'll just use int types
	//    for simplicity (consider int for loops)
	
	//Each node in the Chord system is represented as a Node thread in this
	//    ArrayList, being removed from the ArrayList when it leaves, and
	//    being added to the ArrayList when it joins
	protected ArrayList<Node> nodes;
	
	//Coordinator thread to get and execute commands; should not be started
	//    until node 0 has been set up (which shouldn't take that much time,
	//    so maybe we won't worry about it)
	protected Coordinator coord;
	
	public volatile int messageCount;


	public static void main(String[] args) {
		
		PeerToPeerLookupService p2p = new PeerToPeerLookupService(args);

		p2p.start();

	}
	

	public PeerToPeerLookupService(String[] args) {
		
		this.out = new BufferedWriter(new OutputStreamWriter(System.out));
		
		if (args.length == 2 && args[0].compareTo("-g")==0) { //program is passed "-g filename"

			try {
				out = new BufferedWriter(new PrintWriter(args[1]));
//				System.out.println("PeerToPeer out is file "+args[1]);
			} catch (FileNotFoundException e) {
				System.out.println("Couldn't create output file");
				e.printStackTrace();
				out = new BufferedWriter(new OutputStreamWriter(System.out));
			}
			
		}
		else {
//			System.out.println("PeerToPeer out is System.out");
		}
		
		nodes = new ArrayList<Node>();
		messageCount = 0;

	}
	
	
	public void start() {

		//indicate to Node that it doesn't join normally
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
//			System.out.println("Node with id "+recvId+" does not exist in the system; try again");
			return -1;
		}
		
		Socket socket = null;
		PrintWriter outs = null;
		
		try {
			socket = new Socket("127.0.0.1", 7500+recvId); //this generated an error!
			//the 6th node to get added had an error trying to send a message in join
			outs = new PrintWriter(socket.getOutputStream(), true);
			outs.println(msg);
			ret = 0;
			messageCount++;

		} catch (Exception e) {
			System.out.println("Failed to connect to node "+recvId);
			e.printStackTrace();
			ret = -1;
		}
				
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
		if (a == b) {
//			System.out.println("very special case, "+x+" is not in ("+a+","+b+")"); //just for debugging
			return (x != a); //if x is on the boundary
		}
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
