package mp2;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

/*
 * TODO: give instructions on how to run this code in cs425_mp2/instructions.txt
 */
public class PeerToPeerLookupService {
	
	//This is where all output from "show" commands goes
	//To write to: out.write(strtowrite+"\n"); //it needs this newline!
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


	public static void main(String[] args) {
		
		PeerToPeerLookupService p2p = new PeerToPeerLookupService(args);

		p2p.start();

	}
	

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
		
		nodes = new ArrayList<Node>();

	}
	
	
	public void start() {

		//indicate to Node that it doesn't join normally
		Node nprime = new Node(0, this, "initial");
		nodes.add(nprime);
		
		//Start Coordinator thread after node 0 is created
		coord = new Coordinator(this);
		
	}

}
