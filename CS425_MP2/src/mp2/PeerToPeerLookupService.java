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
	//    ArrayList with initial setup of 256 slots, using the "set" method
	//    to add/leave new nodes in the system, and the "valid" field in
	//    the Node class to find a node and see if it's valid
	//    This implementation also helps with show-all
	protected ArrayList<Node> nodes = new ArrayList<Node>(256);
	
	//Coordinator thread to get and execute commands; should not be started
	//    until node 0 has been set up
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
		
		
		//TODO: declare other class member objects

	}
	
	
	public void start() {
		//TODO: initialize class member objects
		
		
	}

}
