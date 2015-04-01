package mp2;

import java.util.ArrayList;

/*
 * For some request messages, acks are necessary to track precisely
 * (i.e. a search request, where the ack may contain the id of a Node
 * that contains the key, or "null" otherwise)
 * This simple class is used to store received acks that will be associated
 * with a certain message identifier String
 * in the recvacks structure belonging to a Node
 */
public class AckTracker {
	
	//How many more acks are expected to be received
	public volatile int toreceive;
	//All the acks containing "null" information (indicating the Node
	//could not perform the write, or something similar)
	public ArrayList<String> nullacks = new ArrayList<String>();
	//All the acks that do not contain "null" information
	public ArrayList<String> validacks = new ArrayList<String>();
	
	public AckTracker() {
		toreceive = 0;
	}
	
	public AckTracker(int torecv) {
		toreceive = torecv;
	}
}
