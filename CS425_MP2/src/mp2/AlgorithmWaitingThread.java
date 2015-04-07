package mp2;

public class AlgorithmWaitingThread extends Thread {
	
	//creator of this thread
	private Node node;
	private int nodeId;
	//the algorithm this thread should execute
	private String algorithm;
	//requesting node, if any
	private int reqNode = -1;
	//message to return to the requesting node, if any
	private String retMessage;
	
	private String origMessage;
	private String [] origMessageWords;

	
	/*
	 * Constructor based on a method request from another Node received through
	 * this Node's Server;
	 */
	protected AlgorithmWaitingThread(Node node, String algorithm, String message) {
		this.node = node;
		this.nodeId = node.getNodeId();
		this.algorithm = algorithm;
		this.origMessage = message;
		
		String[] words = message.split("\\s+");
		this.origMessageWords = words;
		
		String sendIdString = words[words.length-1];
		this.reqNode = Integer.parseInt(sendIdString);
		
		String msgId = words[1] + " " + words[2] + " " + words[3];
		this.retMessage = "ack "+msgId; //return value and sendId will be added later
		
		new Thread(this, "Node"+nodeId+algorithm+"waitingthread").start();
	}
	
	
	/*
	 * The main purpose of this thread is to wait for responses responding to
	 * queries pertinent to this algorithm during its execution
	 */
	public void run() {
		
		//Find successor: find it, then send reply to requesting Node
		if (algorithm.compareTo("find_successor") == 0) {
			int returnValue = findSuccessor();
			if(returnValue==-1)
				return;
			String sendReply = retMessage + " " + returnValue;
			node.p2p.send(sendReply + " " + nodeId, nodeId, reqNode);
		}
		
		//Find predecessor: find it, then send reply to requesting Node
		if (algorithm.compareTo("find_predecessor") == 0) {
			int returnValue = findPredecessor(Integer.parseInt(origMessageWords[3]));
			if(returnValue==-1)
				return;
			String sendReply = retMessage + " " + returnValue;
			node.p2p.send(sendReply + " " + nodeId, nodeId, reqNode);
		}
		
	}
	
	
	/*
	 * Find successor algorithm
	 * This thread's Node has been asked to find an id's successor
	 */
	private int findSuccessor() {
		
		int id = Integer.parseInt(origMessageWords[3]);
		
		if(node.p2p.insideHalfInclusiveInterval(id, node.getPredecessor(), nodeId)) {
			//System.out.println("whee:"+nodeId);
			return nodeId;
		}
		
		if(node.p2p.insideHalfInclusiveInterval(id, nodeId, node.getSuccessor())) {
			//System.out.println("whee:"+nodeId);
			return node.getSuccessor();
		}
		
		//Otherwise pass the message onto someone else
		int nprime = node.closestPrecedingFinger(id);
		//System.out.println("Closest preceding finger:"+nprime);
		
		String succ_req = "find_successor "+origMessageWords[2]+" " + id;
		node.p2p.send("req " + succ_req + " " + id, id, nprime);
		
		return -1;
	}


	/*
	 * This thread's Node (node n) has been asked to find id's predecessor
	 */
	private int findPredecessor(int id) {
		
		//If you are this id's predecessor, return your nodeID
		if(node.p2p.insideHalfInclusiveInterval(id, nodeId, node.getSuccessor())) {
			//System.out.println("whee:"+nodeId);
			return nodeId;
		}
		
		//Otherwise pass the message onto someone else
		int nprime = node.closestPrecedingFinger(id);
		//System.out.println("Closest preceding finger:"+nprime);
		
		String pred_req = "find_predecessor "+origMessageWords[2]+" " + id;
		node.p2p.send("req " + pred_req + " " + id, id, nprime);
		
		return -1;
	}

}
