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
	
	private String [] origMessageWords;

	
	/*
	 * Constructor based on a method request from another Node received through
	 * this Node's Server;
	 */
	protected AlgorithmWaitingThread(Node node, String algorithm, String message) {
		this.node = node;
		this.nodeId = node.getNodeId();
		this.algorithm = algorithm;
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
			String sendReply = retMessage + " " + returnValue;
			node.p2p.send(sendReply + " " + nodeId, nodeId, reqNode);
		}
		
		//Find predecessor: find it, then send reply to requesting Node
		if (algorithm.compareTo("find_predecessor") == 0) {
			int returnValue = findJustPredecessor(Integer.parseInt(origMessageWords[3]));
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
		
		int nprime = findPredecessor(id);
		
		if (nprime == nodeId) {
			return node.getSuccessor();
		}
		
		else { //ask nprime for its successor
			
			int reqcnt = ++node.reqcnt;
			String successorreq = "successor " +reqcnt+" " + nprime;
			AckTracker successor_reply = new AckTracker(1);
			node.recvacks.put(successorreq, successor_reply); //wait for a single reply
			node.p2p.send("req " + successorreq + " " + nodeId, nodeId, nprime);
			
			//wait on reply
			while (successor_reply.toreceive > 0) {}
			
			String reply_id = successor_reply.validacks.get(0);
			return Integer.parseInt(reply_id);
			
		}

	}


	/*
	 * This thread's Node (node n) has been asked to find id's predecessor
	 */
	private int findJustPredecessor(int id) {
		
		//If you are this id's predecessor, return your nodeID
		if(node.p2p.insideHalfInclusiveInterval(id, nodeId, node.getSuccessor())) {
			return nodeId;
		}
		
		//Otherwise pass the message onto someone else
		int nprime = node.closestPrecedingFinger(id);
		//System.out.println("Closest preceding finger:"+nprime);
		
		String pred_req = "find_predecessor "+origMessageWords[2]+" " + id;
		node.p2p.send("req " + pred_req + " " + id, id, nprime);
		
		return -1;
	}
	
	
	/*
	 * The find_successor algorithm uses this helper function to find
	 * the predecessor of the given id
	 */
	private int findPredecessor(int id) {
		
		int nprime = nodeId;
		int nprimesuccessor = node.getSuccessor();
		
		//If the requested id is our own, we have that information
		if (id == nodeId) {
			return node.getPredecessor();
		}
		
		while (!node.p2p.insideHalfInclusiveInterval(id, nprime, nprimesuccessor)) {
			
			if (nprime == nodeId) { //call our node's method
				nprime = node.closestPrecedingFinger(id);
				
				//set nprimesuccessor
				if (nprime == nodeId) {
					nprimesuccessor = node.getSuccessor();
				}
				else { //ask nprime for its successor
					
					int reqcnt = ++node.reqcnt;
					String successorreq = "successor " +reqcnt+" " + nprime;
					AckTracker successor_reply = new AckTracker(1);
					node.recvacks.put(successorreq, successor_reply); //wait for a single reply
					node.p2p.send("req " + successorreq + " " + nodeId, nodeId, nprime);

					//wait on reply
					while (successor_reply.toreceive > 0) {}
					
					String reply_id = successor_reply.validacks.get(0);
					nprimesuccessor = Integer.parseInt(reply_id);
					
				}
			}
			
			else { //send a message to nprime
				
				int reqcnt = ++node.reqcnt;
				String req = "closest_preceding_finger " +reqcnt+" " + id;
				AckTracker closest_preceding_finger_reply = new AckTracker(1);
				node.recvacks.put(req, closest_preceding_finger_reply); //wait for a single reply
				node.p2p.send("req " + req + " " + nodeId, nodeId, nprime);
				
				//wait on reply
				while (closest_preceding_finger_reply.toreceive > 0) {}
				
				String reply_finger = closest_preceding_finger_reply.validacks.get(0);
				nprime = Integer.parseInt(reply_finger);
				
				//set nprimesuccessor
				if (nprime == nodeId) {
					nprimesuccessor = node.getSuccessor();
				}
				else { //ask nprime for its successor
					
					reqcnt = ++node.reqcnt;
					String successorreq = "successor " +reqcnt+" " + nprime;
					AckTracker successor_reply = new AckTracker(1);
					node.recvacks.put(successorreq, successor_reply); //wait for a single reply
					node.p2p.send("req " + successorreq + " " + nodeId, nodeId, nprime);
					
					//wait on reply
					while (successor_reply.toreceive > 0) {}
					
					String reply_id = successor_reply.validacks.get(0);
					nprimesuccessor = Integer.parseInt(reply_id);
					
				}
				
			}

		}

		return nprime;
	}

}
