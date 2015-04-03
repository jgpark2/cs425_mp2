package mp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * Thread to accept connections from other Nodes
 */
public class Server extends Thread {
	
	protected int m;
	
	//creator of this thread
	private Node node;
	//The socket that the Node listens on
	private ServerSocket server;
	
	
	protected Server(Node node) {
		this.node = node;
		this.m = node.m;
		
		try {
			server = new ServerSocket(7500 + node.getNodeId()); //guarantees unique port
        } catch (IOException e) {
			System.out.println("Could not listen on port " + (7500 + node.getNodeId()));
			e.printStackTrace();
			System.exit(-1);
			return;
        }
		
		new Thread(this, "NodeServer"+node.getNodeId()).start();
	}
	
	
	/*
	 * The main purpose of this thread is to accept connections from other nodes
	 */
	public void run() {
		
		Socket socket;
		
		while (true) {
			try {
				socket = server.accept();
				handleNewSocket(socket);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	/*
	 * Helper function to handle when a new Node has connected with this Server
	 * In some situations, a Node will only temporarily connect
	 */
	private void handleNewSocket(Socket socket) {
		
		BufferedReader ins;
		try {
			ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		} catch (IOException e1) {
			System.out.println("Could not open socket stream");
			e1.printStackTrace();
			return;
		}
		
		String input = "";
		//get message from connecting Node
		try {
			while ((input = ins.readLine())==null) {}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			ins.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		receive(input);
	}
	
	
	/*
	 * Receive and process a message sent to this Node
	 */
	protected void receive(String msg) {
		
		if (msg.compareTo("") == 0)
			return;
		
		String[] words = msg.split("\\s+");
		String sendIdString = words[words.length-1];
		String msgId = words[1] + " " + words[2] + " " + words[3]; //TODO: three-word identifiers
		
		if (words[0].compareTo("req") == 0) {
			
			int sendId = Integer.parseInt(sendIdString);
			int returnValue = -1;
			boolean sendReturnValue = false;
			
			if (words[1].compareTo("find_successor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a find_successor req message: \""+msg+"\"");
				//create waiting thread to get all the answers to this,
				//This thread will then call p2p.send so Server doesn't have to wait
				new AlgorithmWaitingThread(node, words[1], msg);
			}
			
			else if (words[1].compareTo("successor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a successor req message: \""+msg+"\"");
				returnValue = node.getSuccessor();
				sendReturnValue = true;
			}
			
			else if (words[1].compareTo("closest_preceding_finger") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a closest_preceding_finger req message: \""+msg+"\"");
				returnValue = node.closestPrecedingFinger(Integer.parseInt(words[3]));
				sendReturnValue = true;
			}
			
			else if (words[1].compareTo("predecessor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a predecessor req message: \""+msg+"\"");
				returnValue = node.getPredecessor();
				sendReturnValue = true;
			}
			
			else if (words[1].compareTo("set_predecessor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a set_predecessor req message: \""+msg+"\"");
				node.predecessor = Integer.parseInt(words[3]);
			}
			
			if (sendReturnValue) {
				String sendReply = "ack " + msgId + " " + returnValue;
				node.p2p.send(sendReply + " " + node.getNodeId(), node.getNodeId(), sendId);
			}
			
		}
		
		else if (words[0].compareTo("ack") == 0) {
			
			boolean updaterecvacks = false;

			if (words[1].compareTo("find_successor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a find_successor ack message: \""+msg+"\"");
				updaterecvacks = true;
			}
			
			else if (words[1].compareTo("successor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a successor ack message: \""+msg+"\"");
				updaterecvacks = true;
			}
			
			else if (words[1].compareTo("closest_preceding_finger") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a closest_preceding_finger ack message: \""+msg+"\"");
				updaterecvacks = true;
			}
			
			else if (words[1].compareTo("predecessor") == 0) {
				System.out.println("Node "+node.getNodeId()+" is processing a predecessor ack message: \""+msg+"\"");
				updaterecvacks = true;
			}
			
			if (updaterecvacks) {
				String returnValue = words[4];
				AckTracker replyTracker = node.recvacks.get(msgId);
				replyTracker.validacks.add(returnValue);
				replyTracker.toreceive--;
			}
			
		}
		
		else {
			System.out.println("Node "+node.getNodeId()+" received \""+msg+"\"");
		}
		
	}

}
