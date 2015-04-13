package mp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;


/*
 * Thread to accept connections from other Nodes
 */
public class Server extends Thread {
	
	//creator of this thread
	private Node node;
	
	//The socket that the Node listens on
	ServerSocket server;
	
	
	protected Server(Node node) {
		this.node = node;
		
		try {
			server = new ServerSocket();
			server.setReuseAddress(true); //Set to reuse, need to do it on unbound socket first, then bind it later
			server.bind(new InetSocketAddress(7500 + node.getNodeId())); //guarantees unique port
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
	
		Socket socket = null;
		
		while (true) {
			try {
				socket = server.accept();
				handleNewSocket(socket);
			} catch (SocketException e) {
				break;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		
	}
	
	
	/*
	 * Helper function to handle when a Node has connected with this Server
	 * Since a Node will only temporarily connect with the send method
	 * in the PeerToPeerLookupService class, we close the connection after
	 * a message is received
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
		String msgId = words[1] + " " + words[2] + " " + words[3];
		
		if (words[0].compareTo("req") == 0) {
			
			int sendId = Integer.parseInt(sendIdString);
			String returnValue = ""+ -1;
			boolean sendReturnValue = false;
			
			if (words[1].compareTo("find_successor") == 0) {
				//create waiting thread to get all the answers to this
				new AlgorithmWaitingThread(node, words[1], msg);
			}
			
			else if (words[1].compareTo("successor") == 0) {
				returnValue = String.valueOf(node.getSuccessor());
				sendReturnValue = true;
			}
			
			else if (words[1].compareTo("closest_preceding_finger") == 0) {
				returnValue = String.valueOf(node.closestPrecedingFinger(Integer.parseInt(words[3])));
				sendReturnValue = true;
			}
			
			else if (words[1].compareTo("find_predecessor") == 0) {
				//create waiting thread to get all the answers to this
				new AlgorithmWaitingThread(node, words[1], msg);
			}
			
			else if (words[1].compareTo("set_predecessor") == 0) {
				node.predecessor = Integer.parseInt(words[3]);
			}
			else if (words[1].compareTo("set_successor") == 0) {
				node.finger_table[0].node = Integer.parseInt(words[3]);
			}
			else if (words[1].compareTo("update_finger_table") == 0) {
				node.updateFingerTable(Integer.parseInt(words[2]), Integer.parseInt(words[3]));
			}
			else if (words[1].compareTo("leaving_update_finger_table") == 0) {
				//special finger table update request from a leaving node
				node.updateFingerTableOnLeave(Integer.parseInt(words[2]), Integer.parseInt(words[3]), Integer.parseInt(words[4]));
			}
			else if (words[1].compareTo("force_transfer") == 0)  {
				ArrayList<Integer> key_str = new ArrayList<Integer>();
				//Add transferred keys to hashmap
				for (int i=4; i<words.length; i++) {
					Integer key = new Integer(words[i]);
					node.keys.put(key, true);
					key_str.add(key);
				}
			}
			else if (words[1].compareTo("transfer_keys") == 0) {
				//move keys in (range_start, sendId] to the sendId
				int range_start = Integer.parseInt(words[3]);
				
				returnValue = "";
				
				Set<Integer> keyset = node.keys.keySet();
				Iterator<Integer> it = keyset.iterator();
				while (it.hasNext()) {
					Integer key = it.next();
					if (!node.keys.get(key)) continue; //do not move key if it's not ours
					if (node.p2p.insideHalfInclusiveInterval(key.intValue(), range_start, sendId)) {
						//take it out of our keys
						node.keys.put(key, false);
						//add it to keysReturnValue
						returnValue = returnValue + key.toString() + " ";
					}
				}
				
				if (returnValue.compareTo("") != 0)
					returnValue = returnValue.substring(0, returnValue.length()-1);
				
				sendReturnValue = true;
			}
			
			if (sendReturnValue) {
				String sendReply = "ack " + msgId + " " + returnValue;
				node.p2p.send(sendReply + " " + node.getNodeId(), node.getNodeId(), sendId);
			}
			
		}
		
		else if (words[0].compareTo("ack") == 0) {
			
			boolean updaterecvacks = false;

			if (words[1].compareTo("find_successor") == 0)
				updaterecvacks = true;
			
			else if (words[1].compareTo("successor") == 0)
				updaterecvacks = true;
			
			else if (words[1].compareTo("closest_preceding_finger") == 0)
				updaterecvacks = true;
			
			else if (words[1].compareTo("find_predecessor") == 0)
				updaterecvacks = true;
			
			else if (words[1].compareTo("transfer_keys") == 0) {
				String ack = new String(msg);
				AckTracker replyTracker = node.recvacks.get(msgId);
				replyTracker.validacks.add(ack);
				replyTracker.toreceive--;
			}
			
			if (updaterecvacks) {
				String returnValue = words[4];
				AckTracker replyTracker = node.recvacks.get(msgId);
				replyTracker.validacks.add(returnValue);
				replyTracker.toreceive--;
			}
			
		}
		
	}

}
