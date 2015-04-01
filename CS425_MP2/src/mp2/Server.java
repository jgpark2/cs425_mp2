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
	private void handleNewSocket(Socket socket) throws IOException {
		
		BufferedReader ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		
		String input = "";
		while ((input = ins.readLine())==null) {} //get message from connecting Node
		
		node.receive(input);
		
		ins.close();
		socket.close();
	}

}
