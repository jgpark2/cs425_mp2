package mp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/*
 * Thread to communicate with another Node
 */
public class NodeConnection extends Thread {
	
	//creator of this thread
	private Node node;
	//Stream from socket connection
	private BufferedReader ins;
	//Stream to socket connection
	private PrintWriter outs;
	
	//Id of the Node on the other end
	protected int recvId;
	
	
	protected NodeConnection(Node node, Socket socket, int recvId) {
		this.node = node;
		this.recvId = recvId;
		
		try {
			ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			outs = new PrintWriter(socket.getOutputStream(), true);
			
			//send our node id to the Node at the other end
			outs.println(node.getNodeId());
			outs.println("establishing connection");
		} catch (IOException e) {
			System.out.println("Unable to open socket stream");
			e.printStackTrace();
			System.exit(-1);
			return;
		}
		
		new Thread(this, "ConnectionFrom"+node.getNodeId()+"To"+recvId).start();
	}

	
	/*
	 * The main purpose of this thread is to read from the socket connection
	 * input stream
	 */
	public void run() {
		
		String input = "";
		try {
			
			while ((input = ins.readLine()) != null) {
				//TODO: maybe some input gets handled here, maybe not...
				if (input.compareTo("done") == 0)
					break;
				node.receive(input);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Once a connection is done being used, both sides must receive "done"
		outs.println("done");
		try {
			ins.close();
			ins = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		outs.close();
		outs = null;
		
	}
	
	
	/*
	 * Send a message to the other Node that this NodePointer is communicating
	 * with over the socket connection
	 */
	protected void send(String msg) {
		if (msg != null && outs != null)
			outs.println(msg);
	}
	
}
