package mp2;

/*
 * Simple class to store all the information for a finger table entry
 */
public class Finger {

	//start: (n + 2^k) (mod 2^m), where m is 8 and k goes from 0 to m-1
	protected int start;
	//node: first node id >= n.finger[k].start
	protected int node;

	protected Finger () {
		this.start = 0;
		this.node = 0;
	}
	
	protected Finger(int start, int node) {
		this.start = start;
		this.node = node;
	}
	
	protected static int calculateStart(int id, int i, int m) {
		return (id + (int)Math.pow(2, i)) % (int)Math.pow(2, m);
	}
	
}
