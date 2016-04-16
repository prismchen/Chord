
public class FingerTable {
	private int bitNum;
	public Finger[] entries;
	public int successor;
	public int predecessor;
	
	
	// Ctor
	public FingerTable(int m) {
		this.bitNum = m;
		entries = new Finger[m];
		
	}
	
	class Finger {
		public int start;
		public int node; 
	}
}
