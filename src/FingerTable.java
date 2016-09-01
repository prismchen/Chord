import java.io.IOException;


public class FingerTable {
	private int identifier;
	private int bitNum;
	public Finger[] fingers;
	
	private int maxDelay;
	private int minDelay;
	
	// Ctor
	public FingerTable(int fingerTableSize, int identifier, int maxDelay, int minDelay) {
		this.identifier = identifier;
		this.bitNum = fingerTableSize;
		
		this.maxDelay = maxDelay;
		this.minDelay = minDelay;
		
		fingers = new Finger[fingerTableSize];
		for(int i = 0; i < bitNum; i++){
			fingers[i] = new Finger(identifier, i, minDelay, maxDelay);
		}
	}
	
	public int getFingerNode(int index){ 
		return fingers[index].getterNode();
	}
	
	public void setFingerNode(int index, int Node){
		fingers[index].setterNode(Node);
	}
	
	public int getFingerStart(int index){
		return fingers[index].getterStart();
	}
	
	public int getFingerPredecessor(int index) throws NumberFormatException, IOException{
		return fingers[index].getPredecessor();
	}
	
	public int setFingerPredecessor(int index, int value) throws NumberFormatException, IOException{
		return fingers[index].setPredecessor(value);
	}
	
	public void show(){
		for(int i = 0; i < 8; i++){
			System.out.println("Finger " + i + ": " + getFingerNode(i));
		}
	}

}
