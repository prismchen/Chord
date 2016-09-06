package ChordServer;

import java.io.IOException;

public class FingerTable {
	private int mIdentifier;
	private int mBitNum;
	public Finger[] mFingers;
	
	private int mMaxDelay;
	private int mMinDelay;
	
	// Ctor
	public FingerTable(int fingerTableSize, int identifier, int maxDelay, int minDelay) {
		this.mIdentifier = identifier;
		this.mBitNum = fingerTableSize;
		
		this.mMaxDelay = maxDelay;
		this.mMinDelay = minDelay;
		
		mFingers = new Finger[fingerTableSize];
		for(int i = 0; i < mBitNum; i++){
			mFingers[i] = new Finger(identifier, i, minDelay, maxDelay);
		}
	}
	
	public int getFingerNode(int index){ 
		return mFingers[index].getterNode();
	}
	
	public void setFingerNode(int index, int Node){
		mFingers[index].setterNode(Node);
	}
	
	public int getFingerStart(int index){
		return mFingers[index].getterStart();
	}
	
	public int getFingerPredecessor(int index) throws NumberFormatException, IOException{
		return mFingers[index].getPredecessor();
	}
	
	public int setFingerPredecessor(int index, int value) throws NumberFormatException, IOException{
		return mFingers[index].setPredecessor(value);
	}
	
	public void show(){
		for(int i = 0; i < 8; i++){
			System.out.println("ChordServer.Finger " + i + ": " + getFingerNode(i));
		}
	}

}
