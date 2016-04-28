import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;


public class FingerTable {
	private int identifier;
	private int bitNum;
	public Finger[] fingers;
	public int successor;
	public int predecessor;
	
	private int maxDelay;
	private int minDelay;
	
	// Ctor
	public FingerTable(int m, int identifier, int maxDelay, int minDelay) {
		this.identifier = identifier;
		this.bitNum = m;
		
		this.maxDelay = maxDelay;
		this.minDelay = minDelay;
		
		fingers = new Finger[m];
		for(int i = 0; i < bitNum; i++){
			fingers[i] = new Finger(identifier, i);
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
	
	private class Finger {
		public int start;
		public int node; 
		
		private Finger(int identifier, int fingerIndex) {	
			start = (int) (identifier + Math.pow(2, fingerIndex));
		}
		
		public int getterStart(){
			return start;
		}
		
		public void setterStart(int Start){
			start = Start;
		}
		
		public int getterNode(){
			return node;
		}
		
		public void setterNode(int Node){
			node = Node;
		}
		
		public int getPredecessor() throws NumberFormatException, IOException{
			return RemoteProcedureCall(node, "getPredecessor");
		}
		
		public int setPredecessor(int value) throws NumberFormatException, IOException{
			return RemoteProcedureCall(node, "setPredecessor " + value);
		}
		
public int RemoteProcedureCall(int node, String msg) throws IOException{
			
			Timer timer = new Timer();
			
			try (
				Socket socketTemp = new Socket("localhost", 9000 + node);
				PrintWriter socketTempIn = new PrintWriter(socketTemp.getOutputStream(), true);
				BufferedReader socketTempOut = new BufferedReader(new InputStreamReader(socketTemp.getInputStream()))) {
				
				socketTempIn.println("temp");
				
				timer.schedule(new TimerTask() {
					public void run() {	
						socketTempIn.println(msg);
					}
				}, (long) (minDelay + (long)(Math.random() * ((maxDelay - minDelay) + 1))) );
				
				int result = -1;
				
				String feedBack = socketTempOut.readLine();
			
	
				result = Integer.parseInt(feedBack);
				return result;
			} catch (ConnectException e) {
				return -20; // the remote socket is closed
			}
		}
	}
}
