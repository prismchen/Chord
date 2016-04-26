import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


public class FingerTable {
	private int identifier;
	private int bitNum;
	public Finger[] fingers;
	public int successor;
	public int predecessor;
	
	
	// Ctor
	public FingerTable(int m, int identifier) {
		this.identifier = identifier;
		this.bitNum = m;
		fingers = new Finger[m];
		for(int i = 0; i < 8; i++){
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
//			socketIn.println("getPredecessor");
//			return Integer.parseInt(socketOut.readLine());
			return RemoteProcedureCall(node, "getPredecessor");
		}
		
		public int setPredecessor(int value) throws NumberFormatException, IOException{
//			socketIn.println("setPredecessor " + value);
//			return socketOut.readLine();
			return RemoteProcedureCall(node, "setPredecessor " + value);
		}
		
		public int RemoteProcedureCall(int node, String msg) throws IOException{
//	System.out.println("In finger RemoteProcedureCall");
//	System.out.println("node " + node + " msg " + msg );
			Socket socketTemp = new Socket("localhost", 9000 + node);
			PrintWriter socketTempIn = new PrintWriter(socketTemp.getOutputStream(), true);
			BufferedReader socketTempOut = new BufferedReader(new InputStreamReader(socketTemp.getInputStream()));
			socketTempIn.println("temp");
			socketTempIn.println(msg);
			String feedBack;
			int result = -1;
			while((feedBack = socketTempOut.readLine()) != null){
//				System.out.println("275 feedBack " + feedBack);
				result = Integer.parseInt(feedBack);
				break;
			}
			socketTemp.close();
			socketTempIn.close();
			socketTempOut.close();
//		System.out.println("279 " + feedBack);	
			return result;
		}
	}
}
