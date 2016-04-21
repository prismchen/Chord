import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;


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
	
	public String setFingerPredecessor(int index, int value) throws NumberFormatException, IOException{
		return fingers[index].setPredecessor(value);
	}
	
	private class Finger {
		public int start;
		public int node; 
		public Socket socket;
		public PrintWriter socketIn;
		public BufferedReader socketOut;
		
		private Finger(int identifier, int fingerIndex) {
			
			start = (int) (identifier + Math.pow(2, fingerIndex));
//			socket = new Socket("localhost", 9000 + node);
//			socketIn = new PrintWriter(socket.getOutputStream(), true);
//			socketOut = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		
//		private Finger(int Start, int Node) throws UnknownHostException, IOException{
//			start = Start;
//			node = Node;
//			socket = new Socket("localhost", 9000 + node);
//			socketIn = new PrintWriter(socket.getOutputStream(), true);
//			socketOut = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//		}
		
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
			socketIn.println("getPredecessor");
			return Integer.parseInt(socketOut.readLine());
		}
		
		public String setPredecessor(int value) throws NumberFormatException, IOException{
			socketIn.println("setPredecessor " + value);
			return socketOut.readLine();
		}
	}
}
