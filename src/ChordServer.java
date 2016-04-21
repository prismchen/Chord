import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.HashSet;

import org.omg.CORBA.portable.UnknownException;

public class ChordServer {
	
	private ServerSocket listener;
	
	private final static int m = 8;
	private int minDelay;
	private int maxDelay;
	private int initPort;
	private HashSet<Integer> runnningNodes;     //how to detect nodes are offline 

	
	PrintWriter clientOut;
	
	// Ctor of ChordServer
	public ChordServer() {
	
		try {
			listener = new ServerSocket(9000);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		readConfig("config");
		runnningNodes = new HashSet<Integer>();

	}
	
	private void readConfig(String config){
    	// Read and store process info from config file
    	try (BufferedReader configReader = new BufferedReader(new FileReader(config))) {

    		String currLine = configReader.readLine();		
    		String[] delayLimits = currLine.split(" ", 2);
			minDelay = Integer.parseInt(delayLimits[0]);
			maxDelay = Integer.parseInt(delayLimits[1]);
			currLine = configReader.readLine();	
			initPort = Integer.parseInt(currLine);
			
    	} catch (IOException e) {
			e.printStackTrace();
		}

    	// print out results from the configuration file.
    	System.out.println("Finish reading from the configuration file");
		System.out.println("Bounds of delay in milliSec: " + minDelay + " " + maxDelay);
		System.out.println("Starting port: " + initPort);
    }
	
	// run()
	public void run() throws IOException {
		
		new node(0).start();
			
		try {
			new ClientHandler(listener.accept()).start();
        } finally {
            listener.close(); 
        }
	}
	
	
	public static void main(String[] args) throws IOException {
		
		ChordServer server = new ChordServer();
		server.run();
		
	}
	

	// clientHandler thread
	private class ClientHandler extends Thread {
		
		Socket socket;
		BufferedReader clientIn;

		// Ctor
		public ClientHandler(Socket socket) throws IOException {
			this.socket = socket;
			clientIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			clientOut = new PrintWriter(socket.getOutputStream(), true);
			System.out.print("Connected to client ...\n");
		}
		
		public void run() {
			String command = "";
			try {
				while ((command = clientIn.readLine()) != null) { 
					////// test //////
					System.out.println(command);
					String [] tokens = command.split(" ");
					if(tokens[0].indexOf("join") == 0){
						new node(Integer.parseInt(tokens[1]));
					}

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
		
	}
	
	// node thread
	private class node extends Thread {
		
		private int identifier;
		private ServerSocket serverSocket;
		private HashSet<Integer> keys;
		private FingerTable fingerTable;
		private Hashtable<Integer, Socket> nodeSocks;				//store the socket from other nodes  --identifier socket
		private Hashtable<Integer, PrintWriter> nodeOuts;
		private Hashtable<Integer, BufferedReader> nodeIns;
		private Socket socket0;
		private PrintWriter SendTo0;
		private BufferedReader RecFrom0;
		private int predecessor;
		private int successor;
		
		public node(int identifier) throws UnknownHostException, IOException {
			this.identifier = identifier;
			serverSocket = new ServerSocket(9000 + identifier);
			this.keys = new HashSet<Integer>();
			if(identifier == 0){
				for (int i=0; i<256; i++) {
					keys.add(i);
				}
			}
			this.fingerTable = new FingerTable(m, identifier);
			nodeSocks = new Hashtable<Integer, Socket>();
			nodeOuts = new Hashtable<Integer, PrintWriter>();
			nodeIns = new Hashtable<Integer, BufferedReader>();
			
			// connect with node 0
			socket0 = new Socket("localhost", 9000);
			RecFrom0 = new BufferedReader(new InputStreamReader(socket0.getInputStream()));
			SendTo0 = new PrintWriter(socket0.getOutputStream(), true);
			initFingerTable();
			updateOthers();
			
			
//			runnningNodes.add(identifier);	
		}
		
		public void run() {
			System.out.println("node " + identifier + " is running ...");
			while (true) {
				
			}
		}
		
		public int getSuccessor(){
			return successor;
		}
		
		public void initFingerTable() throws IOException{
			int finger0Start = fingerTable.getFingerStart(0);
			String msg = "findSuccessor " + finger0Start;
			SendTo0.println(msg);
			String feedBack = RecFrom0.readLine();
			fingerTable.setFingerNode(0, Integer.parseInt(feedBack));
			predecessor = fingerTable.getFingerPredecessor(0);
			fingerTable.setFingerPredecessor(0, identifier);
		
			for (int i = 1; i < m; i++){
				int fingerStart = fingerTable.getFingerStart(i);
				int fingerNode = fingerTable.getFingerNode(i-1);
				if (isInRange(fingerStart, identifier, fingerNode)){
					fingerTable.setFingerNode(i, fingerNode);
				} else {
					msg = "findSuccessor " + fingerStart;
					SendTo0.println(msg);
					feedBack = RecFrom0.readLine();
					fingerTable.setFingerNode(i, Integer.parseInt(feedBack));
				}
			}
		}
		
		public void updateOthers() throws IOException{
			for (int i = 0; i < m; i++){
				int p = findPredecessor((int)(identifier - Math.pow(2, i)));
				String msg = "Temp " + "updateFingerTable " + identifier + " " + i+1;
				RemoteProcedureCall(p, msg);
			}
		}
		
		public int findSuccessor(int id) throws IOException {
			int nPrime = findPredecessor(id);
			String msg = "Temp " + "getSuccessor";
			return RemoteProcedureCall(nPrime, msg);
			
		}
		
		public int findPredecessor(int id) throws IOException{
			int nPrime = identifier;
			String msg = "Temp " + "getSuccessor";
			int nPrimeSuccessor = RemoteProcedureCall(nPrime, msg);

			while(!isInRange(id, nPrime, nPrimeSuccessor )){
				msg = "Temp " + "closestPrecedingFinger";
				nPrime = RemoteProcedureCall(nPrime, msg);
			}
			return nPrime;
		}
		
		public int closestPrecedingFinger(int id){
			for (int i = m-1; i >= 0; i--){
				if(isInRange(fingerTable.getFingerNode(i), identifier, id)){
					return fingerTable.getFingerNode(i);
				}
			}
			return identifier;
		}
		
		public void updateFingerTable(int value, int index) throws IOException{
			if(isInRange(value, identifier, fingerTable.getFingerNode(index))){
				fingerTable.setFingerNode(index, value);
				int p = predecessor;
				String msg = "Temp " + "updateFingerTable " + value + " " + index;
				RemoteProcedureCall(p, msg);
			}
		}
		
		public int RemoteProcedureCall(int node, String msg) throws IOException{
			Socket socketTemp = new Socket("localhost", 9000 + node);
			PrintWriter socketTempIn = new PrintWriter(socketTemp.getOutputStream(), true);;
			BufferedReader socketTempOut = new BufferedReader(new InputStreamReader(socketTemp.getInputStream()));;
			socketTempIn.println(msg);
			String feedBack = socketTempOut.readLine();
			socketTemp = null;
			return Integer.parseInt(feedBack);
		}
		
		public boolean isInRange(int value, int lowerBound, int upperBound){
			if(lowerBound == upperBound){
				throw new UnknownException(null);
			} else if (lowerBound < upperBound){
				return (lowerBound <= value) && (value < upperBound);
			} else {
				return !((upperBound <= value) && (value < lowerBound));
			}
		}
	}

}
