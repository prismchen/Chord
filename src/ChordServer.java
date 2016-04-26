import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.omg.CORBA.portable.UnknownException;

public class ChordServer {

	private final static int m = 8;
	private int minDelay;
	private int maxDelay;
	private int initPort;
	private HashSet<Integer> runnningNodes;     //how to detect nodes are offline 

	PrintWriter clientOut;
	
	// Ctor of ChordServer
	public ChordServer() {	
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
		new ClientHandler();

	}
	
	public static void main(String[] args) throws IOException {	
		ChordServer server = new ChordServer();
		server.run();
	}

	// clientHandler thread
	private class ClientHandler extends Thread {
		
		ServerSocket clientListener;
		// connenction from all nodes
		HashMap<Integer, Socket> socketMap;
		HashMap<Integer, BufferedReader> readertMap;
		HashMap<Integer, PrintWriter> writerMap;

		// Ctor
		public ClientHandler() throws IOException {
			clientListener = new ServerSocket(8999);					
			new node(0).start();	
			new ConsoleHandler().start();
		}
		
		// accept connections from node
		public void run() {
			
			try {
				// node connects to client, store all the connection in case of failure detector (heart beat?)
				Socket socket = clientListener.accept();
				BufferedReader socketIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter socketOut = new PrintWriter(socket.getOutputStream(), true);
				int identifier = Integer.parseInt(socketIn.readLine());
				socketMap.put(identifier, socket);
				readertMap.put(identifier, socketIn);
				writerMap.put(identifier, socketOut);
				System.out.println(socketIn.readLine());		// ACK for join node
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		private class ConsoleHandler extends Thread {
			BufferedReader stdIn;
			public ConsoleHandler() {
				stdIn = new BufferedReader(new InputStreamReader(System.in));
			}
			public void run(){
				String userInput;
				try {
					while((userInput = stdIn.readLine()) != null){
						String [] tokens = userInput.split(" ");
						if(tokens[0].indexOf("join") == 0){
							new node(Integer.parseInt(tokens[1]));
						} else if(tokens[0].equals("show")){
							RemoteProcedureCall(Integer.parseInt(tokens[1]), "show");
							System.out.println("ACK");
						} else if(tokens[0].equals("showAll")){
							RemoteProcedureCall(0, "showAll");
						}
						//more commands go here
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			public int RemoteProcedureCall(int node, String msg) throws IOException{
				System.out.println("In client RemoteProcedureCall");
				System.out.println("node " + node + " msg " + msg );
				try(
					Socket socketTemp = new Socket("localhost", 9000 + node);
					PrintWriter socketTempIn = new PrintWriter(socketTemp.getOutputStream(), true);
					BufferedReader socketTempOut = new BufferedReader(new InputStreamReader(socketTemp.getInputStream()));
					){
						socketTempIn.println("temp");
						socketTempIn.println(msg);
						String feedBack;
						int result = -1;
						while((feedBack = socketTempOut.readLine()) != null){
							System.out.println("138 feedBack " + feedBack);
							result = Integer.parseInt(feedBack);
							break;
						}
						System.out.println("279 " + feedBack);	
						return result;
					} catch (ConnectException e) {
						System.out.println("Node " + node + " does not exist in system right now");
						return -1;
					}
			}
			
		}
	
	}
	
	// node thread
	private class node extends Thread {
		
		private int identifier;
		private ServerSocket serverSocket;
		private HashSet<Integer> myKeys;
		private HashSet<Integer> replicateKeys;
		private FingerTable fingerTable;
		private Hashtable<Integer, Socket> nodeSocks;				//store the socket from other nodes  --identifier socket
		private Hashtable<Integer, PrintWriter> nodeOuts;
		private Hashtable<Integer, BufferedReader> nodeIns;
		private Socket socketClient;
		private PrintWriter SendToClient;
		private BufferedReader RecFromClient;
		private int predecessor;
		private int successor;
		private boolean hasVisited;
		
		public node(int identifier) throws UnknownHostException, IOException {
		System.out.println("In node ctor");
			this.identifier = identifier;
			serverSocket = new ServerSocket(9000 + identifier);
			new nodeListener(serverSocket).start();
			this.myKeys = new HashSet<Integer>();
			this.replicateKeys = new HashSet<Integer>();
			this.hasVisited = false;
			if(identifier == 0){
				for (int i=0; i<256; i++) {
					myKeys.add(i);
				}
			}
			this.fingerTable = new FingerTable(m, identifier);
			nodeSocks = new Hashtable<Integer, Socket>();
			nodeOuts = new Hashtable<Integer, PrintWriter>();
			nodeIns = new Hashtable<Integer, BufferedReader>();
			
			// connect with node 0
			if (identifier != 0){
				initFingerTable();
				updateOthers();
				/* each node is responsible for the safety and integrity of their own data. 
					Their for each node will ask their successor for keys when it joins the chord
					and ask successor to backup its keys every time its successor is changed
				*/
				askForKeys();		// ask for successor to transfer my keys to me
									// duplicate my predcessor's key
				duplicateMyKeys();	// ask for successor to duplicate my keys
				System.out.println("ACK");
			}
			duplicateMyKeys();
			
			// connect with client
			socketClient = new Socket("localhost", 8999);
			RecFromClient = new BufferedReader(new InputStreamReader(socketClient.getInputStream()));
			SendToClient = new PrintWriter(socketClient.getOutputStream(), true);
			SendToClient.println(identifier);
			SendToClient.println("ACK");						
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
		
		public void setSuccessor(int successor) throws IOException{
//	System.out.println("In setSuccessor identifier " + identifier + " successor " + successor);		
			this.successor = successor;
			if(!myKeys.isEmpty()){
				duplicateMyKeys();
			}
		}
		
		public int getPredecessor(){
			return predecessor;
		}
		
		public void setPredecessor(int predecessor) throws IOException{
			this.predecessor = predecessor;
		}
		
		public void initFingerTable() throws IOException{
//	System.out.println("In initFingerTable");
			int finger0Start = fingerTable.getFingerStart(0);
			String msg = "findSuccessor " + finger0Start;
			int feedBack = RemoteProcedureCall(0, msg);
//	System.out.println("Line 245 " + feedBack);
			fingerTable.setFingerNode(0, feedBack);
			setSuccessor(feedBack);
//	System.out.println("here " + fingerTable.getFingerPredecessor(0));
			setPredecessor(fingerTable.getFingerPredecessor(0));
//	System.out.println("Line 197");
			fingerTable.setFingerPredecessor(0, identifier);
		
			for (int i = 1; i < m; i++){	
//	System.out.println("i" + i);
				int fingerStart = fingerTable.getFingerStart(i);
				int fingerNode = fingerTable.getFingerNode(i-1);
				if (isInRange(fingerStart, identifier, fingerNode, true, false)){
					fingerTable.setFingerNode(i, fingerNode);
				} else {
					msg = "findSuccessor " + fingerStart;
					feedBack = RemoteProcedureCall(0, msg);
					fingerTable.setFingerNode(i, feedBack);
				}
			}
		}
		
		public void updateOthers() throws IOException{
//	System.out.println("In updateOthers");
			for (int i = 0; i < m; i++){
				int p = findPredecessor((int)(identifier - Math.pow(2, i) + 1));				// modified 
//	System.out.println("p " + p);
				String msg = "updateFingerTable " + identifier + " " + i;
				RemoteProcedureCall(p, msg);
			}
		}
		
		public void updateFingerTable(int value, int index) throws IOException{
//	System.out.println("Identifier " + identifier + " In updateFingerTable");
			if(isInRange(value, identifier, fingerTable.getFingerNode(index), false, false)){	// modified
				if(index == 0){
					setSuccessor(value);
				}
				fingerTable.setFingerNode(index, value);
				int p = predecessor;
				String msg = "updateFingerTable " + value + " " + index ;
				RemoteProcedureCall(p, msg);
			}
		}
		
		// ask for successor keys that should be mine
		public void askForKeys() throws IOException{
			RemoteProcedureCall(successor, "transferKeys " + identifier);
		}
		
		// be asked by predecessor to transfer keys to newly added predecessor
		public String transferKeys(int node) throws IOException{
//		System.out.println("In transferKeys " + node);
			StringBuilder result = new StringBuilder();
			HashSet<Integer> myNewKeys = new HashSet<Integer>();
			for(int key: myKeys){
				if(isInRange(key, node, identifier, false, true)){
					myNewKeys.add(key);
				} else {
					result.append(key + " ");
				}
			}
			myKeys = myNewKeys;
			
			duplicateMyKeys();		// ask successor to duplicate my current keys
			
			return result.toString();
		}
		
		// ask for successor to save their keys for backup
		public void duplicateMyKeys() throws IOException{
			String mySerialKey = serializeMyKeys();
			RemoteProcedureCall(successor, "setReplicateKeys " + mySerialKey);
		}		
		
		// set my keys hashset
		public void setMyKeys(String serializedKeys) throws IOException{
			if(!serializedKeys.equals("")){
				myKeys = new HashSet<Integer>();
				String [] tokens = serializedKeys.split(" ");
				for(String token : tokens){
					myKeys.add(Integer.parseInt(token));
				}
				duplicateMyKeys();
			}
		}
		
		// set replicate keys hashset
		public void setReplicateKeys(String serializedKeys){
			if(!serializedKeys.equals("")){
				replicateKeys = new HashSet<Integer>();
				String [] tokens = serializedKeys.split(" ");
				for(String token : tokens){
					replicateKeys.add(Integer.parseInt(token));
				}
			}	
		}
		
		public String serializeMyKeys(){
			StringBuilder result = new StringBuilder();
		     for (int key : myKeys) {
		         result = result.append(key + " ");
		      }
		     return result.toString();
		}
		
		public int findSuccessor(int id) throws IOException {
//	System.out.println("In findSuccessor");
			int nPrime = findPredecessor(id);
			String msg = "getSuccessor";
			return RemoteProcedureCall(nPrime, msg);
			
		}
		
		public int findPredecessor(int id) throws IOException{
	System.out.println("Identifier " + identifier + "In findPredecessor " + id);
			int nPrime = identifier;
			int nPrimeSuccessor = getSuccessor();
			String msg;
			while(!isInRange(id, nPrime, nPrimeSuccessor, false, true)){
				msg = "closestPrecedingFinger " + id;
				nPrime = RemoteProcedureCall(nPrime, msg);
				msg = "getSuccessor";
				nPrimeSuccessor = RemoteProcedureCall(nPrime, msg);
			}
			return nPrime;
		}
		
		public int closestPrecedingFinger(int id){
//	System.out.println("In closestPrecedingFinger");
			for (int i = m-1; i >= 0; i--){
				if(isInRange(fingerTable.getFingerNode(i), identifier, id, false, false)){
					return fingerTable.getFingerNode(i);
				}
			}
			return identifier;
		}
		
		
		public void showAll() throws IOException{
			
			if(identifier == 0 && hasVisited == false){
				show();
				hasVisited = true;
				RemoteProcedureCall(successor, "showAll");
				return;
			} else if (identifier == 0 && hasVisited == true){
				hasVisited = false;
				System.out.println("ACK showAll");
				return;
			} else {
				show();
				RemoteProcedureCall(successor, "showAll");
				return;
			}
	
		}
		
		
		// print finger table of p, keys of p to the console
		public void show(){
			System.out.println("Node " + identifier);
			System.out.println("Predecessor " + predecessor);
			System.out.println("Successor " + successor);
			fingerTable.show();
			System.out.println("Keys in node " + identifier);
			System.out.println(myKeys.toString());
			System.out.println("Replicate Keys in node " + identifier);
			System.out.println(replicateKeys.toString());
		}
		
		public int RemoteProcedureCall(int node, String msg) throws IOException{
//	System.out.println("In RemoteProcedureCall");
//	System.out.println("node " + node + " msg " + msg );
			try (
				Socket socketTemp = new Socket("localhost", 9000 + node);
				PrintWriter socketTempIn = new PrintWriter(socketTemp.getOutputStream(), true);
				BufferedReader socketTempOut = new BufferedReader(new InputStreamReader(socketTemp.getInputStream()))) {
				
				socketTempIn.println("temp");
				socketTempIn.println(msg);
				
				int result = -1;
				
				String feedBack = socketTempOut.readLine();
//					System.out.println("275 feedBack " + feedBack);
				
				if (msg.indexOf("transferKeys") == 0){
					setMyKeys(feedBack);
					return 0;
				} else if (msg.indexOf("setReplicateKeys") == 0){
					return 0;
				}
				result = Integer.parseInt(feedBack);
//				System.out.println("279 " + feedBack);	
				return result;
			}
		}
		
		public boolean isInRange(int value, int lowerBound, int upperBound, boolean leftIsClose, boolean rightIsClose){
//	System.out.println("In isInRange");
//	System.out.println(value + " " + lowerBound + " " + upperBound + " " + leftIsClose + " " + rightIsClose);
			if(lowerBound == upperBound){
				return true;
			} else if (lowerBound < upperBound){
				if(leftIsClose && rightIsClose){
					return (lowerBound <= value) && (value <= upperBound);
				} else if (leftIsClose){
					return (lowerBound <= value) && (value < upperBound);
				} else if (rightIsClose){
					return (lowerBound < value) && (value <= upperBound);
				} else {
					return (lowerBound < value) && (value < upperBound);
				}
				
			} else {
				if (value <= upperBound){
					if (rightIsClose){
						return value <= upperBound;
					} else {
						return value < upperBound;
					}
				} else {
					if(leftIsClose && rightIsClose){
						return ((upperBound <= value) && (lowerBound <= value));
					} else if (leftIsClose){
						return ((upperBound < value) && (lowerBound <= value));
					} else if (rightIsClose){
						return ((upperBound <= value) && (lowerBound < value));
					} else {
						return ((upperBound < value) && (lowerBound < value));
					}
				}
				
			}
		}
		
		
		// node listener for accepting new connetions
		private class nodeListener extends Thread {
			ServerSocket serverSocket;
			BufferedReader serverIn;
			
			public nodeListener(ServerSocket serverSocket){
//	System.out.println("In nodeListener");			
				this.serverSocket = serverSocket;
			}
			
			public void run(){
				Socket hiddenSocket;
				try {
					while(true){
						hiddenSocket = serverSocket.accept();
						BufferedReader reader = new BufferedReader(new InputStreamReader(hiddenSocket.getInputStream()));
						PrintWriter writer = new PrintWriter(hiddenSocket.getOutputStream(), true);				
						String type  = reader.readLine();
						if(type.equals("temp")){
							new TempHandler(hiddenSocket, reader, writer).start();
						}
					}	
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}

			
			// Temp connection handler
			private class TempHandler extends Thread {
				
				Socket socket;
				BufferedReader tempIn;
				PrintWriter tempOut;
				
				public TempHandler(Socket socket, BufferedReader reader, PrintWriter writer) throws IOException {
//				System.out.println("TempHandler ctor");
					this.socket = socket;
					this.tempIn = reader;
					this.tempOut = writer;
//					System.out.print("Server " + identifier + " is in temp connection \n");
					
				}
				
				public void run(){
					String command;
					try {
						while((command = tempIn.readLine()) != null){
							System.out.println("Command " + command);
							String tokens[] = command.split(" ");
							if(tokens[0].equals("findSuccessor")){
								int result = findSuccessor(Integer.parseInt(tokens[1]));
								tempOut.println(result);
							} else if (tokens[0].equals("updateFingerTable")){
								updateFingerTable(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
								tempOut.println("-1");
							} else if (tokens[0].equals("getSuccessor")){
								int result = getSuccessor();
								tempOut.println(result);
							} else if (tokens[0].equals("getPredecessor")){
								int result = getPredecessor();
								tempOut.println(result);
							} else if (tokens[0].equals("setSuccessor")){
								setSuccessor(Integer.parseInt(tokens[1]));
								tempOut.println("-1");
							} else if (tokens[0].equals("setPredecessor")){
								setPredecessor(Integer.parseInt(tokens[1]));
								tempOut.println("-1");
							} else if (tokens[0].equals("closestPrecedingFinger")){
								int result = closestPrecedingFinger(Integer.parseInt(tokens[1]));
								tempOut.println(result);
							} else if (tokens[0].equals("transferKeys")){
								String result = transferKeys(Integer.parseInt(tokens[1]));
								tempOut.println(result);	
							} else if (tokens[0].equals("setReplicateKeys")){
								setReplicateKeys(command.substring(command.indexOf(" ") + 1));
								tempOut.println(0);	
							} else if (tokens[0].equals("show")){
								show();
								tempOut.println(0);	
							} else if (tokens[0].equals("showAll")){
								showAll();
							} else {
								System.out.println("Unknown procedure call");
								tempOut.println("Unknown");
							}
							break;
						}
					} catch (IOException e) {
						return;
					}	
					
				}
			}
			
		}
		
		
		
	}

}
