package ChordServer;

import ChordServer.FingerTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

// ChordServer.Node thread
public class Node extends Thread {

	private int fingerTableSize;
	private int identifier;
	private ServerSocket serverSocket;
	private HashSet<Integer> myKeys;
	private HashSet<Integer> replicateKeys;
	private FingerTable fingerTable;
	private int predecessor;
	private int successor;
	private int nextSuccessor;
	private boolean hasVisited;
	private Thread listener;

	private AtomicInteger counter;

	private int maxDelay;
	private int minDelay;

	public Node(int identifier, int m, int maxDelay, int minDelay, AtomicInteger counter) throws UnknownHostException, IOException {
		this.maxDelay = maxDelay;
		this.minDelay = minDelay;
		this.counter = counter;
		this.fingerTableSize = m;
		this.identifier = identifier;
		serverSocket = new ServerSocket(9000 + identifier);

		listener = new Thread(new nodeListener(serverSocket));
		listener.start();

		this.myKeys = new HashSet<Integer>();
		this.replicateKeys = new HashSet<Integer>();
		this.hasVisited = false;
		if(identifier == 0){
			for (int i=0; i<256; i++) {
				myKeys.add(i);
			}
		}
		this.fingerTable = new FingerTable(m, identifier, maxDelay, minDelay);

		// connect with ChordServer.Node 0
		if (identifier != 0){
			initFingerTable();
			updateOthers();
				/* each ChordServer.Node is responsible for the safety and integrity of their own data.
					Their for each ChordServer.Node will ask their successor for keys when it joins the chord
					and ask successor to backup its keys every time its successor is changed
				*/
			askForKeys();		// ask for successor to transfer my keys to me
			// duplicate my predcessor's key
			duplicateMyKeys();	// ask for successor to duplicate my keys
			System.out.println("ACK");
		}
		duplicateMyKeys();

	}

	public void run() {

		synchronized(this) {
			notify();
		}

		heartBeatMonitor heartBeatMonitor = new heartBeatMonitor();
		heartBeatMonitor.start();

		System.out.println("ChordServer.Node " + identifier + " is running ...");

		try {
			listener.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		heartBeatMonitor.terminate();

		System.out.println("ChordServer.Node " + identifier + " ending...");

	}

	private class heartBeatMonitor extends Thread{
		Timer timer;
		int delay = 2;
		int period = 7;

		public void terminate() {
			timer.cancel();
			System.out.println("heartBeatMonitor of ChordServer.Node " + identifier + " ending...");
		}

		public heartBeatMonitor() {
			timer = new Timer();
		}

		public void run() {
			timer.scheduleAtFixedRate(new TimerTask() {
				public void run() {
					try {
						int res = RemoteProcedureCall(successor, "isAlive");
						if (res >= 0) {
							nextSuccessor = res;
						}
						else if (res == -20) {
							System.out.println("ChordServer.Node " + successor + " failed");
							int oldSuccessor = successor;
							successor = nextSuccessor;
							mergeYourKeysAndUpdatePre();
							duplicateMyKeys();
							fixFinger(oldSuccessor, successor, identifier);
						}
						else System.out.println("invalid liveness state");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}, delay*1000, period*1000);
		}

	}

	public void fixFinger(int oldNode, int newNode, int start) throws IOException {
		for (int i = 0; i< fingerTableSize; i++) {
			if(fingerTable.getFingerNode(i) == oldNode) {
				fingerTable.setFingerNode(i, newNode);
			}
		}
		if (predecessor != start)
			RemoteProcedureCall(predecessor, "fix " + oldNode + " to " + newNode + " from " + start);
		else {
			System.out.println("Fixing ended");
			System.out.println("ACK");
		}

	}

	public int getSuccessor(){
		return successor;
	}

	public void setSuccessor(int successor) throws IOException{
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
		int finger0Start = fingerTable.getFingerStart(0);
		String msg = "findSuccessor " + finger0Start;
		int feedBack = RemoteProcedureCall(0, msg);
		fingerTable.setFingerNode(0, feedBack);
		setSuccessor(feedBack);
		setPredecessor(fingerTable.getFingerPredecessor(0));
		fingerTable.setFingerPredecessor(0, identifier);

		for (int i = 1; i < fingerTableSize; i++){
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
		for (int i = 0; i < fingerTableSize; i++){
			int p = findPredecessor((int)(identifier - Math.pow(2, i) + 1 + 256) % 256);				// modified
			String msg = "updateFingerTable " + identifier + " " + i;
			RemoteProcedureCall(p, msg);
		}
	}

	public void updateFingerTable(int value, int index) throws IOException{
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

	public void mergeYourKeysAndUpdatePre() throws IOException {
		RemoteProcedureCall(successor, "mergeKeys " + identifier);
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
		int nPrime = findPredecessor(id);

		String msg = "getSuccessor";
		return RemoteProcedureCall(nPrime, msg);

	}

	public int findPredecessor(int id) throws IOException{
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
		for (int i = fingerTableSize -1; i >= 0; i--){
			if(isInRange(fingerTable.getFingerNode(i), identifier, id, false, false)){
				return fingerTable.getFingerNode(i);
			}
		}
		System.out.println("identifier " + identifier);
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
			System.out.println("ACK");
			return;
		} else {
			show();
			RemoteProcedureCall(successor, "showAll");
			return;
		}

	}


	// print finger table of p, keys of p to the console
	public void show(){
		System.out.println("ChordServer.Node " + identifier);
		System.out.println("Predecessor " + predecessor);
		System.out.println("Successor " + successor);
		System.out.println("nextSuccessor " + nextSuccessor);
		fingerTable.show();
		System.out.println("Keys in ChordServer.Node " + identifier);
		System.out.println(myKeys.toString());
		System.out.println("Replicate Keys in ChordServer.Node " + identifier);
		System.out.println(replicateKeys.toString());
	}

	void mergeKeysAndUpdatePre(String newPredecessor) throws IOException {
		myKeys.addAll(replicateKeys);
		replicateKeys.clear();
		predecessor = Integer.parseInt(newPredecessor);
		duplicateMyKeys();
	}

	public int RemoteProcedureCall(int node, String msg) throws IOException{
		if(!msg.equals("isAlive")){

		}

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

			if (msg.indexOf("transferKeys") == 0){
				setMyKeys(feedBack);
				return 0;
			} else if (msg.indexOf("setReplicateKeys") == 0){
				return 0;
			} else if(feedBack == null) {
				return -20;
			}

			result = Integer.parseInt(feedBack);
			return result;
		} catch (ConnectException e) {
			return -20; // the remote socket is closed
		}
	}

	public boolean isInRange(int value, int lowerBound, int upperBound, boolean leftIsClose, boolean rightIsClose){
		if(lowerBound == upperBound && value != upperBound){
			return true;
		} else if (lowerBound == upperBound && value == upperBound){
			return false;
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

	// ChordServer.Node listener for accepting new connetions
	private class nodeListener extends Thread {
		ServerSocket serverSocket;
		BufferedReader serverIn;

		public nodeListener(ServerSocket serverSocket){
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

						TempHandler th = new TempHandler(hiddenSocket, reader, writer);
						th.start();
						synchronized(th) {
							th.wait();
							if (th.listenerCrash) {
								break;
							}
						}
					}
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}

			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("listener of ChordServer.Node " + identifier + " ending...");
		}


		// Temp connection handler
		private class TempHandler extends Thread {

			Socket socket;
			BufferedReader tempIn;
			PrintWriter tempOut;
			boolean listenerCrash;

			public TempHandler(Socket socket, BufferedReader reader, PrintWriter writer) throws IOException {
				this.socket = socket;
				this.tempIn = reader;
				this.tempOut = writer;
				listenerCrash = false;
			}

			void crashListener() {
				listenerCrash = true;
			}

			public void run(){
				String command;
				try {
					while((command = tempIn.readLine()) != null){
						String tokens[] = command.split(" ");

						if(tokens[0].equals("findSuccessor")){
							synchronized(this) {
								notify();
							}
							int result = findSuccessor(Integer.parseInt(tokens[1]));
							tempOut.println(result);
						} else if (tokens[0].equals("updateFingerTable")){
							synchronized(this) {
								notify();
							}
							updateFingerTable(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
							tempOut.println("-1");
						} else if (tokens[0].equals("getSuccessor")){
							synchronized(this) {
								notify();
							}
							int result = getSuccessor();
							tempOut.println(result);
						} else if (tokens[0].equals("getPredecessor")){
							synchronized(this) {
								notify();
							}
							int result = getPredecessor();
							tempOut.println(result);
						} else if (tokens[0].equals("setSuccessor")){
							synchronized(this) {
								notify();
							}
							setSuccessor(Integer.parseInt(tokens[1]));
							tempOut.println("-1");
						} else if (tokens[0].equals("setPredecessor")){
							synchronized(this) {
								notify();
							}
							setPredecessor(Integer.parseInt(tokens[1]));
							tempOut.println("-1");
						} else if (tokens[0].equals("closestPrecedingFinger")){
							synchronized(this) {
								notify();
							}
							int result = closestPrecedingFinger(Integer.parseInt(tokens[1]));
							tempOut.println(result);
						} else if (tokens[0].equals("transferKeys")){
							synchronized(this) {
								notify();
							}
							String result = transferKeys(Integer.parseInt(tokens[1]));
							tempOut.println(result);
						} else if (tokens[0].equals("setReplicateKeys")){
							synchronized(this) {
								notify();
							}
							setReplicateKeys(command.substring(command.indexOf(" ") + 1));
							tempOut.println(0);
						} else if (tokens[0].equals("show")){
							synchronized(this) {
								notify();
							}
							show();
							tempOut.println(0);
						} else if (tokens[0].equals("showAll")){
							synchronized(this) {
								notify();
							}
							showAll();
							tempOut.println(0);
						} else if (tokens[0].equals("crash")) {
							crashListener();
							synchronized(this) {
								notify();
							}
							tempOut.println("-2");
						} else if (tokens[0].equals("isAlive")) {
							synchronized(this) {
								notify();
							}
							tempOut.println(successor);
						} else if (tokens[0].equals("fix")) {
							synchronized(this) {
								notify();
							}
							fixFinger(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[3]), Integer.parseInt(tokens[5]));
							tempOut.println("-30");
						} else if (tokens[0].equals("mergeKeys")) {
							synchronized(this) {
								notify();
							}
							mergeKeysAndUpdatePre(tokens[1]);
							tempOut.println("-30");
						} else {
							synchronized(this) {
								notify();
							}
							System.out.println("Unknown procedure call");
							tempOut.println("-3");
						}
						break;
					}
				} catch (IOException e) {
					return;
				} finally {
					try {
						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}