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

public class ChordServer {
	
	private ServerSocket listener;
	
	private final static int m = 8;
	private int minDelay;
	private int maxDelay;
	private int initPort;
	private HashSet<Integer> allKeys;
	
	private HashSet<Integer> runnningNodes;
	private Hashtable<Integer, Socket> nodeSocks;
	private Hashtable<Integer, PrintWriter> nodeOuts;
	private Hashtable<Integer, BufferedReader> nodeIns;
	
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
		allKeys = new HashSet<Integer>();
		for (int i=0; i<256; i++) {
			allKeys.add(i);
		}
	}
	
	// run()
	public void run() throws IOException {
		
		new node(0, allKeys).start();
		
		try {
			new ClientHandler(listener.accept()).start();
        } finally {
            listener.close(); 
        }
	}
	
	
	public static void main(String[] args) throws IOException {
		
		ChordServer cs = new ChordServer();
		cs.run();
		
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
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	// node thread
	private class node extends Thread {
		
		private int identifier;
		private HashSet<Integer> keys;
		private FingerTable fingerTable;
		
		public node(int identifier, HashSet<Integer> initKeys) throws UnknownHostException, IOException {
			this.identifier = identifier;
			this.keys = initKeys;
			this.fingerTable = new FingerTable(m);
			runnningNodes.add(identifier);
			nodeSocks.put(identifier, new Socket("localhost", 9000 + identifier));
			nodeIns.put(identifier, new BufferedReader(new InputStreamReader(nodeSocks.get(identifier).getInputStream())));
			nodeOuts.put(identifier, new PrintWriter(nodeSocks.get(identifier).getOutputStream(), true));
		}
		
		public void run() {
			System.out.println("node " + identifier + " is running ...");
			while (true) {
				
			}
		}
	}

}
