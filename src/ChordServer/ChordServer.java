package ChordServer;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ChordServer {

	AtomicInteger counter;
	public AtomicInteger isBusy;
	public ArrayList<Integer> joinedNode;
	public Random randNum;
	
	private static final int FINAL_TABLE_SIZE = 8;
	private int minDelay;
	private int maxDelay;

	// Ctor of ChordServer.ChordServer
	public ChordServer() {	
		counter = new AtomicInteger();
		isBusy = new AtomicInteger();
		joinedNode = new ArrayList<Integer>();
		joinedNode.add(0);
		readConfig("config");
	}
	
	private void readConfig(String config){
    	// Read and store process info from config file
    	try (BufferedReader configReader = new BufferedReader(new FileReader(config))) {

    		String currLine = configReader.readLine();		
    		String[] delayLimits = currLine.split(" ", 2);
			minDelay = Integer.parseInt(delayLimits[0]);
			maxDelay = Integer.parseInt(delayLimits[1]);
			currLine = configReader.readLine();	
			
    	} catch (IOException e) {
			e.printStackTrace();
		}

    	// print out results from the configuration file.
    	System.out.println("Finish reading from the configuration file");
		System.out.println("Bounds of delay in milliSec: " + minDelay + " " + maxDelay);
    }
	
	public void run() throws IOException {
		new ConsoleHandler().start();
		new Node(0, FINAL_TABLE_SIZE, maxDelay, minDelay, counter).start();
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
						new Node(Integer.parseInt(tokens[1]), FINAL_TABLE_SIZE, maxDelay, minDelay, counter).start();
					} else if(userInput.equals("show all")){
						RemoteProcedureCall(0, "showAll");
					} else if(tokens[0].equals("show")){
						int p = Integer.parseInt(tokens[1]);
						int res = RemoteProcedureCall(p, "show");
						if (res == -20) {
							System.out.println("ChordServer.Node " + p + " does not exit");
						}
						else {
							System.out.println("ACK");
							synchronized(this) {
								notify();
							}
						}
					} else if(tokens[0].equals("find")) {
						int p = Integer.parseInt(tokens[1]);
						int k = Integer.parseInt(tokens[2]);
						String msg = "findSuccessor " + k;
						int res = RemoteProcedureCall(p, msg);
						if (res != -20)
							System.out.println("ChordServer.Node " + res + " contains " + k);
						else 
							System.out.println("ChordServer.Node " + p + " does not exit");
					} else if(tokens[0].equals("crash")){
						int p = Integer.parseInt(tokens[1]);
						String msg = "crash " + p;
						int res = RemoteProcedureCall(p, msg);
						if (res == -20) {
							System.out.println("ChordServer.Node " + p + " does not exit");
						}
					} else if(tokens[0].equals("test1")){
						// test1 10 123456
						countJoin(Integer.parseInt(tokens[1]), Long.valueOf(tokens[2]).longValue());
					} else if(tokens[0].equals("test2")){
						//test2 10
						countFind(Integer.parseInt(tokens[1]), Long.valueOf(tokens[2]).longValue());
					} else {
						System.out.println("Invalid input");
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void countJoin(int nodeNum, long seed) throws UnknownHostException, IOException{
			int curNumNode = 0;
			counter.set(0);
			randNum = new Random();
		    randNum.setSeed(seed);
		    int nextNode;
		    
			while(curNumNode < nodeNum){
				while(true){
					nextNode = Math.abs(randNum.nextInt() % 256);
					if(!joinedNode.contains(nextNode)){
						break;
					}
				}
				joinedNode.add(nextNode);
				
				Node newNode = new Node(nextNode, FINAL_TABLE_SIZE, maxDelay, minDelay, counter);
				newNode.start();
				
			    synchronized(newNode){
		            try{
		                newNode.wait();
		            }catch(InterruptedException e){
		                e.printStackTrace();
		            }
		        }
				
				curNumNode++;
			}
			System.out.println("msgNum: "+ counter.get());
		}
		
		public void countFind(int findNum, long seed) throws IOException{
			int curFindNode = 0;
			counter.set(0);
			randNum = new Random();
		    randNum.setSeed(seed);
			while(curFindNode < findNum){
				Collections.shuffle(joinedNode);
				int askNode = joinedNode.get(0);
				int askKey = Math.abs(randNum.nextInt() % 256);
				System.out.println("ChordServer.Node " + askNode + " find key " + askKey);
				String msg = "findSuccessor " + askKey;
				RemoteProcedureCall(askNode, msg);	
				curFindNode++;
			}
			System.out.println("msgNum: "+ counter.get());
		}
		
		public int RemoteProcedureCall(int node, String msg) throws IOException{
			counter.incrementAndGet();
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
						result = Integer.parseInt(feedBack);
						break;
					}
					return result;
				} catch (ConnectException e) {
					return -20;
				}
		}		
	}

	public static void main(String[] args) throws IOException {
		ChordServer server = new ChordServer();
		server.run();
	}
	
}
