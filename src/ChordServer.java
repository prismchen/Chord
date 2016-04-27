import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;

public class ChordServer {

	private int m = 8;
	private int minDelay;
	private int maxDelay;
	
	// Ctor of ChordServer
	public ChordServer() {	
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
	
	// run()
	public void run() throws IOException {
		new ConsoleHandler().start();
		new node(0, m).start();
	}
	
	public static void main(String[] args) throws IOException {	
		ChordServer server = new ChordServer();
		server.run();
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
						new node(Integer.parseInt(tokens[1]), m).start();
					} else if(userInput.equals("show all")){
						RemoteProcedureCall(0, "showAll");
//						System.out.println("ACK");
					} else if(tokens[0].equals("show")){
						RemoteProcedureCall(Integer.parseInt(tokens[1]), "show");
						System.out.println("ACK");
					} else if(tokens[0].equals("find")) {
						int p = Integer.parseInt(tokens[1]);
						int k = Integer.parseInt(tokens[2]);
						String msg = "findSuccessor " + k;
						int res = RemoteProcedureCall(p, msg);
						if (res != -20)
							System.out.println("node " + res + " contains " + k);
						else 
							System.out.println("node " + p + " does not exit");
					} else if(tokens[0].equals("crash")){
						int p = Integer.parseInt(tokens[1]);
						String msg = "crash " + p;
						int res = RemoteProcedureCall(p, msg);
						if (res == -20) {
							System.out.println("node " + p + " does not exit");
						}
					} else {
						System.out.println("Invalid input");
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public int RemoteProcedureCall(int node, String msg) throws IOException{
//			System.out.println("In client RemoteProcedureCall");
//			System.out.println("node " + node + " msg " + msg );
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
//						System.out.println("138 feedBack " + feedBack);
						result = Integer.parseInt(feedBack);
						break;
					}
//					System.out.println("279 " + feedBack);	
					return result;
				} catch (ConnectException e) {
					return -20;
				}
		}		
	}
}
