import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {
	
	Socket socket;
	
	PrintWriter out;
	BufferedReader in;
	
	// Ctor
	public Client() {
			
		try {
			socket = new Socket("localhost", 9000);
			System.out.print("Connected to server ...\n");
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new PrintWriter(socket.getOutputStream(), true);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		
		new InputHandler().start();
		String recvMsg = "";
		
		try {
			while ((recvMsg = in.readLine()) != null) { 
				// print to stdOut
				System.out.println(recvMsg);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	// Main
	public static void main(String[] args) {
		Client c = new Client();
		c.run();
	}
	
	// 
	private class InputHandler extends Thread {
		BufferedReader stdIn;
		
		public InputHandler() {
			stdIn = new BufferedReader(new InputStreamReader(System.in));
		}
		
		public void run() {
			
			System.out.println("Ready for inputs: ");
			String userInput = null;
			
			try {
				while ((userInput = stdIn.readLine()) != null) {
					out.println(userInput);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}
}
