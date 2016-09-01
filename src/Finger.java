import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by xiaochen on 9/1/16.
 */
public class Finger {
    public int start;
    public int node;

    private int minDelay;
    private int maxDelay;

    public Finger(int identifier, int fingerIndex, int minDelay, int maxDelay) {
        start = (int) (identifier + Math.pow(2, fingerIndex));
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
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

    int RemoteProcedureCall(int node, String msg) throws IOException{

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

