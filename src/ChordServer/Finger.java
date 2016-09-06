package ChordServer;

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
    public int mStart;
    public int mNode;

    private int mMinDelay;
    private int mMaxDelay;

    public Finger(int identifier, int fingerIndex, int minDelay, int maxDelay) {
        mStart = (int) (identifier + Math.pow(2, fingerIndex));
        this.mMinDelay = minDelay;
        this.mMaxDelay = maxDelay;
    }

    public int getterStart(){
        return mStart;
    }

    public void setterStart(int Start){
        mStart = Start;
    }

    public int getterNode(){
        return mNode;
    }

    public void setterNode(int Node){
        mNode = Node;
    }

    public int getPredecessor() throws NumberFormatException, IOException{
        return RemoteProcedureCall(mNode, "getPredecessor");
    }

    public int setPredecessor(int value) throws NumberFormatException, IOException{
        return RemoteProcedureCall(mNode, "setPredecessor " + value);
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
            }, (long) (mMinDelay + (long)(Math.random() * ((mMaxDelay - mMinDelay) + 1))) );

            int result = -1;

            String feedBack = socketTempOut.readLine();


            result = Integer.parseInt(feedBack);
            return result;
        } catch (ConnectException e) {
            return -20; // the remote socket is closed
        }
    }
}

