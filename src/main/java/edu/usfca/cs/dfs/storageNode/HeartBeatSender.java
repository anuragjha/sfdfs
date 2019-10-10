package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.Client;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.util.TimerTask;

public class HeartBeatSender extends TimerTask {

    private StorageMessages.StorageMessageWrapper heartBeat;
    private String connectingAddress;
    private int connectingPort;
    //private StorageNodeClient client;

    public HeartBeatSender(StorageNodeClient client, String connectingAddress, int connectingPort, StorageMessages.StorageMessageWrapper heartBeat) {
        //this.client = client;
        this.connectingAddress = connectingAddress;
        this.connectingPort = connectingPort;
        this.heartBeat = heartBeat;
    }

    @Override
    public void run() {
        this.sendHeartBeat();
    }

    public void sendHeartBeat() {

        try {
            new Client().runClient(false, "storage", connectingAddress, connectingPort, heartBeat);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//        try {
//            this.client.startClient(this.connectingAddress, this.connectingPort, this.heartBeat);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }


}
