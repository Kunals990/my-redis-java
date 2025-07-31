package handler.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

// In handler/replication/GetAckBroadcaster.java

public class GetAckBroadcaster implements Runnable {
    @Override
    public void run() {
        while (true) {
            try {
                List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
                if (!replicas.isEmpty()) {
                    String getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                    for (ReplicaInfo replica : replicas) {
                        // ONLY send GETACK to replicas that are fully online
                        if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                            try {
                                replica.getChannel().write(ByteBuffer.wrap(getAck.getBytes()));
                            } catch (IOException e) {
                                System.err.println("Failed to send GETACK to " + replica.getListeningPort() + ", removing replica.");
                                ReplicaManager.removeReplica(replica.getChannel());
                            }
                        }
                    }
                }
                Thread.sleep(200); // Increased sleep time slightly
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupted status
                e.printStackTrace();
                break;
            }
        }
    }
}