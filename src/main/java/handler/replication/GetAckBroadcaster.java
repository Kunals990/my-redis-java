package handler.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class GetAckBroadcaster implements Runnable{
    @Override
    public void run() {
        while(true){
            try {

                List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
                String getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

                for(ReplicaInfo replica : replicas){
                    SocketChannel replicaChannel = replica.getChannel();
                    try {
                        replicaChannel.write(ByteBuffer.wrap(getAck.getBytes()));
                    } catch (IOException e) {
                        System.err.println("Failed to send GETACK to " + replica.getListeningPort());
                    }
                }
                Thread.sleep(100);
            }catch (InterruptedException e){
                e.printStackTrace();
                break;
            }
        }
    }
}
