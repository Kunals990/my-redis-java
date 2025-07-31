package handler.replication;
// In handler/replication/GetAckBroadcaster.java

import handler.SelectorRegistry; // <-- Import this
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey; // <-- Import this
import java.nio.channels.Selector;   // <-- Import this
import java.nio.channels.SocketChannel;
import java.util.List;

public class GetAckBroadcaster implements Runnable {

    @Override
    public void run() {
        Selector selector = SelectorRegistry.getSelector();

        while (true) {
            try {
                Thread.sleep(200); // Wait a bit

                List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
                if (replicas.isEmpty()) {
                    continue;
                }

                String getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                byte[] getAckBytes = getAck.getBytes();

                for (ReplicaInfo replica : replicas) {
                    if (replica.getState() == ReplicaInfo.ReplicaState.ONLINE) {
                        SocketChannel replicaChannel = replica.getChannel();

                        // 1. Add the command to the queue
                        replica.getWriteQueue().add(ByteBuffer.wrap(getAckBytes));

                        // 2. Signal the main selector that this channel has data to write
                        SelectionKey key = replicaChannel.keyFor(selector);
                        if (key != null && key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                    }
                }
                // After queueing all ACKs, wake up the selector
                selector.wakeup();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}