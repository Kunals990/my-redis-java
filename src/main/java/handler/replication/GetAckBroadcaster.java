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

    // Define a grace period in milliseconds. 500ms is a safe value.
    private static final long GRACE_PERIOD_MS = 500;

    @Override
    public void run() {
        Selector selector = SelectorRegistry.getSelector();

        while (true) {
            try {
                // The sleep interval can be shorter now.
                Thread.sleep(100);

                List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
                if (replicas.isEmpty()) {
                    continue;
                }

                String getAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                byte[] getAckBytes = getAck.getBytes();

                for (ReplicaInfo replica : replicas) {
                    if (replica.getState() != ReplicaInfo.ReplicaState.ONLINE) {
                        continue;
                    }

                    // Check if the grace period has passed since the last activity
                    long timeSinceLastActivity = System.currentTimeMillis() - replica.getLastActivityTime();

                    if (timeSinceLastActivity > GRACE_PERIOD_MS) {
                        SocketChannel replicaChannel = replica.getChannel();

                        // Queue the GETACK command
                        replica.getWriteQueue().add(ByteBuffer.wrap(getAckBytes));


                        // Signal the main selector
                        SelectionKey key = replicaChannel.keyFor(selector);
                        if (key != null && key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                    }
                }
                // Wake up the selector if any work was queued
                selector.wakeup();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("GetAckBroadcaster interrupted.");
                break;
            }
        }
    }
}