package handler;

import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class WaitClientTimeoutChecker extends Thread {

    @Override
    public void run() {
        while (true) {
            for (WaitClient client : WaitClientManager.getAll()) {
                int acked = 0;
                for (ReplicaInfo replica : ReplicaManager.getReplicas()) {
                    if (replica.getReplicationOffset() >= client.expectedOffset) {
                        acked++;
                    }
                }

                if (acked >= client.requiredReplicas ||
                        System.currentTimeMillis() - client.startTime >= client.timeoutMillis) {

                    try {
                        client.channel.write(ByteBuffer.wrap((":" + acked + "\r\n").getBytes()));
                    } catch (IOException e) {
                        System.out.println("Error writing to WAIT client: " + e.getMessage());
                    }

                    WaitClientManager.remove(client);
                }
            }

            try {
                Thread.sleep(100); // Tune this as needed
            } catch (InterruptedException ignored) {}
        }
    }


}

