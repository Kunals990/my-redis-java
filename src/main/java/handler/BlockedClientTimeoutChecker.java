package handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class BlockedClientTimeoutChecker extends Thread {

    @Override
    public void run() {
        BlockingClientManager manager = BlockingClientManager.getInstance();

        while (true) {
            try {
                List<BlockedClient> expired = manager.getExpiredClients();

                for (BlockedClient client : expired) {
                    String nullResponse = "$-1\r\n"; // RESP null bulk string
                    client.channel.write(ByteBuffer.wrap(nullResponse.getBytes()));
                }

                Thread.sleep(100); // check every 100ms
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
