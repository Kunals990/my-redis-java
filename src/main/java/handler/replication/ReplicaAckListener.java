package handler.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class ReplicaAckListener implements Runnable {
    private final Selector selector;

    public ReplicaAckListener() throws IOException {
        this.selector = Selector.open();
        for (ReplicaInfo replica : ReplicaManager.getReplicas()) {
            SocketChannel channel = replica.getChannel();
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ, replica);
        }
    }

    @Override
    public void run() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        while (true) {
            try {
                selector.select(100); // block up to 100ms
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        ReplicaInfo replica = (ReplicaInfo) key.attachment();

                        buffer.clear();
                        int bytesRead = channel.read(buffer);
                        if (bytesRead == -1) {
                            key.cancel();
                            channel.close();
                            continue;
                        }

                        buffer.flip();
                        String response = new String(buffer.array(), 0, bytesRead);

                        // naive ACK parser (customize based on RESP lib you're using)
                        if (response.contains("REPLCONF") && response.contains("ACK")) {
                            String[] parts = response.split("\r\n");
                            for (int i = 0; i < parts.length; i++) {
                                if ("ACK".equalsIgnoreCase(parts[i])) {
                                    long offset = Long.parseLong(parts[i + 1]);
                                    replica.setReplicationOffset(offset);
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

