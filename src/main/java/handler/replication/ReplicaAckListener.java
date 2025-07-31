package handler.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class ReplicaAckListener implements Runnable {
    private static ReplicaAckListener instance;
    private static final Object lock = new Object();
    private final Selector selector;

    // Make the constructor private
    private ReplicaAckListener() throws IOException {
        this.selector = Selector.open();
    }

    // Public method to get the singleton instance
    public static ReplicaAckListener getInstance() throws IOException {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new ReplicaAckListener();
                }
            }
        }
        return instance;
    }

    // Public method to register a new replica's channel
    public void registerNewReplica(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        // We must wake up the selector to register the new key
        selector.wakeup();
        channel.register(selector, SelectionKey.OP_READ);
    }

    @Override
    public void run() {
        // The run loop logic itself is mostly okay, but let's make the parser more robust
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        while (true) {
            try {
                // Add any newly connected replicas to the selector
                // The select() call will now block until there's activity or wakeup() is called
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(channel);

                        buffer.clear();
                        int bytesRead = channel.read(buffer);
                        if (bytesRead <= 0) {
                            if(bytesRead == -1) {
                                key.cancel();
                                ReplicaManager.removeReplica(channel);
                            }
                            continue;
                        }

                        // ... Your ACK parsing logic ...
                        // For now, this part is okay for the next step.
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}