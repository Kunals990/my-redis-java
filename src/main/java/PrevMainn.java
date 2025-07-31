import config.ServerConfig;
import handler.BlockedClientTimeoutChecker;
import handler.CommandHandler;
import handler.SelectorRegistry;
import handler.WaitClientTimeoutChecker;
import handler.replication.*;
import protocols.RESPParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class PrevMainn {

    private static final Map<SocketChannel, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        int port = 6379;
        String masterHost = null;
        int masterPort = -1;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
                i++;
            } else if (args[i].equals("--replicaof") && i + 1 < args.length) {
                String[] parts = args[i + 1].split(" ");
                masterHost = parts[0];
                masterPort = Integer.parseInt(parts[1]);
                i++;
            }
        }

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        Selector selector = Selector.open();
        SelectorRegistry.setSelector(selector);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        if (masterHost != null && masterPort != -1) {
            ServerConfig.setRole("slave");
            ServerConfig.setMaster_host(masterHost);
            ServerConfig.setMaster_port(String.valueOf(masterPort));
            ReplicaConnectionHandler replicaHandler = new ReplicaConnectionHandler(masterHost, masterPort, port);
            Executors.newSingleThreadExecutor().submit(replicaHandler);
        } else {
            ServerConfig.setRole("master");
        }

        if (ServerConfig.isMaster()) {
//            new Thread(new GetAckBroadcaster()).start();
            new Thread(new WaitClientTimeoutChecker()).start();
        }
        new Thread(new BlockedClientTimeoutChecker()).start();

        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();

            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                if (!key.isValid()) {
                    continue;
                }

                try {
                    if (key.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        SocketChannel clientChannel = server.accept();
                        if (clientChannel != null) {
                            clientChannel.configureBlocking(false);
                            clientChannel.register(selector, SelectionKey.OP_READ);
                            clientBuffers.put(clientChannel, ByteBuffer.allocate(1024));
                            System.out.println("Accepted new client: " + clientChannel.getRemoteAddress());
                        }
                    }
                    if (key.isReadable()) {
                        handleReadableKey(key);
                    }
                    if (key.isWritable()) {
                        handleWritableKey(key);
                    }
                } catch (IOException e) {
                    System.err.println("IOException, closing connection: " + e.getMessage());
                    cleanupClient(key);
                }
            }
        }
    }

    private static void handleReadableKey(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = clientBuffers.get(clientChannel);
        if (buffer == null) return;

        int bytesRead = clientChannel.read(buffer);
        if (bytesRead == -1) {
            System.out.println("Client disconnected cleanly.");
            cleanupClient(key);
            return;
        }

        buffer.flip();
        while (buffer.hasRemaining()) {
            RESPParser parser = new RESPParser(buffer);
            List<String> commandParts = parser.parse();

            if (commandParts != null) {
                String response = CommandHandler.handle(commandParts, clientChannel);
                if (response != null) {
                    clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                }
            } else {
                break;
            }
        }
        buffer.compact();
    }

    private static void handleWritableKey(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(channel);

        if (replica != null) {
            ByteBuffer buffer;
            while ((buffer = replica.getWriteQueue().peek()) != null) {
                channel.write(buffer);
                if (buffer.hasRemaining()) {
                    return;
                }
                replica.getWriteQueue().poll();
            }
            if (replica.getWriteQueue().isEmpty()) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private static void cleanupClient(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        try {
            clientBuffers.remove(clientChannel);
            key.cancel();
            clientChannel.close();
        } catch (IOException e) {
            System.err.println("Error during client cleanup: " + e.getMessage());
        }
    }
}