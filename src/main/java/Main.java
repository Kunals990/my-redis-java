import config.ServerConfig;
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

public class Main {
    private static final Map<SocketChannel, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        // ... Argument parsing is the same ...
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

        // Assume args are parsed here
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        Selector selector = Selector.open();
        SelectorRegistry.setSelector(selector);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        if (masterHost != null && masterPort != -1) {
            ServerConfig.setRole("slave");
            // Initiate the non-blocking connection to the master
            new MasterLink(masterHost, masterPort, port, selector);
        } else {
            ServerConfig.setRole("master");
        }

        // Background threads for master functionality
        if (ServerConfig.isMaster()) {
            new Thread(new WaitClientTimeoutChecker()).start();
        }

        // --- THE UNIFIED EVENT LOOP ---
        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();

            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();
                if (!key.isValid()) continue;

                try {
                    if (key.isAcceptable()) {
                        // A new client is connecting to us
                        handleAcceptable(key, selector);
                    } else if (key.isConnectable()) {
                        // Our connection to the master is ready
                        ((MasterLink) key.attachment()).handleConnect(key);
                    } else if (key.isReadable()) {
                        Object attachment = key.attachment();
                        if (attachment instanceof MasterLink) {
                            // Data is coming from the master
                            ((MasterLink) attachment).handleRead(key);
                        } else {
                            // Data is coming from a normal client
                            handleClientRead(key);
                        }
                    } else if (key.isWritable()) {
                        Object attachment = key.attachment();
                        if (attachment instanceof MasterLink) {
                            // We are ready to send an ACK to the master
                            ((MasterLink) attachment).handleWrite(key);
                        } else {
                            // We are ready to send propagated data to a replica
                            handleReplicaWrite(key);
                        }
                    }
                } catch (IOException e) {
                    cleanupConnection(key);
                }
            }
        }
    }

    private static void handleAcceptable(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
        clientBuffers.put(clientChannel, ByteBuffer.allocate(1024));
    }

    private static void handleClientRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = clientBuffers.get(clientChannel);
        int bytesRead = clientChannel.read(buffer);
        if (bytesRead == -1) {
            cleanupConnection(key);
            return;
        }
        RESPParser parser = new RESPParser(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            Object parsed = parser.parse();
            if (parsed instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> commandParts = (List<String>) parsed;
                String response = CommandHandler.handle(commandParts, clientChannel);
                if (response != null) {
                    clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                }
            } else {
                // Either incomplete or a simple string/integer—stop parsing
                break;
            }
        }
        buffer.compact();
    }

    private static void handleReplicaWrite(SelectionKey key) throws IOException {
        // This is for when WE are the master, writing to our replicas
        SocketChannel channel = (SocketChannel) key.channel();
        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(channel);
        if (replica != null) {
            ByteBuffer buffer;
            while ((buffer = replica.getWriteQueue().peek()) != null) {
                channel.write(buffer);
                if (buffer.hasRemaining()) return;
                replica.getWriteQueue().poll();
            }
            if (replica.getWriteQueue().isEmpty()) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private static void cleanupConnection(SelectionKey key) {
        try {
            SocketChannel channel = (SocketChannel) key.channel();
            clientBuffers.remove(channel);
            key.cancel();
            channel.close();
        } catch (Exception e) {
            // Ignore
        }
    }
}