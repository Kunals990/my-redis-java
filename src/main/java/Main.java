import config.ServerConfig;
import handler.BlockedClientTimeoutChecker;
import handler.CommandHandler;
import handler.SelectorRegistry;
import handler.WaitClientTimeoutChecker;
import handler.replication.*;
import protocols.RESPParser; // Ensure this is your new ByteBuffer-based parser

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

public class Main {

    // A map to store a buffer for each client connection. This is crucial for handling partial reads.
    private static final Map<SocketChannel, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        int port = 6379;
        String masterHost = null;
        int masterPort = -1;

        // --- Argument Parsing (your existing code is correct) ---
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

        // --- Server Setup ---
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        Selector selector = Selector.open();
        SelectorRegistry.setSelector(selector);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        // --- Role Determination and Background Threads (your existing code is correct) ---
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
            new Thread(new GetAckBroadcaster()).start();
            new Thread(new WaitClientTimeoutChecker()).start();
        }
        new Thread(new BlockedClientTimeoutChecker()).start();


        // ====================================================================
        // --- Main Event Loop ---
        // ====================================================================
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
                            clientBuffers.put(clientChannel, ByteBuffer.allocate(1024)); // Allocate a buffer for the new client
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

        int bytesRead = clientChannel.read(buffer);
        if (bytesRead == -1) {
            System.out.println("Client disconnected cleanly.");
            cleanupClient(key);
            return;
        }

        buffer.flip(); // Prepare buffer for reading

        // Loop to process all complete commands currently in the buffer
        while (buffer.hasRemaining()) {
            RESPParser parser = new RESPParser(buffer);
            List<String> commandParts = parser.parse();

            if (commandParts != null) {
                // We successfully parsed a complete command
                String commandName = commandParts.get(0).toUpperCase();

                // Simple command size estimation for offset. A more precise method
                // would be to have the parser report the exact bytes consumed.
                int commandSize = 0;
                for (String part : commandParts) {
                    commandSize += part.getBytes().length;
                }

                if (isWriteCommand(commandName)) {
                    ServerConfig.incrementMasterOffset(commandSize);
                }

                String response = CommandHandler.handle(commandParts, clientChannel);
                if (response != null) {
                    clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                }
            } else {
                // Incomplete command in buffer, wait for more data
                break;
            }
        }
        buffer.compact(); // Discard read data and prepare for next read
    }

    private static void handleWritableKey(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ReplicaInfo replica = ReplicaManager.getReplicaByChannel(channel);

        if (replica != null) {
            ByteBuffer buffer;
            while ((buffer = replica.getWriteQueue().peek()) != null) {
                channel.write(buffer);
                if (buffer.hasRemaining()) {
                    // Could not write the entire buffer, wait for the next writable event
                    return;
                }
                replica.getWriteQueue().poll(); // Remove the fully written buffer
            }
            // All queued data has been written, so we are no longer interested in write events
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

    private static boolean isWriteCommand(String cmd) {
        return switch (cmd.toUpperCase()) {
            case "SET", "DEL", "RPUSH", "LPUSH", "LSET", "LREM", "XADD", "INCR", "DECR" -> true;
            default -> false;
        };
    }
}