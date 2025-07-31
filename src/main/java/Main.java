import handler.*;
import config.ServerConfig;
import handler.replication.*;
import protocols.RESPParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException {
        int port = 6379;
        String masterHost = null;
        int masterPort = -1;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
                i++; // Skip the value
            } else if (args[i].equals("--replicaof") && i + 1 < args.length) {
                String[] parts = args[i + 1].split(" ");
                masterHost = parts[0];
                masterPort = Integer.parseInt(parts[1]);
                i++; // Skip the value
            }
        }

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
            Thread getAckThread = new Thread(new GetAckBroadcaster());
            getAckThread.start();
            Thread replicaAckListner =  new Thread(new ReplicaAckListener());
            replicaAckListner.start();
            Thread waitClientTimeout = new Thread(new WaitClientTimeoutChecker());
            waitClientTimeout.start();
        }

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        BlockedClientTimeoutChecker timeoutChecker = new BlockedClientTimeoutChecker();
        timeoutChecker.start();

        // Step 3: Event loop
        while (true) {
            selector.select(); // Wait until some channels are ready

            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();

            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove(); // Always remove the key once handled

                // Accept new client connection
                if (key.isAcceptable()) {
                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel clientChannel = server.accept();

                    if (clientChannel != null) {
                        clientChannel.configureBlocking(false);
                        clientChannel.register(selector, SelectionKey.OP_READ);
                        System.out.println("Accepted new client: " + clientChannel.getRemoteAddress());
                    }
                }

                if (key.isReadable()) {
                    SocketChannel clientChannel = (SocketChannel) key.channel();

                    if (BlockingClientManager.getInstance().isBlocked(clientChannel)  || WaitClientManager.isWaiting(clientChannel)) {
                        System.out.println("Ignored command from blocked client: " + clientChannel.getRemoteAddress());
                        continue;
                    }

                    ByteBuffer buffer = ByteBuffer.allocate(1024);

                    int bytesRead = -1;
                    try {
                        bytesRead = clientChannel.read(buffer);
                    } catch (IOException e) {
                        // Client closed unexpectedly
                        key.cancel();
                        clientChannel.close();
                        continue;
                    }

                    if (bytesRead == -1) {
                        // Client closed connection
                        System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
                        key.cancel();
                        clientChannel.close();
                        continue;
                    }

                    // Process the input
                    buffer.flip();
                    String input = new String(buffer.array(), 0, buffer.limit());
                    int respBytes = input.getBytes(StandardCharsets.UTF_8).length;
//                    System.out.println("Received: " + input);

                    try{
                        List<String> commandParts = RESPParser.parse(input);
//                        System.out.println("Parsed command: "+commandParts);
                        String commandName = commandParts.get(0).toUpperCase();
                        if (isWriteCommand(commandName)) {
                            ServerConfig.setMaster_offset(respBytes);
                        }

                        String response = CommandHandler.handle(commandParts,clientChannel);
                        if (response != null) {
                            clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                        }

                    } catch (RuntimeException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (key.isWritable()) {
                    SocketChannel channel = (SocketChannel) key.channel();

                    ReplicaInfo replica = ReplicaManager.getReplicaByChannel(channel);
                    if (replica != null) {
                        ByteBuffer buffer;

                        while ((buffer = replica.getWriteQueue().peek()) != null) {
                            try {
                                channel.write(buffer);
                                if (buffer.hasRemaining()) {
                                    // Could not write entire buffer; wait for next writable
                                    break;
                                }
                                replica.getWriteQueue().poll(); // Remove fully written buffer
                            } catch (IOException e) {
                                System.err.println("Failed to write to replica: " + e.getMessage());
                                key.cancel();
                                channel.close();
                                break;
                            }
                        }

                        if (replica.getWriteQueue().isEmpty()) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                    }
                }

            }
        }
    }

    private static boolean isWriteCommand(String cmd) {
        return switch (cmd) {
            case "SET", "DEL", "RPUSH", "LPUSH", "LSET", "LREM", "XADD", "INCR", "DECR" -> true;
            default -> false;
        };
    }
}
