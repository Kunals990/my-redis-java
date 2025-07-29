import handler.BlockedClientTimeoutChecker;
import handler.CommandHandler;
import config.ServerConfig;
import handler.replication.ReplicaConnectionHandler;
import protocols.RESPParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException {
        int port = 6379;

        for(int i=0;i<args.length-1;i++){
            if(args[i].equals("--port")){
                try {
                    port=Integer.parseInt(args[i+1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port number: "+args[i+1]);
                    System.exit(1);
                }

            }

            if(args[i].equals("--replicaof")){
                String[] parts = args[i + 1].split(" ");
                String masterHost = parts[0];
                String masterPort = parts[1];
                ServerConfig.setMaster_host(masterHost);
                ServerConfig.setMaster_port(masterPort);
                ServerConfig.setRole("slave");

                ReplicaConnectionHandler replicaHandler = new ReplicaConnectionHandler(masterHost,Integer.parseInt(masterPort));
                Executors.newSingleThreadExecutor().submit(replicaHandler);
            }
        }

        // Step 1: Setup non-blocking server socket channel
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        // Step 2: Register server socket with selector for accept events
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

                // Read data from client
                if (key.isReadable()) {
                    SocketChannel clientChannel = (SocketChannel) key.channel();
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
                    String input = new String(buffer.array(), 0, buffer.limit()).trim();
                    System.out.println("Received: " + input);

                    try{
                        List<String> commandParts = RESPParser.parse(input);
                        System.out.println("Parsed command: "+commandParts);

                        String response = CommandHandler.handle(commandParts,clientChannel);
                        if (response != null) {
                            clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                        }

                    } catch (RuntimeException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
