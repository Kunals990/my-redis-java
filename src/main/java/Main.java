import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class Main {
    public static void main(String[] args) throws IOException {
        int port = 6379;

        // Step 1: Setup non-blocking server socket channel
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(port));
        System.out.println("Event-loop server started on port " + port);

        // Step 2: Register server socket with selector for accept events
        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

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

                    // Handle basic command
                    if (input.equalsIgnoreCase("PING")) {
                        String response = "+PONG\r\n";
                        clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                    } else {
                        String response = "-Unknown command\r\n";
                        clientChannel.write(ByteBuffer.wrap(response.getBytes()));
                    }
                }
            }
        }
    }
}
