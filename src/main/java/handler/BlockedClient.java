package handler;

import java.nio.channels.SocketChannel;

public class BlockedClient {
    public final SocketChannel channel;
    public final long startTime;
    public final long timeoutMillis;

    public BlockedClient(SocketChannel channel, long startTime, long timeoutMillis) {
        this.channel = channel;
        this.startTime = startTime;
        this.timeoutMillis = timeoutMillis;
    }
}
