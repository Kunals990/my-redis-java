package handler;

import java.nio.channels.SocketChannel;

public class WaitClient {
    public final SocketChannel channel;
    public final long expectedOffset;
    public final int requiredReplicas;
    public final long startTime;
    public final long timeoutMillis;

    public WaitClient(SocketChannel channel, long expectedOffset, int requiredReplicas, long timeoutMillis) {
        this.channel = channel;
        this.expectedOffset = expectedOffset;
        this.requiredReplicas = requiredReplicas;
        this.startTime = System.currentTimeMillis();
        this.timeoutMillis = timeoutMillis;
    }

    public SocketChannel getChannel(){
        return channel;
    }
}