package handler.replication;

import java.nio.channels.SocketChannel;

public class ReplicaInfo {
    private final SocketChannel channel;
    private final int listeningPort;

    public ReplicaInfo(SocketChannel channel, int listeningPort) {
        this.channel = channel;
        this.listeningPort = listeningPort;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    @Override
    public String toString() {
        return "ReplicaInfo{" +
                "channel=" + channel +
                ", listeningPort=" + listeningPort +
                '}';
    }
}