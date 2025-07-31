package handler.replication;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

public class ReplicaInfo {
    private final SocketChannel channel;
    private Queue<ByteBuffer> writeQueue = new LinkedList<>();
    private final int listeningPort;
    private volatile long replicationOffset = 0;

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

    public long getReplicationOffset() {
        return replicationOffset;
    }

    public void setReplicationOffset(long offset) {
        this.replicationOffset = offset;
    }

    public Queue<ByteBuffer> getWriteQueue() {
        return writeQueue;
    }

    @Override
    public String toString() {
        return "ReplicaInfo{" +
                "channel=" + channel +
                ", listeningPort=" + listeningPort +
                '}';
    }
}