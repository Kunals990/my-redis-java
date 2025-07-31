package handler.replication;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReplicaInfo {

    public enum ReplicaState {
        CONNECTING, // Initial state, handshake in progress
        ONLINE      // Handshake complete, ready for commands
    }

    private final SocketChannel channel;
    private Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final int listeningPort;
    private volatile long replicationOffset = 0;
    private volatile ReplicaState state;

    public ReplicaInfo(SocketChannel channel, int listeningPort) {
        this.channel = channel;
        this.listeningPort = listeningPort;
        this.state=ReplicaState.CONNECTING;
    }

    public ReplicaState getState() {
        return state;
    }

    public void setState(ReplicaState state) {
        this.state = state;
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