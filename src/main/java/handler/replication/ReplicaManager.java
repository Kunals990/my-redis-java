package handler.replication;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReplicaManager {

    private static final List<ReplicaInfo> replicas = new CopyOnWriteArrayList<>();

    public static void addReplica(SocketChannel channel, int listeningPort) {
        replicas.add(new ReplicaInfo(channel, listeningPort));
        try {
            // Tell the listener to start watching this new replica for ACKs
            ReplicaAckListener.getInstance().registerNewReplica(channel);
        } catch (IOException e) {
            System.err.println("Failed to register new replica with ACK listener");
            e.printStackTrace();
        }
    }

    public static List<ReplicaInfo> getReplicas() {
        return replicas;
    }

    public static int noOfReplicas(){
        return replicas.size();
    }

    public static void removeReplica(SocketChannel channel) {
        replicas.removeIf(r -> r.getChannel().equals(channel));
        System.out.println("[ReplicaManager] Removed replica: " + channel);
    }

    public static void clearAll() {
        replicas.clear();
        System.out.println("[ReplicaManager] Cleared all replicas.");
    }

    public static void updateReplicaOffset(SocketChannel channel, long offset) {
        for (ReplicaInfo replica : replicas) {
            if (replica.getChannel().equals(channel)) {
                replica.setReplicationOffset(offset);
                break;
            }
        }
    }

    public static ReplicaInfo getReplicaByChannel(SocketChannel channel) {
        for (ReplicaInfo replica : replicas) {
            if (replica.getChannel().equals(channel)) {
                return replica;
            }
        }
        return null;
    }

    public static int getNumReplicasAcked(long offset) {
        int count = 0;
        for (ReplicaInfo replica : replicas) {
            if (replica.getReplicationOffset() >= offset) {
                count++;
            }
        }
        return count;
    }
}
