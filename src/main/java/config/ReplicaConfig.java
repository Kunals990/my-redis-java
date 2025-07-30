package config;

public class ReplicaConfig {
    static long offset=0;

    public static long getOffset() {
        return offset;
    }

    public static void incrOffset(long offset) {
        ReplicaConfig.offset += offset;
    }
}
