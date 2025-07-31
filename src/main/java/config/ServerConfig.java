package config;

import java.util.concurrent.atomic.AtomicLong;

public class ServerConfig {
    private static String role = "master";
    private static String master_host;
    private static String master_port;
    private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static String master_repl_offset = "0";
    private static final AtomicLong master_offset = new AtomicLong(0);

    public static void setRole(String r) { role = r; }
    public static String getRole() { return role; }
    public static String getMaster_replid() { return master_replid; }
    public static void setMaster_replid(String master_replid) { ServerConfig.master_replid = master_replid; }
    public static String getMaster_repl_offset() { return master_repl_offset; }
    public static void setMaster_repl_offset(String master_repl_offset) { ServerConfig.master_repl_offset = master_repl_offset; }
    public static String getMaster_host() { return master_host; }
    public static void setMaster_host(String host) { ServerConfig.master_host = host; }
    public static String getMaster_port() { return master_port; }
    public static void setMaster_port(String master_port) { ServerConfig.master_port = master_port; }
    public static boolean isMaster() { return role.equalsIgnoreCase("master"); }
    public static void incrementMasterOffset(long bytes) { master_offset.addAndGet(bytes); }
    public static long getMaster_offset() { return master_offset.get(); }
}