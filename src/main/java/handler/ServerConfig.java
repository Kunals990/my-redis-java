package handler;

public class ServerConfig {
    private static String role ="master";

    private static String master_replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static String master_repl_offset="0";

    public static void setRole(String r){
        role=r;
    }

    public static String getRole(){
        return role;
    }

    public static String getMaster_replid() {
        return master_replid;
    }

    public static void setMaster_replid(String master_replid) {
        ServerConfig.master_replid = master_replid;
    }

    public static String getMaster_repl_offset() {
        return master_repl_offset;
    }

    public static void setMaster_repl_offset(String master_repl_offset) {
        ServerConfig.master_repl_offset = master_repl_offset;
    }


}
