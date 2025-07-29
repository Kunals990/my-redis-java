package handler;

public class ServerConfig {
    private static String role ="master";

    public static void setRole(String r){
        role=r;
    }

    public static String getRole(){
        return role;
    }
}
