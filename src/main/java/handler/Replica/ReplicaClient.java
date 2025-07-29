package handler.Replica;

import handler.ServerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ReplicaClient {

    public void ping(){
        String masterHost = ServerConfig.getMaster_host();
        int masterPort = Integer.parseInt(ServerConfig.getMaster_port());
        String response=null;
        try(Socket socket = new Socket(masterHost,masterPort)){
            socket.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             response= in.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
