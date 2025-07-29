package handler.Replica;

import handler.ServerConfig;
import handler.commands.INFOcommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ReplicaClient {

    public String ping(){
        String masterHost = ServerConfig.getMaster_host();
        int masterPort = Integer.parseInt(ServerConfig.getMaster_port());
        String response=null;
        try(Socket socket = new Socket(masterHost,masterPort)){
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            out.println("*1\r\n$4\r\nPING\r\n");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             response= in.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return response;
    }
}
