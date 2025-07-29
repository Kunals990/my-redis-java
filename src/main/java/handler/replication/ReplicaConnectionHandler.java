package handler.replication;

import util.RESPUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Logger;

public class ReplicaConnectionHandler implements Runnable {

    private static final Logger logger = Logger.getLogger(ReplicaConnectionHandler.class.getName());

    private final String masterHost;
    private final int masterPort;

    public ReplicaConnectionHandler(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    @Override
    public void run() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            logger.info("Connected to master at " + masterHost + ":" + masterPort);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            byte[] pingCommand = RESPUtils.buildCommand("PING");
            out.write(pingCommand);
            out.flush();
            logger.info("PING sent to master.");
            byte[] buffer = new byte[1024];
            int read = in.read(buffer);
            String response = new String(buffer, 0, read);

            byte[] replConfig1 = RESPUtils.buildCommand("REPLCONF","listening-port","6380");
            out.write(replConfig1);
            out.flush();

            byte[] replConfig2= RESPUtils.buildCommand("REPLCONF","capa","psync2");
            out.write(replConfig2);
            out.flush();

            logger.info("Received from master: " + response);

        } catch (IOException e) {
            logger.severe("Failed to connect to master: " + e.getMessage());
        }
    }
}
