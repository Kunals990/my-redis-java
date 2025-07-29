package handler.replication;

import config.ServerConfig;
import util.RESPResponseParser;
import util.RESPUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
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

            completeHandShake1(out,in);
            completeHandShake2(out,in);
            completeHandShake3(out,in);
            completeHandShake4(out,in);

        } catch (IOException e) {
            logger.severe("Failed to connect to master: " + e.getMessage());
        }
    }

    private void completeHandShake1(OutputStream outputStream, InputStream inputStream) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("PING");
        byte[] pingCommand = RESPUtils.buildCommand(request);
        outputStream.write(pingCommand);
        outputStream.flush();

        byte[] buffer = new byte[1024];
        int read = inputStream.read(buffer);
        String response = RESPResponseParser.parseSimpleString(buffer,read);
        if(!"PONG".equalsIgnoreCase(response)){
            throw new IOException("Handshake stage one failed. Got: " + response);
        }
    }

    private void completeHandShake2(OutputStream outputStream, InputStream inputStream) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("listening-port");
        request.add("6380");
        byte[] replConfig1 = RESPUtils.buildCommand(request);
        outputStream.write(replConfig1);
        outputStream.flush();

        byte[] buffer = new byte[1024];
        int read = inputStream.read(buffer);
        String response = RESPResponseParser.parseSimpleString(buffer,read);
        if(!"OK".equalsIgnoreCase(response)){
            throw new IOException("Handshake stage two failed. Got: " + response);
        }

    }

    private void completeHandShake3(OutputStream outputStream, InputStream inputStream) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("REPLCONF");
        request.add("capa");
        request.add("psync2");
        byte[] replConfig1 = RESPUtils.buildCommand(request);
        outputStream.write(replConfig1);
        outputStream.flush();

        byte[] buffer = new byte[1024];
        int read = inputStream.read(buffer);
        String response = RESPResponseParser.parseSimpleString(buffer,read);
        if(!"OK".equalsIgnoreCase(response)){
            throw new IOException("Handshake stage three failed. Got: " + response);
        }
    }

    private void completeHandShake4(OutputStream outputStream,InputStream inputStream) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("PSYNC");
        request.add("?");
        request.add("-1");
        byte[] psync = RESPUtils.buildCommand(request);
        outputStream.write(psync);
        outputStream.flush();

        byte[] buffer = new byte[1024];
        int read = inputStream.read(buffer);
        String response = RESPResponseParser.parseSimpleString(buffer,read);

    }


}
