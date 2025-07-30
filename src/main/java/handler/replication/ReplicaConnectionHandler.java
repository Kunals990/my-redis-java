package handler.replication;

import config.ReplicaConfig;
import handler.Command;
import handler.CommandRegistry;
import handler.commands.*;
import util.RESPResponseParser;
import util.RESPUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
            BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
            completeHandShake1(out, in);
            completeHandShake2(out, in);
            completeHandShake3(out, in);
            completeHandShake4(out, in);
            startCommandReplicationLoop(out, in);

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
        String response = RESPResponseParser.parseSimpleString(buffer, read);
        if (!"PONG".equalsIgnoreCase(response)) {
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
        String response = RESPResponseParser.parseSimpleString(buffer, read);
        if (!"OK".equalsIgnoreCase(response)) {
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
        String response = RESPResponseParser.parseSimpleString(buffer, read);
        if (!"OK".equalsIgnoreCase(response)) {
            throw new IOException("Handshake stage three failed. Got: " + response);
        }
    }

    private void completeHandShake4(OutputStream out, InputStream in) throws IOException {
        List<String> req = List.of("PSYNC", "?", "-1");
        out.write(RESPUtils.buildCommand(req));
        out.flush();

        byte[] statusBuf = new byte[1024];
        int n = in.read(statusBuf);
        if (n <= 0) throw new IOException("EOF while waiting for FULLRESYNC");
        String status = RESPResponseParser.parseSimpleString(statusBuf, n);
        int dollar = in.read();
        if (dollar != '$') {
            throw new IOException("Expected '$' starting RDB bulk‐string, got: " + (char)dollar);
        }
        int len = readInt(in);

        long skipped = 0;
        while (skipped < len + 2) {
            long s = in.skip(len + 2 - skipped);
            if (s <= 0) throw new IOException("Failed to skip RDB payload");
            skipped += s;
        }
    }

    private int readInt(InputStream in) throws IOException {
        int b;
        int result = 0;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int nl = in.read(); // should be '\n'
                if (nl != '\n') throw new IOException("Expected LF after CR");
                break;
            }
            if (b < '0' || b > '9') {
                throw new IOException("Non‐digit in length: " + (char)b);
            }
            result = result * 10 + (b - '0');
        }
        if (b == -1) throw new IOException("EOF reading length");
        return result;
    }


    private void startCommandReplicationLoop(OutputStream outputStream, BufferedInputStream inputStream) throws IOException {
        RESPParser parser = new RESPParser(inputStream);
        long offset = 0;
        while (true) {
            offset = ReplicaConfig.getOffset();
            List<String> commandArgs = parser.parseArray();
            if (commandArgs == null || commandArgs.isEmpty()) {
                continue;
            }

            String cmd = commandArgs.get(0).toUpperCase();

            if (cmd.equalsIgnoreCase("REPLCONF") && commandArgs.size() == 3
                    && commandArgs.get(1).equalsIgnoreCase("GETACK")
                    && commandArgs.get(2).equals("*")) {

                String offsetStr = Long.toString(offset);
                String ackResponse = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + offsetStr.length() + "\r\n" + offsetStr + "\r\n";
                outputStream.write(ackResponse.getBytes());
                outputStream.flush();
                continue;
            }

            Command command = CommandRegistry.getCommand(cmd);
            if (command != null) {
                // Execute without writing back to any channel
                command.execute(commandArgs, null);
            } else {
                logger.warning("Unknown replicated command: " + cmd);
            }
        }
    }

}


class RESPParser {

    private final BufferedInputStream in;
    private long bytesRead = 0;

    public RESPParser(BufferedInputStream in) {
        this.in = in;
    }

    public List<String> parseArray() throws IOException {
        bytesRead = 0;

        in.mark(1);
        int b = in.read();
        if (b == -1 || (char) b != '*') return null;
        in.reset();

        b = in.read();
        incrBytes(1);

        int arrayLength = readInt();
        List<String> elements = new ArrayList<>();

        for (int i = 0; i < arrayLength; i++) {
            int type = in.read();
            incrBytes(1);
            if (type != '$') throw new IOException("Expected bulk string");

            int len = readInt();
            byte[] data = in.readNBytes(len);
            incrBytes(len);

            in.readNBytes(2);
            incrBytes(2);

            elements.add(new String(data));
        }

        ReplicaConfig.incrOffset(bytesRead);
        return elements;
    }

    private int readInt() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            incrBytes(1);
            if ((char) b == '\r') {
                int next = in.read();
                if (next != -1) incrBytes(1);
                break;
            }
            sb.append((char) b);
        }
        return Integer.parseInt(sb.toString());
    }

    private void incrBytes(long n) {
        bytesRead += n;
    }
}


