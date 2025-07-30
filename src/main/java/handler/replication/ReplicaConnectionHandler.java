package handler.replication;

import config.ReplicaConfig;
import config.ServerConfig;
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

import static java.lang.System.in;

public class ReplicaConnectionHandler implements Runnable {

    private static final Logger logger = Logger.getLogger(ReplicaConnectionHandler.class.getName());

    private final String masterHost;
    private final int masterPort;

    public ReplicaConnectionHandler(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    private static final Map<String, Command> commandMap = Map.ofEntries(
            Map.entry("PING", new PingCommand()),
            Map.entry("ECHO", new EchoCommand()),
            Map.entry("SET", new SetCommand()),
            Map.entry("GET", new GetCommand()),
            Map.entry("RPUSH", new RPUSHcommand()),
            Map.entry("LRANGE", new LRANGEcommand()),
            Map.entry("LPUSH", new LPUSHcommand()),
            Map.entry("LLEN", new LLENcommand()),
            Map.entry("LPOP", new LPOPcommand()),
            Map.entry("BLPOP", new BLPOPcommand()),
            Map.entry("TYPE", new TYPEcommand()),
            Map.entry("XADD", new XADDcommand()),
            Map.entry("XRANGE", new XRANGEcommand()),
            Map.entry("XREAD", new XREADcommand()),
            Map.entry("INCR", new INCRcommand()),
            Map.entry("MULTI", MULTIcommand.getInstance()),
            Map.entry("EXEC", new EXECcommand()),
            Map.entry("DISCARD", new DISCARDcommand()),
            Map.entry("INFO", new INFOcommand()),
            Map.entry("REPLCONF", new REPLCONFcommand()),
            Map.entry("PSYNC", new PSYNCcommand()),
            Map.entry("WAIT", new WAITcommand())
    );

    @Override
    public void run() {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            logger.info("Connected to master at " + masterHost + ":" + masterPort);

            OutputStream out = socket.getOutputStream();
            InputStream inputStream = new BufferedInputStream(socket.getInputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(socket.getInputStream());
            completeHandShake1(out, inputStream);
            completeHandShake2(out, inputStream);
            completeHandShake3(out, inputStream);
            completeHandShake4(out, inputStream);
            startCommandReplicationLoop(out, bufferedInputStream);

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

    private void completeHandShake4(OutputStream outputStream, InputStream inputStream) throws IOException {
        List<String> request = new ArrayList<>();
        request.add("PSYNC");
        request.add("?");
        request.add("-1");
        byte[] psync = RESPUtils.buildCommand(request);
        outputStream.write(psync);
        outputStream.flush();

        byte[] buffer = new byte[1024];
        int read = inputStream.read(buffer);
        //RDB File
        String response = RESPResponseParser.parseSimpleString(buffer, read);

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


