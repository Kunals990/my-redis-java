package handler.commands;

import config.ServerConfig;
import handler.Command;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class PSYNCcommand implements Command {

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        String replicationId = ServerConfig.getMaster_replid();
        return "+FULLRESYNC"+replicationId+" 0\r\n";
    }
}
