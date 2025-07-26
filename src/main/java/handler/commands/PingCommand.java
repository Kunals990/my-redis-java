package handler.commands;

import handler.Command;

import java.nio.channels.SocketChannel;
import java.util.List;

public class PingCommand implements Command {

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        return "+PONG\r\n";
    }
}
