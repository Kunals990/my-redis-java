package handler.commands;

import handler.Command;

import java.nio.channels.SocketChannel;
import java.util.List;

public class EchoCommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {
        if (args.size() < 2) return "-ERR wrong number of arguments for 'echo'\r\n";
        String msg = args.get(1);
        return "$" + msg.length() + "\r\n" + msg + "\r\n";
    }
}
