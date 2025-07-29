package handler.commands;

import handler.Command;
import store.CommandStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MULTIcommand implements Command {

    private static final MULTIcommand INSTANCE = new MULTIcommand();
    public MULTIcommand() {};

    public static MULTIcommand getInstance(){
        return INSTANCE;
    }

    public boolean isMulti(SocketChannel channel) {
        return multiClients.contains(channel);
    }

    private final Set<SocketChannel> multiClients = new HashSet<>();

    public void disableMulti(SocketChannel channel) {
        multiClients.remove(channel);
    }

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        multiClients.add(clientChannel);
        return "+OK\r\n";
    }
}
