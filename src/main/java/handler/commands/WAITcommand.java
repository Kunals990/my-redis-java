package handler.commands;

import handler.Command;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public class WAITcommand implements Command {
    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
        return ":"+replicas.size()+"0\r\n";
    }
}
