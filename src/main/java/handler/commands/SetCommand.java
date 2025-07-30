package handler.commands;

import config.ServerConfig;
import handler.Command;
import handler.replication.ReplicaInfo;
import handler.replication.ReplicaManager;
import protocols.RESPBuilder;
import store.KeyValueStore;
import util.RESPUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SetCommand implements Command {

    KeyValueStore store = KeyValueStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) {

        if (args.size() < 3) return "-ERR wrong number of arguments for 'set'\r\n";

        int expiry = -1; // -1 means no expiry

        if (args.size() == 5 && args.get(3).equalsIgnoreCase("PX")) {
            try {
                expiry = Integer.parseInt(args.get(4));
            } catch (NumberFormatException e) {
                return "-ERR PX value is not a valid integer\r\n";
            }
        }

        String key = args.get(1);
        String value = args.get(2);

        store.set(key,value,expiry);

        if(ServerConfig.getRole().equalsIgnoreCase("master")){
            List<ReplicaInfo> replicas = ReplicaManager.getReplicas();
            byte[] commandBytes = RESPUtils.buildCommand(args);
            for(ReplicaInfo replica :replicas ){
                try {
                    SocketChannel replicaChannel = replica.getChannel();
                    replicaChannel.write(ByteBuffer.wrap(commandBytes));
                }catch (IOException e){
                    return "-ERR Invalid Replica "+replica;
                }

            }
        }
        if(ServerConfig.getRole().equalsIgnoreCase("slave")){
            return null;
        }
        return "+OK\r\n";
    }
}
