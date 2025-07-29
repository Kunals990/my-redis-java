package handler.commands;

import handler.Command;
import handler.CommandHandler;
import store.CommandStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class EXECcommand implements Command {

    CommandStore commandStore = CommandStore.getInstance();
    MULTIcommand multIcommand = MULTIcommand.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(!MULTIcommand.getInstance().isMulti(clientChannel)){
            return "-ERR EXEC without MULTI\r\n";
        }
        else{
            Queue<List<String>> commands = commandStore.getQueue(clientChannel);
            if(commands.isEmpty()){
                multIcommand.disableMulti(clientChannel);
                return "*0\r\n";
            }
            List<String> results = new ArrayList<>();
            while(!commands.isEmpty()){
                List<String> commandArgs=commands.poll();
                String result = CommandHandler.handle(commandArgs,clientChannel);
                results.add(result.trim());
            }

            multIcommand.disableMulti(clientChannel);
            commandStore.clearQueue(clientChannel);

            StringBuilder resp = new StringBuilder();
            resp.append("*").append(results.size()).append("\r\n");
            for(String res:results){
                resp.append(res).append("\r\n");
            }

            return resp.toString();
        }
    }
}
