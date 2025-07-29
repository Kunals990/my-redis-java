package handler;

import handler.commands.*;
import store.CommandStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

public class CommandHandler {

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
            Map.entry("XADD",new XADDcommand()),
            Map.entry("XRANGE",new XRANGEcommand()),
            Map.entry("XREAD",new XREADcommand()),
            Map.entry("INCR",new INCRcommand()),
            Map.entry("MULTI",new MULTIcommand()),
            Map.entry("EXEC",new EXECcommand())
    );

    static CommandStore commandStore = CommandStore.getInstance();

    public static String handle(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.isEmpty()) {
            return "-ERR Empty command\r\n";
        }

        if (MULTIcommand.getInstance().isMulti(clientChannel)) {
            return commandStore.addToQueue(clientChannel, args);
        }

        String commandName = args.get(0).toUpperCase();
        Command command = commandMap.get(commandName);

        if (command == null) {
            return "-ERR unknown command '" + commandName + "'\r\n";
        }

        return command.execute(args,clientChannel);
    }
}
