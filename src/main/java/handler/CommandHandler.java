package handler;

import handler.commands.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandHandler {
    private static final Map<String, Command> commandMap = new HashMap<>();

    static {
        commandMap.put("PING", new PingCommand());
        commandMap.put("ECHO",new EchoCommand());
        commandMap.put("SET",new SetCommand());
        commandMap.put("GET",new GetCommand());
        commandMap.put("RpushCommand",new RpushCommand());
    }

    public static String handle(List<String> args) {
        if (args.isEmpty()) {
            return "-ERR Empty command\r\n";
        }

        String commandName = args.get(0).toUpperCase();
        Command command = commandMap.get(commandName);

        if (command == null) {
            return "-ERR unknown command '" + commandName + "'\r\n";
        }

        return command.execute(args);
    }
}
