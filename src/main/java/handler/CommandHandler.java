package handler;

import handler.commands.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandHandler {

    private static final Map<String, Command> commandMap = Map.of(
            "PING", new PingCommand(),
            "ECHO", new EchoCommand(),
            "SET", new SetCommand(),
            "GET", new GetCommand(),
            "RPUSH", new RpushCommand(),
            "LRANGE", new LRANGEcommand(),
            "LPUSH",new LPUSHcommand(),
            "LLEN",new LLENcommand(),
            "LPOP",new LPOPcommand()
    );

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
