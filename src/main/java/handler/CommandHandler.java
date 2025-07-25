package handler;

import java.util.List;

public class CommandHandler {

    public static String handle(List<String> commandParts) {
        if (commandParts.isEmpty()) {
            return "-ERR Empty command\r\n";
        }

        String command = commandParts.get(0).toUpperCase();

        switch (command) {
            case "PING":
                return "+PONG\r\n";

            case "ECHO":
                if (commandParts.size() < 2) {
                    return "-ERR wrong number of arguments for 'echo'\r\n";
                }
                return "$" + commandParts.get(1).length() + "\r\n" + commandParts.get(1) + "\r\n";

            default:
                return "-ERR unknown command '" + commandParts.get(0) + "'\r\n";
        }
    }
}
