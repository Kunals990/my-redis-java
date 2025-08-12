package protocols;

import java.util.ArrayList;
import java.util.List;

public class RESPParser {

    public static List<String> parse(String input) {
        List<String> args = new ArrayList<>();

        if (!input.startsWith("*")) {
            throw new IllegalArgumentException("Not a valid RESP array");
        }

        String[] lines = input.split("\r\n");

        int i = 0;
        if (!lines[i].startsWith("*")) {
            throw new IllegalArgumentException("Expected array header");
        }

        int numArgs = Integer.parseInt(lines[i++].substring(1));

        while (i < lines.length && args.size() < numArgs) {
            if (!lines[i].startsWith("$")) {
                throw new IllegalArgumentException("Expected bulk string");
            }

            int length = Integer.parseInt(lines[i++].substring(1));
            if (i >= lines.length || lines[i].length() != length) {
                throw new IllegalArgumentException("Invalid bulk string length");
            }

            args.add(lines[i++]);
        }

        return args;
    }
}
