package protocols;

import java.util.ArrayList;
import java.util.List;

public class RESPParser {
    private final String input;
    private int pos = 0;

    public RESPParser(String input) {
        this.input = input;
    }

    public static List<String> parse(String input) {
        if (input == null || input.isEmpty()) {
            return new ArrayList<>();
        }
        return new RESPParser(input).parse();
    }

    private List<String> parse() {
        char type = readChar();
        if (type == '*') {
            return parseArray();
        } else {
            // Simple string command support for things like `redis-cli PING`
            return parseSimpleCommand();
        }
    }

    private List<String> parseSimpleCommand() {
        // Reset and read the whole line as a single command
        pos = 0;
        String line = readLine();
        return new ArrayList<>(List.of(line.split(" ")));
    }

    private List<String> parseArray() {
        int numArgs = readIntLine();
        List<String> args = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; i++) {
            char type = readChar();
            if (type != '$') {
                throw new IllegalStateException("Expected bulk string for array element, got " + type);
            }
            args.add(parseBulkString());
        }
        return args;
    }

    private String parseBulkString() {
        int length = readIntLine();
        if (length == -1) {
            return null;
        }
        String bulkString = input.substring(pos, pos + length);
        pos += length;
        // Skip trailing CRLF
        if (pos <= input.length() - 2 && input.charAt(pos) == '\r' && input.charAt(pos + 1) == '\n') {
            pos += 2;
        }
        return bulkString;
    }

    private char readChar() {
        return input.charAt(pos++);
    }

    private String readLine() {
        int start = pos;
        while (pos < input.length() && input.charAt(pos) != '\r') {
            pos++;
        }
        String line = input.substring(start, pos);
        // Skip CRLF
        if (pos <= input.length() - 2 && input.charAt(pos) == '\r' && input.charAt(pos + 1) == '\n') {
            pos += 2;
        }
        return line;
    }

    private int readIntLine() {
        return Integer.parseInt(readLine());
    }
}