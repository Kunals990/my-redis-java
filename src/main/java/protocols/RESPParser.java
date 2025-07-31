// In protocols/RESPParser.java
package protocols;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RESPParser {
    private final ByteBuffer buffer;

    public RESPParser(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public List<String> parse() {
        if (!buffer.hasRemaining()) {
            return null;
        }
        buffer.mark(); // Mark the current position in case of incomplete command
        char type = (char) buffer.get();

        if (type == '*') {
            return parseArray();
        }

        // If we don't have a complete command, reset the buffer to the mark
        buffer.reset();
        return null;
    }

    private List<String> parseArray() {
        int numElements = parseInteger();
        if (numElements == -1) {
            return null; // Incomplete
        }

        List<String> result = new ArrayList<>(numElements);
        for (int i = 0; i < numElements; i++) {
            if (buffer.get() != '$') {
                return null; // Protocol error or incomplete
            }
            int len = parseInteger();
            if (len == -1) {
                return null; // Incomplete
            }
            if (buffer.remaining() < len + 2) { // +2 for \r\n
                return null; // Incomplete
            }
            byte[] bulkStringBytes = new byte[len];
            buffer.get(bulkStringBytes);
            result.add(new String(bulkStringBytes, StandardCharsets.UTF_8));
            // Consume trailing \r\n
            buffer.get();
            buffer.get();
        }
        return result;
    }

    private int parseInteger() {
        StringBuilder sb = new StringBuilder();
        char c;
        while (buffer.hasRemaining()) {
            c = (char) buffer.get();
            if (c == '\r') {
                if (buffer.hasRemaining() && (char) buffer.get() == '\n') {
                    return Integer.parseInt(sb.toString());
                } else {
                    // Incomplete CRLF
                    return -1;
                }
            }
            sb.append(c);
        }
        return -1; // Incomplete
    }
}