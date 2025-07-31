// protocols/RESPParser.java
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

    /**
     * Parse the next complete RESP message from the buffer, if any:
     *  - '*' => List<String>
     *  - '+' => simple string (String)
     *  - ':' => integer (Long)
     * Returns null if there's not yet a full message.
     */
    public Object parse() {
        if (!buffer.hasRemaining()) return null;
        buffer.mark();
        char type = (char) buffer.get();

        switch (type) {
            case '*':
                return parseArray();
            case '+':
                return parseSimpleString();
            case ':':
                return parseInteger();
            default:
                // Unsupported or incomplete; reset so next time we retry
                buffer.reset();
                return null;
        }
    }

    private List<String> parseArray() {
        Integer numElements = readIntCRLF();
        if (numElements == null) {
            buffer.reset();
            return null;
        }
        List<String> result = new ArrayList<>(numElements);
        for (int i = 0; i < numElements; i++) {
            if (!buffer.hasRemaining() || (char) buffer.get() != '$') {
                buffer.reset();
                return null;
            }
            Integer len = readIntCRLF();
            if (len == null || buffer.remaining() < len + 2) {
                buffer.reset();
                return null;
            }
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            result.add(new String(bytes, StandardCharsets.UTF_8));
            // consume CRLF
            buffer.get();
            buffer.get();
        }
        return result;
    }

    private String parseSimpleString() {
        StringBuilder sb = new StringBuilder();
        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            if (c == '\r' && buffer.hasRemaining() && (char) buffer.get() == '\n') {
                return sb.toString();
            }
            sb.append(c);
        }
        buffer.reset();
        return null;
    }

    private Long parseInteger() {
        StringBuilder sb = new StringBuilder();
        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            if (c == '\r' && buffer.hasRemaining() && (char) buffer.get() == '\n') {
                try {
                    return Long.parseLong(sb.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            sb.append(c);
        }
        buffer.reset();
        return null;
    }

    /** Helper: read an ASCII integer ending in CRLF; returns null if incomplete */
    private Integer readIntCRLF() {
        StringBuilder sb = new StringBuilder();
        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            if (c == '\r' && buffer.hasRemaining() && (char) buffer.get() == '\n') {
                try {
                    return Integer.parseInt(sb.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            sb.append(c);
        }
        return null; // incomplete
    }
}
