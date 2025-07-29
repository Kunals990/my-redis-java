package util;

public class RESPResponseParser {

    public static String parseSimpleString(byte[] data, int length) {
        if (length < 1) return null;
        char prefix = (char) data[0];

        switch (prefix) {
            case '+': // Simple String
            case '-': // Error
                return new String(data, 1, length - 3); // Skip + and \r\n
            case ':': // Integer
                return new String(data, 1, length - 3);
            default:
                return new String(data, 0, length); // Fallback
        }
    }
}
