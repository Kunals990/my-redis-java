package util;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RESPUtils {

    public static byte[] buildCommand(List<String> args) {
        StringBuilder sb = new StringBuilder();

        // 1. Array prefix
        sb.append('*');
        sb.append(args.size());
        sb.append("\r\n");

        // 2. Each element as a Bulk String
        for (String arg : args) {
            // Get the length of the string in bytes using UTF-8 encoding
            byte[] argBytes = arg.getBytes(StandardCharsets.UTF_8);

            sb.append('$');
            sb.append(argBytes.length); // <-- THE CRITICAL FIX
            sb.append("\r\n");
            sb.append(arg);
            sb.append("\r\n");
        }

        // Always serialize the final command using UTF-8
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}