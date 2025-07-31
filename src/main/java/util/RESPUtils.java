package util;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RESPUtils {
    public static byte[] buildCommand(List<String> args) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.size()).append("\r\n");
        for (String arg : args) {
            byte[] argBytes = arg.getBytes(StandardCharsets.UTF_8);
            sb.append("$").append(argBytes.length).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}