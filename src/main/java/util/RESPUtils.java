package util;

import java.util.List;

public class RESPUtils {

    public static byte[] buildCommand(List<String> args) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.size()).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        return sb.toString().getBytes();
    }
}
