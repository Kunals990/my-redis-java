package protocols;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RESPBuilder {

    public static String simpleString(String msg) {
        return "+" + msg + "\r\n";
    }

    public static String error(String msg) {
        return "-" + msg + "\r\n";
    }

    public static String bulkString(String msg) {
        return "$" + msg.length() + "\r\n" + msg + "\r\n";
    }

    public static String streamEntries(Map<String, List<String>> entriesByKey) {
        StringBuilder builder = new StringBuilder();
        builder.append("*").append(entriesByKey.size()).append("\r\n");

        for (Map.Entry<String, List<String>> entry : entriesByKey.entrySet()) {
            String key = entry.getKey();
            List<String> flatList = entry.getValue();

            builder.append("*2\r\n");
            builder.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");

            // Group entries by (id + fields)
            List<String> encodedEntries = new ArrayList<>();
            for (int i = 0; i < flatList.size();) {
                String id = flatList.get(i++);
                List<String> fieldValPairs = new ArrayList<>();
                while (i + 1 < flatList.size() && !flatList.get(i).contains("-")) {
                    fieldValPairs.add(flatList.get(i++)); // field
                    fieldValPairs.add(flatList.get(i++)); // value
                }

                StringBuilder entryBuilder = new StringBuilder();
                entryBuilder.append("*2\r\n");

                entryBuilder.append("$").append(id.length()).append("\r\n").append(id).append("\r\n");

                entryBuilder.append("*").append(fieldValPairs.size()).append("\r\n");
                for (String s : fieldValPairs) {
                    entryBuilder.append("$").append(s.length()).append("\r\n").append(s).append("\r\n");
                }

                encodedEntries.add(entryBuilder.toString());
            }

            builder.append("*").append(encodedEntries.size()).append("\r\n");
            for (String encoded : encodedEntries) {
                builder.append(encoded);
            }
        }

        return builder.toString();
    }

}
