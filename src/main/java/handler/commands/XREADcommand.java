package handler.commands;

import handler.Command;
import store.StreamEntry;
import store.StreamStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.*;

public class XREADcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() < 4 || !args.get(1).equalsIgnoreCase("streams")) {
            return "-ERR wrong XREAD syntax\r\n";
        }

        int streamsStart = 2;
        int idsStart = -1;

        for (int i = 2; i < args.size(); i++) {
            if (args.get(i).contains("-") || args.get(i).equals("0")) {
                idsStart = i;
                break;
            }
        }

        if (idsStart == -1 || (args.size() - idsStart) != (idsStart - streamsStart)) {
            return "-ERR wrong number of IDs for streams\r\n";
        }

        List<String> streamKeys = args.subList(streamsStart, idsStart);
        List<String> startIds = args.subList(idsStart, args.size());

        List<String> topLevelResponse = new ArrayList<>();
        topLevelResponse.add("*" + streamKeys.size() + "\r\n");

        for (int i = 0; i < streamKeys.size(); i++) {
            String key = streamKeys.get(i);
            String startId = normalizeId(startIds.get(i));

            List<StreamEntry> entries = streamStore.getStream(key);
            if (entries == null) {
                // Return empty array for this stream
                topLevelResponse.add("*2\r\n");
                topLevelResponse.add("$" + key.length() + "\r\n" + key + "\r\n");
                topLevelResponse.add("*0\r\n");
                continue;
            }

            List<String> entryBlocks = new ArrayList<>();
            for (StreamEntry entry : entries) {
                if (compareIds(entry.getId(), startId) > 0) {
                    // Each entry is *2: ID + field-value array
                    entryBlocks.add("*2\r\n");
                    entryBlocks.add("$" + entry.getId().length() + "\r\n" + entry.getId() + "\r\n");

                    List<String> fieldValue = new ArrayList<>();
                    entry.getFields().forEach((field, value) -> {
                        fieldValue.add("$" + field.length() + "\r\n" + field + "\r\n");
                        fieldValue.add("$" + value.length() + "\r\n" + value + "\r\n");
                    });

                    entryBlocks.add("*" + entry.getFields().size() * 2 + "\r\n" +
                            String.join("", fieldValue));
                    break; // Only return first matching entry
                }
            }

            topLevelResponse.add("*2\r\n");
            topLevelResponse.add("$" + key.length() + "\r\n" + key + "\r\n");

            if (entryBlocks.isEmpty()) {
                topLevelResponse.add("*0\r\n");
            } else {
                topLevelResponse.add("*" + entryBlocks.size() / 3 + "\r\n"); // only one entry block
                topLevelResponse.add(String.join("", entryBlocks));
            }
        }

        return String.join("", topLevelResponse);
    }

    private String normalizeId(String id) {
        if (!id.contains("-")) {
            return id + "-0";
        }
        return id;
    }

    private int compareIds(String id1, String id2) {
        String[] parts1 = id1.split("-");
        String[] parts2 = id2.split("-");

        long t1 = Long.parseLong(parts1[0]);
        long t2 = Long.parseLong(parts2[0]);

        if (t1 != t2) return Long.compare(t1, t2);

        int s1 = Integer.parseInt(parts1[1]);
        int s2 = Integer.parseInt(parts2[1]);

        return Integer.compare(s1, s2);
    }
}
