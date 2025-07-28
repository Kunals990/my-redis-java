package handler.commands;

import handler.Command;
import store.StreamEntry;
import store.StreamStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class XRANGEcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() != 4) {
            return "-ERR wrong number of arguments for 'xrange' command\r\n";
        }

        String streamKey = args.get(1);
        String startRaw = args.get(2);
        String endRaw = args.get(3);

        List<StreamEntry> entries = streamStore.getStream(streamKey);
        if (entries == null || entries.isEmpty()) {
            return "*0\r\n"; // empty array
        }

        String startId = normalizeId(startRaw, false);
        String endId = normalizeId(endRaw, true);

        List<String> responseLines = new ArrayList<>();
        int count = 0;

        for (StreamEntry entry : entries) {
            String entryId = entry.getId();
            if (compareIds(entryId, startId) >= 0 && compareIds(entryId, endId) <= 0) {
                responseLines.add("*2\r\n");
                responseLines.add("$" + entryId.length() + "\r\n" + entryId + "\r\n");

                List<String> fieldValueList = new ArrayList<>();
                entry.getFields().forEach((field, value) -> {
                    fieldValueList.add("$" + field.length() + "\r\n" + field + "\r\n");
                    fieldValueList.add("$" + value.length() + "\r\n" + value + "\r\n");
                });

                responseLines.add("*" + entry.getFields().size() * 2 + "\r\n" +
                        String.join("", fieldValueList));

                count++;
            }
        }

        if (count == 0) {
            return "*0\r\n";
        }

        return "*" + count + "\r\n" + String.join("", responseLines);
    }

    private String normalizeId(String id, boolean isEnd) {
        if (id.equals("-")) {
            return "0-0"; // beginning of stream
        }
        if (id.equals("+")) {
            return Long.MAX_VALUE + "-" + Integer.MAX_VALUE; // end of stream
        }
        if (!id.contains("-")) {
            return isEnd ? id + "-999999" : id + "-0";
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
