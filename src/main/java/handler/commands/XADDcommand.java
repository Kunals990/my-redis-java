package handler.commands;

import handler.Command;
import store.StreamEntry;
import store.StreamStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class XADDcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(args.size() < 5 || (args.size() - 3) % 2 != 0)
            return "-ERR wrong number of arguments for 'XADD'\r\n";

        String streamKey = args.get(1);
        String rawId = args.get(2);

        String finalId;
        try {
            finalId = generateValidId(streamKey, rawId);
        } catch (IOException e) {
            return e.getMessage();
        }

        Map<String, String> entry = new LinkedHashMap<>();
        for (int i = 3; i < args.size(); i += 2) {
            entry.put(args.get(i), args.get(i + 1));
        }

        streamStore.addEntry(streamKey, finalId, entry);
        return "$" + finalId.length() + "\r\n" + finalId + "\r\n";
    }


    private String generateValidId(String streamKey, String rawId) throws IOException {
        long msTime;
        int seqNum;

        if (Objects.equals(rawId, "*")) {
            msTime = System.currentTimeMillis();
            seqNum = 0;
        } else {
            String[] parts = rawId.split("-");
            if (parts.length != 2) {
                throw new IOException("-ERR Invalid ID format\r\n");
            }

            if (Objects.equals(parts[1], "*")) {
                msTime = Long.parseLong(parts[0]);
                if (msTime == 0) {
                    seqNum = 1; // If no entries, start at 1
                    StreamEntry lastEntry = streamStore.getLastEntry(streamKey);
                    if (lastEntry != null) {
                        String[] lastParts = lastEntry.getId().split("-");
                        int lastSeq = Integer.parseInt(lastParts[1]);
                        seqNum = lastSeq + 1;
                    }
                } else {
                    // Similar logic for other ms values with *
                    StreamEntry lastEntry = streamStore.getLastEntry(streamKey);
                    int lastSeq = -1;
                    if (lastEntry != null && lastEntry.getId().startsWith(parts[0] + "-")) {
                        String[] lastParts = lastEntry.getId().split("-");
                        lastSeq = Integer.parseInt(lastParts[1]);
                    }
                    seqNum = lastSeq + 1;
                }
            } else {
                try {
                    msTime = Long.parseLong(parts[0]);
                    seqNum = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    throw new IOException("-ERR Invalid ID format\r\n");
                }

                if (msTime == 0 && seqNum == 0) {
                    throw new IOException("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                }
            }
        }

        // Validate against last ID in stream
        StreamEntry lastEntry = streamStore.getLastEntry(streamKey);
        if (lastEntry != null) {
            String[] lastParts = lastEntry.getId().split("-");
            long lastMs = Long.parseLong(lastParts[0]);
            int lastSeq = Integer.parseInt(lastParts[1]);

            if (msTime < lastMs || (msTime == lastMs && seqNum <= lastSeq)) {
                throw new IOException("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            }
        }

        return msTime + "-" + seqNum;
    }

}
