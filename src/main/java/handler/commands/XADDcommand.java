package handler.commands;

import handler.BlockingClientManager;
import handler.Command;
import protocols.RESPBuilder;
import store.StreamEntry;
import store.StreamStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class XADDcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();
    private final BlockingClientManager blockingManager = BlockingClientManager.getInstance();


    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if (args.size() < 5 || (args.size() - 3) % 2 != 0) {
            return "-ERR wrong number of arguments for 'XADD'\r\n";
        }

        String streamKey = args.get(1);
        String rawId = args.get(2);

        String finalId;
        try {
            finalId = generateValidId(streamKey, rawId);
        } catch (IOException e) {
            return e.getMessage();
        }

        Map<String, String> entryFields = new LinkedHashMap<>();
        for (int i = 3; i < args.size(); i += 2) {
            entryFields.put(args.get(i), args.get(i + 1));
        }

        // Add to stream
        streamStore.addEntry(streamKey, finalId, entryFields);

        // Notify blocking clients if any
        SocketChannel waitingClient = blockingManager.getNextBlockedClient(streamKey);
        if (waitingClient != null) {
            Map<String, List<String>> payload = new HashMap<>();
            List<String> flatList = new ArrayList<>();
            flatList.add(finalId);
            for (Map.Entry<String, String> field : entryFields.entrySet()) {
                flatList.add(field.getKey());
                flatList.add(field.getValue());
            }
            payload.put(streamKey, flatList);

            String response = RESPBuilder.streamEntries(payload);
            waitingClient.write(ByteBuffer.wrap(response.getBytes()));
        }

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
                try {
                    msTime = Long.parseLong(parts[0]);
                } catch (NumberFormatException e) {
                    throw new IOException("-ERR Invalid ID format\r\n");
                }

                seqNum = 0;

                int maxSeq = -1;
                for (StreamEntry entry : streamStore.getStream(streamKey)) {
                    String[] idParts = entry.getId().split("-");
                    if (idParts.length != 2) continue;

                    if (Long.parseLong(idParts[0]) == msTime) {
                        int s = Integer.parseInt(idParts[1]);
                        maxSeq = Math.max(maxSeq, s);
                    }
                }

                seqNum = (maxSeq == -1) ? 1 : maxSeq + 1;

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
