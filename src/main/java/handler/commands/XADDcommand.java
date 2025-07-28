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

    private String generateValidId(String rawId, String streamKey) throws IOException {
        long msTime;
        int seqNum;
        boolean isAutoTime = false;
        boolean isAutoSeq = false;

        if (rawId.equals("*")) {
            msTime = System.currentTimeMillis();
            seqNum = 0;
            isAutoTime = true;
            isAutoSeq = true;
        } else if (rawId.contains("-")) {
            String[] parts = rawId.split("-");
            if (parts.length != 2) {
                throw new IOException("-ERR Invalid ID format\r\n");
            }

            isAutoTime = parts[0].equals("*");
            isAutoSeq = parts[1].equals("*");

            // Auto timestamp, fixed sequence
            if (isAutoTime && !isAutoSeq) {
                msTime = System.currentTimeMillis();
                try {
                    seqNum = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    throw new IOException("-ERR Invalid ID format\r\n");
                }
            }

            // Fixed timestamp, auto sequence: "0-*"
            else if (!isAutoTime && isAutoSeq) {
                try {
                    msTime = Long.parseLong(parts[0]);
                } catch (NumberFormatException e) {
                    throw new IOException("-ERR Invalid ID format\r\n");
                }

                int maxSeq = -1;
                List<StreamEntry> allEntries = streamStore.getStream(streamKey);
                for (StreamEntry entry : allEntries) {
                    String[] idParts = entry.getId().split("-");
                    if (idParts.length != 2) continue;

                    if (Long.parseLong(idParts[0]) == msTime) {
                        int seq = Integer.parseInt(idParts[1]);
                        maxSeq = Math.max(maxSeq, seq);
                    }
                }

                seqNum = (maxSeq == -1) ? 1 : maxSeq + 1;
            }

            // Both fixed
            else {
                try {
                    msTime = Long.parseLong(parts[0]);
                    seqNum = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    throw new IOException("-ERR Invalid ID format\r\n");
                }

                List<StreamEntry> allEntries = streamStore.getStream(streamKey);
                if (!allEntries.isEmpty()) {
                    String lastId = allEntries.get(allEntries.size() - 1).getId();
                    String[] lastParts = lastId.split("-");

                    long lastMs = Long.parseLong(lastParts[0]);
                    int lastSeq = Integer.parseInt(lastParts[1]);

                    if (msTime < lastMs || (msTime == lastMs && seqNum <= lastSeq)) {
                        throw new IOException("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                    }
                }
            }
        } else {
            throw new IOException("-ERR Invalid ID format\r\n");
        }

        return msTime + "-" + seqNum;
    }


}
