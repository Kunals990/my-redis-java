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
        String rawId     = args.get(2);

        String finalId;
        try {
            finalId = generateValidId(rawId, streamKey);
        } catch (IOException e) {
            return e.getMessage();
        }

        Map<String, String> entryFields = new LinkedHashMap<>();
        for (int i = 3; i < args.size(); i += 2) {
            entryFields.put(args.get(i), args.get(i + 1));
        }

        streamStore.addEntry(streamKey, finalId, entryFields);

        // Notify any blocking client
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
        int  seqNum;

        boolean isAutoTime = false;
        boolean isAutoSeq  = false;

        if (rawId.equals("*")) {
            isAutoTime = true;
            isAutoSeq  = true;
        } else if (rawId.contains("-")) {
            String[] parts = rawId.split("-");
            if (parts.length != 2) throw new IOException("-ERR Invalid ID format\r\n");
            isAutoTime = parts[0].equals("*");
            isAutoSeq  = parts[1].equals("*");
        } else {
            throw new IOException("-ERR Invalid ID format\r\n");
        }

        List<StreamEntry> allEntries = streamStore.getStream(streamKey);
        long lastMs  = -1;
        int  lastSeq = -1;
        if (!allEntries.isEmpty()) {
            String[] lastParts = allEntries.get(allEntries.size() - 1).getId().split("-");
            lastMs  = Long.parseLong(lastParts[0]);
            lastSeq = Integer.parseInt(lastParts[1]);
        }

        if (isAutoTime && isAutoSeq) {
            // fully auto
            long now = System.currentTimeMillis();
            msTime = now;
            seqNum = 0;
            if (lastMs > now || (lastMs == now && lastSeq >= 0)) {
                msTime = lastMs;
                seqNum = lastSeq + 1;
            }

        } else if (isAutoTime) {
            // auto time, user seq
            int providedSeq;
            try {
                providedSeq = Integer.parseInt(rawId.split("-")[1]);
            } catch (NumberFormatException e) {
                throw new IOException("-ERR Invalid ID format\r\n");
            }
            long now = System.currentTimeMillis();
            msTime = now;
            seqNum = providedSeq;
            if (lastMs > now || (lastMs == now && lastSeq >= seqNum)) {
                msTime = lastMs;
                seqNum = lastSeq + 1;
            }

        } else if (isAutoSeq) {
            // user time, auto seq
            long candidateMs;
            try {
                candidateMs = Long.parseLong(rawId.split("-")[0]);
            } catch (NumberFormatException e) {
                throw new IOException("-ERR Invalid ID format\r\n");
            }
            if (lastMs >= candidateMs) {
                msTime = lastMs;
                seqNum = lastSeq + 1;
            } else {
                msTime = candidateMs;
                int maxSeqAtTs = -1;
                for (StreamEntry entry : allEntries) {
                    String[] idParts = entry.getId().split("-");
                    if (Long.parseLong(idParts[0]) == candidateMs) {
                        maxSeqAtTs = Math.max(maxSeqAtTs, Integer.parseInt(idParts[1]));
                    }
                }
                if (maxSeqAtTs >= 0) {
                    seqNum = maxSeqAtTs + 1;
                } else if (candidateMs == 0) {
                    seqNum = 1;
                } else {
                    seqNum = 0;
                }
            }

        }
        else {
            // fully specified by user
            // parse the user’s timestamp/seq
            String[] parts = rawId.split("-");
            long rawMs;
            int  rawSeq;
            try {
                rawMs  = Long.parseLong(parts[0]);
                rawSeq = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new IOException("-ERR Invalid ID format\r\n");
            }

            // 1) Special case: user literally asked for 0-0
            if (rawMs == 0 && rawSeq == 0) {
                throw new IOException("-ERR The ID specified in XADD must be greater than 0-0\r\n");
            }

            // 2) If the user’s ID is ≤ the stream’s top, block it
            if (lastMs > rawMs || (lastMs == rawMs && lastSeq >= rawSeq)) {
                throw new IOException("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            }

            // 3) Otherwise it’s valid
            msTime = rawMs;
            seqNum = rawSeq;
        }

        return msTime + "-" + seqNum;
    }
}
