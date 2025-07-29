package handler.commands;

import handler.BlockedClient;
import handler.BlockingClientManager;
import handler.Command;
import protocols.RESPBuilder;
import store.StreamEntry;
import store.StreamStore;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class XREADcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();
    private final BlockingClientManager blockingManager = BlockingClientManager.getInstance();

    @Override
    public String execute(List<String> commandParts, SocketChannel clientChannel) {
        try {
            boolean isBlocking = false;
            long timeout = 0;
            int streamsIndex = -1;
            boolean isBlocking$=false;


            // Parse XREAD [BLOCK timeout] STREAMS key [key2 ...] ID [ID2 ...]
            for (int i = 1; i < commandParts.size(); i++) {
                if (commandParts.get(i).equalsIgnoreCase("BLOCK")) {
                    isBlocking = true;
                    timeout = Long.parseLong(commandParts.get(i + 1));
                    i++; // skip timeout value
                }
                else if (commandParts.get(i).equalsIgnoreCase("STREAMS")) {
                    streamsIndex = i;
                    break;
                }
            }

            if (streamsIndex == -1 || streamsIndex + 1 >= commandParts.size()) {
                return RESPBuilder.error("ERR syntax error: missing STREAMS section");
            }

            List<String> keys = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            int numKeys = (commandParts.size() - streamsIndex - 1) / 2;
            for (int i = streamsIndex + 1; i <= streamsIndex + numKeys; i++) {
                keys.add(commandParts.get(i));
            }
            for (int i = 0; i < commandParts.size(); i++) {
                String rawId = commandParts.get(streamsIndex + 1 + numKeys + i);
                if (rawId.equals("$")) {
                    List<StreamEntry> stream = streamStore.getStream(keys.get(i));
                    if (!stream.isEmpty()) {
                        String lastId = stream.get(stream.size() - 1).getId();
                        ids.add(lastId);
                    } else {
                        ids.add("0-0");
                    }
                } else {
                    ids.add(rawId);
                }
            }

            Map<String, List<String>> availableEntries = new LinkedHashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                String streamKey = keys.get(i);
                String id = ids.get(i);
                List<String> entries = streamStore.readRangeAfter(streamKey, id);
                if (!entries.isEmpty()) {
                    availableEntries.put(streamKey, entries);
                }
            }

            if (!availableEntries.isEmpty()) {
                return RESPBuilder.streamEntries(availableEntries);
            }

            if (isBlocking) {
                // Delay response, add to blocking queue
                blockingManager.addBlockedClientForStreams(keys, clientChannel, timeout);
                return null; // do not respond now
            } else {
                return "$-1\r\n"; // RESP null if no data and not blocking
            }

        } catch (Exception e) {
            return RESPBuilder.error("ERR " + e.getMessage());
        }
    }


}
