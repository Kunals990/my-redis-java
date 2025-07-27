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
    long prevMillisecondsTime=-1;
    int prevSequenceNumber = -1;

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(args.size()<5|| (args.size() - 3) % 2 != 0) return "-ERR wrong number of arguments for 'XADD'\r\n";

        String streamKey = args.get(1);
        String id = args.get(2);
        long msTime;
        int seqNum;
        if(Objects.equals(id, "*")){
            msTime=System.currentTimeMillis();
            seqNum=0;
        }
        else{
            String[] parts =id.split("-");
            if (parts.length != 2) {
                return "-ERR Invalid ID format\r\n";
            }

            try {
                msTime = Long.parseLong(parts[0]);
                seqNum = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                return "-ERR Invalid ID format\r\n";
            }
        }


        if (msTime == 0 && seqNum == 0) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        StreamEntry lastEntry = streamStore.getLastEntry(streamKey);
        if (lastEntry != null) {
            String[] lastParts = lastEntry.getId().split("-");
            long lastMs = Long.parseLong(lastParts[0]);
            int lastSeq = Integer.parseInt(lastParts[1]);

            if (msTime < lastMs || (msTime == lastMs && seqNum <= lastSeq)) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }

        Map<String, String> entry = new LinkedHashMap<>();
        for (int i = 3; i < args.size(); i += 2) {
            entry.put(args.get(i), args.get(i + 1));
        }

        streamStore.addEntry(streamKey, id, entry);
        return "$"+id.length()+"\r\n" + id + "\r\n";
    }
}
