package handler.commands;

import handler.Command;
import store.StreamStore;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class XADDcommand implements Command {

    private final StreamStore streamStore = StreamStore.getInstance();
    long prevMillisecondsTime=-1;
    int prevSequenceNumber = -1;

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(args.size()<5|| (args.size() - 3) % 2 != 0) return "-ERR wrong number of arguments for 'XADD'\r\n";

        String streamKey = args.get(1);
        String id = args.get(2);
        String[] parts =id.split("-");

        long millisecondsTime = Integer.parseInt(parts[0]);
        int sequenceNumber=Integer.parseInt(parts[1]);
        if(millisecondsTime>prevMillisecondsTime){
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        }
        if(millisecondsTime==prevMillisecondsTime){
            if(sequenceNumber<prevSequenceNumber){
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }
        prevSequenceNumber=sequenceNumber;
        prevMillisecondsTime=millisecondsTime;
        Map<String, String> entry = new LinkedHashMap<>();
        for (int i = 3; i < args.size(); i += 2) {
            entry.put(args.get(i), args.get(i + 1));
        }

        streamStore.addEntry(streamKey, id, entry);
        return "$"+id.length()+"\r\n" + id + "\r\n";
    }
}
