package handler.commands;

import handler.BlockingClientManager;
import handler.Command;
import store.ListStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class RPUSHcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String> args, SocketChannel clientChannel) throws IOException {
        if(args.size()<3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";

        String key=args.get(1);
        String value = args.get(2);
        List<String> values = args.subList(2, args.size());

        // Check if any client is blocked on the key
        SocketChannel blockedClient = BlockingClientManager.getInstance().getNextBlockedClient(key);
        if (blockedClient != null) {
            String firstValue = values.get(0);
            String resp = "*2\r\n$" + key.length() + "\r\n" + key + "\r\n" +
                    "$" + firstValue.length() + "\r\n" + firstValue + "\r\n";
            blockedClient.write(ByteBuffer.wrap(resp.getBytes()));

            // Store remaining values (if any)
            if (values.size() > 1) {
                listStore.appendToList(key, values.subList(1, values.size()));
            }

            return ":" + values.size() + "\r\n";
        }

        return listStore.appendToList(key, values);

    }
}
