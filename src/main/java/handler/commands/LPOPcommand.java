package handler.commands;

import handler.Command;
import store.ListStore;

import java.util.List;

public class LPOPcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String> args) {

        if(args.size()<2) return "-ERR wrong number of arguments for 'LPOP'\r\n";

        String key = args.get(1);

        List<String> list = listStore.getList(key);
        if(list==null || list.isEmpty()) return "$-1\r\n";

        int noOfElements = 1;
        if (args.size() == 3) {
            try {
                noOfElements = Integer.parseInt(args.get(2));
                if (noOfElements <= 0) return "-ERR value is out of range, must be positive\r\n";
            } catch (NumberFormatException e) {
                return "-ERR invalid number format\r\n";
            }
        }

        noOfElements = Math.min(noOfElements, list.size());

        if(noOfElements==1) {
            String element=list.getFirst();
            list.removeFirst();
            return "$"+element.length()+"\r\n"+element+"\r\n";
        }

        StringBuilder result = new StringBuilder("*" + noOfElements + "\r\n");
        for (int i = 0; i < noOfElements; i++) {
            String element = list.removeFirst();
            result.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
        }

        return result.toString();
    }
}
