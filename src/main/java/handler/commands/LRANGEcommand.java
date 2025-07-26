package handler.commands;

import handler.Command;
import store.ListStore;

import java.util.List;

public class LRANGEcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String>args){
        if(args.size()<4) return "-ERR wrong number of arguments for 'LRANGE'\r\n";

        String key = args.get(1);
        List<String> list = listStore.getList(key);

        Integer start = Integer.parseInt(args.get(2));
        Integer stop = Integer.parseInt(args.get(3));

        if(list==null ||list.isEmpty() || start >=list.size() || start>stop) return "*0\r\n";

        Integer size = list.size();
        if(stop>size) {
            stop=size-1;
        }

        StringBuilder result = new StringBuilder("*" + list.size() + "\r\n");
        for(int i=start;i<=stop;i++){
            result.append("$").append(list.get(i).length()).append("\r\n");
            result.append(list.get(i)).append("\r\n");
        }

        return result.toString();
    }

}
