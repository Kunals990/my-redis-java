package handler.commands;

import handler.Command;
import store.ListStore;

import java.util.List;

public class LLENcommand implements Command {

    ListStore listStore = ListStore.getInstance();

    @Override
    public String execute(List<String> args) {
        if(args.size()<2) return "-ERR wrong number of arguments for 'LLEN'\r\n";

        String key = args.get(1);

        List<String> list = listStore.getList(key);

        if(list==null || list.isEmpty()) return ":0\r\n";

        return ":"+list.size()+"\r\n";
    }
}
