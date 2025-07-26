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

        //wrap in string
        String result = "*"+list.size()+"\r\n";
        while(!list.isEmpty()){
            result += "$"+list.get(0).length()+"\r\n";
            result +=list.get(0)+"\r\n";
            list.remove(0);
        }
        return result;
    }

}
