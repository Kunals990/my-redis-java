package handler;

import handler.commands.*;

import java.util.Map;

public class CommandRegistry {
    private static final Map<String, Command> commandMap = Map.ofEntries(
            Map.entry("PING", new PingCommand()),
            Map.entry("ECHO", new EchoCommand()),
            Map.entry("SET", new SetCommand()),
            Map.entry("GET", new GetCommand()),
            Map.entry("RPUSH", new RPUSHcommand()),
            Map.entry("LRANGE", new LRANGEcommand()),
            Map.entry("LPUSH", new LPUSHcommand()),
            Map.entry("LLEN", new LLENcommand()),
            Map.entry("LPOP", new LPOPcommand()),
            Map.entry("BLPOP", new BLPOPcommand()),
            Map.entry("TYPE", new TYPEcommand()),
            Map.entry("XADD", new XADDcommand()),
            Map.entry("XRANGE", new XRANGEcommand()),
            Map.entry("XREAD", new XREADcommand()),
            Map.entry("INCR", new INCRcommand()),
            Map.entry("MULTI", MULTIcommand.getInstance()),
            Map.entry("EXEC", new EXECcommand()),
            Map.entry("DISCARD", new DISCARDcommand()),
            Map.entry("INFO", new INFOcommand()),
            Map.entry("REPLCONF", new REPLCONFcommand()),
            Map.entry("PSYNC", new PSYNCcommand()),
            Map.entry("WAIT", new WAITcommand())
    );


    public static Command getCommand(String name) {
        return commandMap.get(name.toUpperCase());
    }
}
