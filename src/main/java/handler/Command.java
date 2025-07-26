package handler;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;

public interface Command {
    String execute(List<String> args, SocketChannel clientChannel) throws IOException;
}