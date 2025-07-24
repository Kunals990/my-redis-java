import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

class ClientHandler extends Thread {
    public final Socket clientsocket;

    ClientHandler(Socket clientsocket) {
        this.clientsocket = clientsocket;
    }

    public void run () {
        try(InputStream inputStream = clientsocket.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             OutputStream outputStream = clientsocket.getOutputStream()
        ){
            String inputLine;
            while((inputLine=reader.readLine())!=null){
                if ("PING".equals(inputLine)) {
                    outputStream.write("+PONG\r\n".getBytes());
                    outputStream.flush();
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            try{
                clientsocket.close();
            }catch (IOException e){};
        }
    }
}

public class ThreadImplementation {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");


    int port = 6379;
    try (ServerSocket serverSocket = new ServerSocket(port)){
      serverSocket.setReuseAddress(true);

      while(true){
          Socket clientSocket = serverSocket.accept();

          ClientHandler handler = new ClientHandler(clientSocket);
          handler.start();
      }


    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }
}
