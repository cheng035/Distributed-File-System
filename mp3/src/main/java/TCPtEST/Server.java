package TCPtEST;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    public static void main(String [] args) throws IOException {
        TcpService tcp = new TcpService();
        ServerSocket ssock = new ServerSocket(3000);
        Socket socket = ssock.accept();


        tcp.receiveFile(socket,"asd.txt",false);

    }
}
