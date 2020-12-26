package TCPtEST;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class Client {

    public static void main(String [] args) throws IOException {
        TcpService tcp = new TcpService();
        Socket socket = new Socket(InetAddress.getByName("localhost"), 3000);
        tcp.sendFile("TCP_Test.iml","asd.txt",socket);
    }
}
