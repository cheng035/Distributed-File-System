package TCPtEST;

import java.io.*;
import java.net.Socket;

public class TcpService {
    public boolean sendFile(String localFileName, String sdfsFileName, Socket clientSocket) throws IOException {
        File file = new File(localFileName);
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        //Get socket's output stream
        OutputStream os = clientSocket.getOutputStream();
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true);
//        out.println("receive "+sdfsFileName);
        //Read File Contents into contents array
        byte[] contents;
        long fileLength = file.length();
        long current = 0;

        long start = System.nanoTime();
        while(current!=fileLength){
            int size = 10000;
            if(fileLength - current >= size)
                current += size;
            else{
                size = (int)(fileLength - current);
                current = fileLength;
            }
            contents = new byte[size];
            bis.read(contents, 0, size);
            os.write(contents);
            System.out.print("Sending file ... "+(current*100)/fileLength+"% complete!");
        }

        os.flush();
        //File transfer done. Close the socket connection!
        clientSocket.close();

        System.out.println("File sent succesfully!");
        return true;
    }



    public boolean receiveFile(Socket socket, String name, boolean if_fetch) throws IOException {

        byte[] contents = new byte[10000];
        PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
        out.println("qweqwe");

        //Initialize the FileOutputStream to the output file's full path.
        FileOutputStream fos = new FileOutputStream(name);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        InputStream is = socket.getInputStream();

        //No of bytes read in one read() call
        int bytesRead = 0;

        while((bytesRead=is.read(contents))!=-1)
            bos.write(contents, 0, bytesRead);

        bos.flush();
        socket.close();

        System.out.println("File saved successfully!");
        return true;
    }
}
