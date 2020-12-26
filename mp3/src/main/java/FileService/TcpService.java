package FileService;

import DetectionService.UdpService;
import Entity.Machine;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class TcpService {
    Logger log = Logger.getLogger(TcpService.class.getName());

    public boolean sendFile(String localFileName, String sdfsFileName, Socket clientSocket) throws IOException, InterruptedException {
        File file = new File(localFileName);
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);
        Logger log = Logger.getLogger(UdpService.class.getName());
        //Get socket's output stream
        OutputStream os = clientSocket.getOutputStream();
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true);
        out.println("receive "+sdfsFileName);
        //Read File Contents into contents array
        byte[] contents;
        long fileLength = file.length();
        long current = 0;

        //
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        while (!in.readLine().equals("ready")) {
            System.out.print("");
            Thread.sleep(10);
        }


        while(current!=fileLength) {
            int size = 10000;
            if (fileLength - current >= size)
                current += size;
            else {
                size = (int) (fileLength - current);
                current = fileLength;
            }
            contents = new byte[size];
            bis.read(contents, 0, size);
            os.write(contents);
            // log.info("Content: " + Arrays.toString(contents));
            if (((int) (current * 100) / fileLength) % 20 == 0) {
                log.debug("Sending file ... " + (current * 100) / fileLength + "% complete!");
            }
        }

        os.flush();
        os.close();
        //File transfer done. Close the socket connection!

        bis.close();
        fis.close();

        clientSocket.close();
        log.debug("File sent succesfully!");
        return true;
    }



    public boolean receiveFile(Socket socket, String name, boolean if_fetch, Machine machine) throws IOException {

        //Initialize the FileOutputStream to the output file's full path.
        String file_path = name;
        if (!if_fetch){
            file_path = "./sdfs_" + machine.getPort() + "/" + name;
        }
        byte[] contents = new byte[10000];
        PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
        out.println("ready");

        // Initialize the FileOutputStream to the output file's full path.
        FileOutputStream fos = new FileOutputStream(file_path);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        InputStream is = socket.getInputStream();

        //No of bytes read in one read() call
        int bytesRead = 0;

        log.info("Receiving file...");
        while(true) {
            bytesRead = is.read(contents);
            if (bytesRead == -1){
                break;
            }
            // System.out.println(bytesRead);
            // System.out.println(Arrays.toString(contents));
            bos.write(contents, 0, bytesRead);
        }

        bos.flush();
        bos.close();
        fos.close();
        socket.close();
        is.close();
        log.info("File saved successfully!");
        return true;
    }


}
