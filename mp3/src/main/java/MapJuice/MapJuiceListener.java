package MapJuice;

import DetectionService.UdpService;
import Entity.Machine;
import FileService.FileClientService;
import FileService.TcpService;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

// This calss is used to receive the command to do the map reduce job
public class MapJuiceListener {
    private ServerSocket serverSocket;
    Logger log = Logger.getLogger(UdpService.class.getName());

    public void start(Machine machine) throws IOException {
        serverSocket = new ServerSocket(machine.getFilePort()+1);
        while (true)
            new ClientHandler(serverSocket.accept(), machine).start();
    }

    public void stop() throws IOException {
        serverSocket.close();
    }

    private static class ClientHandler extends Thread {
        private Socket clientSocket;
        private PrintWriter out;
        private BufferedReader in;
        private Machine machine;
        private FileClientService fileClientService = new FileClientService();

        Logger log = Logger.getLogger(UdpService.class.getName());
        private ReduceService reduceService = new ReduceService();
        public ClientHandler(Socket socket, Machine machine) {
            this.clientSocket = socket;
            this.machine = machine;
        }

        @SneakyThrows
        public void run() {
            try {
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            // get the input here


            String inputLine = null;
            try {
                inputLine = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //handle different input
            String[] strs = inputLine.split(" ");
            String command = strs[0];

            System.out.println("Received command: " + inputLine);

            // ------------------------------------------------------------ Command Execution Section
            // use to fetch the file in local
            if (command.equals("map")) {
                String fileName = strs[1];

                try {
                    in.close();
                    out.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                while(true) {
                   if(fileClientService.getFile(machine, fileName, fileName, true))
                       break;
                }
                MapService.map(fileName,machine,strs[2]);
                File getFile = new File(fileName);
                getFile.delete();
            }




            if(command.equals("map2")){
                String fileName1=strs[1];
                while(true) {
                    if(fileClientService.getFile(machine, fileName1, fileName1, true))
                        break;
                }
                fileClientService.getFile(machine,fileName1,fileName1,true);

                MapService.map2(fileName1,strs[2],machine);
                File file1=new File(fileName1);
                file1.delete();
            }


            if (command.equals("reduce")) {
                String fileName = strs[1];
                while(true) {
                    if(fileClientService.getFile(machine, fileName, fileName, true))
                        break;
                }
                fileClientService.getFile(machine,fileName,fileName,true);
                reduceService.reduce(fileName,machine,strs[2]);
                File getFile = new File(fileName);
                getFile.delete();
            }







            if(command.equals("reduce2")){

                String fileName1=strs[1];
                String fileName2=strs[2];
                while(true) {
                    if(fileClientService.getFile(machine, fileName1, fileName1, true))
                        break;
                }

                while(true) {
                    if(fileClientService.getFile(machine, fileName2, fileName2, true))
                        break;
                }
                fileClientService.getFile(machine,fileName1,fileName1,true);
                fileClientService.getFile(machine,fileName2,fileName2,true);
                Thread.sleep(200);
                reduceService.reduce2(fileName1,fileName2,strs[3],machine);
                File file1=new File(fileName1);
                File file2=new File(fileName2);
                file1.delete();
                file2.delete();
            }



            if(command.equals("reduce3")){
                String fileName1=strs[1];
                String fileName2=strs[2];
                while(true) {
                    if(fileClientService.getFile(machine, fileName1, fileName1, true))
                        break;
                }

                while(true) {
                    if(fileClientService.getFile(machine, fileName2, fileName2, true))
                        break;
                }
                fileClientService.getFile(machine,fileName1,fileName1,true);
                fileClientService.getFile(machine,fileName2,fileName2,true);
                Thread.sleep(200);
                reduceService.reduce3(fileName1,fileName2,strs[3],machine);
                File file1=new File(fileName1);
                File file2=new File(fileName2);
                file1.delete();
                file2.delete();
            }


            try {
                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }


    }

}
