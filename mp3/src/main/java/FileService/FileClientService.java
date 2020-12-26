package FileService;

import DetectionService.UdpService;
import Entity.Machine;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class FileClientService {

    // file client use to send the quest to other nodes.
    TcpService tcpService = new  TcpService();;
    static Logger log = Logger.getLogger(FileClientService.class.getName());


    public boolean putFile(Machine machine, String sdfsFileName, String localFileName) throws IOException, InterruptedException {
        PrintWriter out;
        BufferedReader in;
        boolean flag = false;
        String targetAddress=machine.getMasterAddress();
        int port =machine.getMasterPort();
        try {
            Socket clientSocket = new Socket(targetAddress, port);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            long start = System.currentTimeMillis();

            //send the file name to master node
            out.println("put " + sdfsFileName + " " + machine.getId());
            //get the respond information

            //fetch the location of the file
            String inputLine;
            LinkedList<String> target = new LinkedList<>();

            while ((inputLine = in.readLine()) != null) {
                if (".".equals(inputLine)) {
                    break;
                } else
                    target.add(inputLine);
            }

            System.out.println(target.toString());

            for (String member : target) {
                String[] res = member.split(" ");
                String fileAddress = res[1];
                int filePort = Integer.parseInt(res[0]) + 1;
                try {
                    Socket socket = new Socket(fileAddress, filePort);

                    if (tcpService.sendFile(localFileName, sdfsFileName, socket)) {
                        out.println(member); // tell the server the file is sent
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 'end' notation
            out.println(".");

            // Maintain a local copy
            File localFile = new File(localFileName);
            Files.copy(localFile.toPath(), (new File("./sdfs_" + machine.getPort() + "/" + sdfsFileName)).toPath(),
                    StandardCopyOption.REPLACE_EXISTING);

            log.info("PUT Time consuming: " + (System.currentTimeMillis() - start) + " ms");

            //get the file based on the location
            in.close();
            out.close();
            clientSocket.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return true;

    }

    // This method for query files with name.
    public LinkedList<String> queryFile(Machine machine, String sdfsFileName) throws IOException {
        PrintWriter out;
        BufferedReader in;
        boolean flag = false;
        String targetAddress=machine.getMasterAddress();
        int port =machine.getMasterPort();
        Socket clientSocket = new Socket(targetAddress,port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //send the file name to master node
        out.println("query "+sdfsFileName);

        String inputLine;
        LinkedList<String> members = new LinkedList<>();
        while ((inputLine = in.readLine()) != null) {
            if (".".equals(inputLine)) {
                break;
            }
            else
                members.add(inputLine);
        }

        // file does not exist
        if (members.size() == 0){
            in.close();
            out.close();
            clientSocket.close();
            return null;
        }

        in.close();
        out.close();
        clientSocket.close();
        return members;
    }

    // use this method to find the file location, and then get the file

    public boolean getFile(Machine machine, String sdfsFileName, String localName, boolean active) throws IOException, InterruptedException {
        PrintWriter out;
        BufferedReader in;
        boolean flag = false;
        try{

        String targetAddress=machine.getMasterAddress();
        int port =machine.getMasterPort();
        Socket clientSocket = new Socket(targetAddress,port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //send the file name to master node
        out.println("query "+sdfsFileName);

        // receive the member list
        String inputLine;
        LinkedList<String> members = new LinkedList<>();
        while ((inputLine = in.readLine()) != null) {
            if (".".equals(inputLine)) {
                break;
            }
            else
                members.add(inputLine);
        }

        // file does not exist
        if (members.size() == 0){
            in.close();
            out.close();
            clientSocket.close();
            return false;
        }

        // Choose a random number
        int n = members.size();
        final long l = System.currentTimeMillis();
        int num = (int) (l%(n));
        String resp = members.get(num);

        //fetch the location of the file
        String[] res=resp.split(" ");

        long start = System.currentTimeMillis();
        String fileAddress = res[1];
        int filePort = Integer.parseInt(res[0]);

        //get the file based on the location
        try {
            Socket newSocket = new Socket(fileAddress, filePort);
            PrintWriter newOut = new PrintWriter(newSocket.getOutputStream(), true);
            BufferedReader newIn = new BufferedReader(new InputStreamReader(newSocket.getInputStream()));
            newOut.println("fetch " + sdfsFileName);


            while (!newIn.readLine().split(" ")[0].equals("receive")) {
                System.out.print("");
                Thread.sleep(10);
            }

            if (tcpService.receiveFile(newSocket, localName, active, machine)) {
                out.println("ack");
                flag = true;
            }

            log.info("GET Time consuming: " + (System.currentTimeMillis() - start) + " ms");

            newOut.close();
            in.close();
            out.close();
            clientSocket.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    public boolean deleteFile(Machine machine, String sdfsFileName) throws IOException {
        PrintWriter out;
        BufferedReader in;
        boolean flag = false;
        String targetAddress=machine.getMasterAddress();
        int port =machine.getMasterPort();
        Socket clientSocket = new Socket(targetAddress,port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //send the file name to master node
        out.println("delete "+sdfsFileName);
        //get the respond information
        String resp = in.readLine();
        if (resp.equals("null")) {
            log.warn("No such file");
        } else {
            log.info("Success in deleting: " + sdfsFileName);
        }

        in.close();
        out.close();
        clientSocket.close();
        return flag;
    }

    public static void deleteLocalFile(Machine machine, String sdfsFileName) throws IOException {
        // delete local files
        String filePath = "./sdfs_" + machine.getPort() + "/" + sdfsFileName;
        boolean result = Files.deleteIfExists(Paths.get(filePath)); // delete the file
        System.out.println("File: " + sdfsFileName + " removing, result: " + result);
    }


    //send the current files to master
    public static void sendFileList(Machine machine) throws IOException {
        log.info("send the file list");
        PrintWriter out;
        BufferedReader in;
        boolean flag = false;
        String targetAddress=machine.getMasterAddress();
        int port =machine.getMasterPort();
        Socket clientSocket = new Socket(targetAddress,port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //send the file name to master node
        out.println("election "+machine.getId());


        for(File file : Objects.requireNonNull(new File("./sdfs_" + machine.getPort()).listFiles())) {
            if (!file.isDirectory()){
                out.println(file.getName());
            }
        }
        out.println(".");
        in.close();
        out.close();

    }
//ask other nodes to fetch one file put it into sdfs.
    public boolean askToFetch(String randomKey, String fileName ) throws IOException {
        PrintWriter out;
        BufferedReader in;
        String[] strs = randomKey.split(" ");
        int port = Integer.valueOf(strs[0])+1;
        String address = strs[1];
        Socket clientSocket = new Socket(address,port);
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out.println("fch "+fileName);

        return true;

    }
    //used by a master node, tell other point that this node is the master
    public void tellMaster(Machine machine,String id) throws IOException {
        try{
        log.info("tell "+id+"I'm the master");
        String[] strs=id.split(" ");
        int port = Integer.valueOf(strs[0])+1;
        String address=strs[1];
        Socket socket=new Socket(address,port);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println("master "+ machine.getId());}
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
