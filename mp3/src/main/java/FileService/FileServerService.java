package FileService;

import DetectionService.UdpService;
import Entity.Machine;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// file service is used to
public class FileServerService {
    private ServerSocket serverSocket;
    Logger log = Logger.getLogger(UdpService.class.getName());
    public void start(Machine machine) throws IOException {
        serverSocket = new ServerSocket(machine.getFilePort());
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
        private TcpService tcpService = new TcpService();
        Logger log = Logger.getLogger(UdpService.class.getName());

        public ClientHandler(Socket socket, Machine machine) {
            this.clientSocket = socket;
            this.machine=machine;
        }

        @SneakyThrows
        public void run(){
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
            String[] strs=inputLine.split(" ");
            String command = strs[0];
            System.out.println("Received command: " + inputLine);

            // ------------------------------------------------------------ Command Execution Section
            // use to fetch the file in local
            if (command.equals("fetch")){
                log.info("receive command fetch");
                String filename=strs[1];

                try {
                    tcpService.sendFile("./sdfs_" + machine.getPort() + "/" + filename,
                            filename, clientSocket);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //use in master side, to search the location of the file
            else if(command.equals("query")){
                log.info("receive command query");
                String filename=strs[1];
                ConcurrentHashMap<String, HashSet<String>> locations=machine.getFileStore();
                HashSet<String> memberList=locations.get(filename);

                if (memberList == null) {
                    log.info("No such file");
                } else {
                    for (String member: memberList){
                        System.out.println(member);
                        String[] id_array=member.split(" ");
                        // send addr and port number
                        int filePort=Integer.parseInt(id_array[0])+1;
                        out.println(filePort+" "+id_array[1]);
                    }
                }

                /*
                int n = locations.size();
                final long l = System.currentTimeMillis();
                int num = (int) (l%(n-1));
                String id = memberList.get(num);
                String[] id_array=id.split(" ");
                */

                // 'end' notation
                out.println(".");
            }

            // use in master side, to put new file
            // File source should send request as "put sdfsFilename sourcePort sourceAddr sourceTimeStamp"
            else if(command.equals("put")){
                log.info("receive command put");
                String sdfsFileName=strs[1];
                ConcurrentHashMap<String, HashSet<String>> fileStore= machine.getFileStore();
                // Update current file
                if (fileStore.containsKey(sdfsFileName)) {
                    HashSet<String> lists = fileStore.get(sdfsFileName);
                    for (String m: lists){
                        out.println(m);
                    }
                    out.println('.');
                }

                else{
                    // Store the new file to randomly selected members
                    // ID for the source node
                    String sourceID = strs[2] + " " + strs[3] + " " + strs[4];
                    ConcurrentHashMap<String, HashMap> members = machine.getMembers();
                    ConcurrentHashMap<String, HashMap> membersWithoutSource =
                            new ConcurrentHashMap<String, HashMap>(members);
                    membersWithoutSource.remove(sourceID);
                    System.out.println("count1: "+members.size()+" count2: " + membersWithoutSource.size());

                    HashSet<String> lists=new HashSet<>();
                    int member_size = membersWithoutSource.size();
                    // Number of nodes to which Gossip will send info at each period
                    int b_value = Math.min(3, member_size);    // The sender itself should also own a copy

                    Random rand = new Random();
                    HashSet<Integer> sendList = new HashSet<Integer>();
                    int send_counter = 0;

                    // Generate random node indexes
                    while(true){
                        int num = rand.nextInt(member_size);
                        if(sendList.contains(num)){
                            continue;
                        } else {
                            sendList.add(num);
                        }
                        if(sendList.size() >= b_value){
                            break;
                        }
                    }

                    // Ask the corresponding nodes to retrieve the file
                    for (Map.Entry<String, HashMap> entry : membersWithoutSource.entrySet()) {
                        // Entry ID
                        // Format: "port addr time_stamp"
                        String id = entry.getKey();
                        // If index matches, send info
                        if (sendList.contains(send_counter)){
                            // send addr and port number
                            out.println(id);
                        }
                        send_counter++;
                    }
                    out.println(".");

                    while(true){
                        try {
                           inputLine = in.readLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        if(inputLine.equals(".") || inputLine == null)
                            break;
                        lists.add(inputLine);

                    }
                    // Add the replica from the source node (which sends out the 'put' request)
                    lists.add(sourceID);
                    fileStore.put(sdfsFileName,lists);
                }
            }

            // --- BEING ASKED --- to receive the file --- format: "receive sdfsFileName"
            else if (command.equals("receive")){
                log.info("receive command receive");
                String sdfsName=strs[1];
                // file source IP & port
                // String sourceAddr = strs[2];
                // String sourcePort = strs[3];

                try {
                    // receive file given the source.
                    tcpService.receiveFile(clientSocket, sdfsName, false, machine);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            // use in master side, to delete the files
            else if(command.equals("delete")){
                log.info("receive command delete");
                String sdfsFileName=strs[1];

                // Send delete request
                ConcurrentHashMap<String, HashSet<String>> fileStore= machine.getFileStore();
                if (fileStore.containsKey(sdfsFileName)) {
                    HashSet<String> lists = fileStore.get(sdfsFileName);

                    // For members in this list, ask them to delete this file
                    for (String node: lists){
                        String[] node_info = node.split(" ");
                        int port = Integer.parseInt(node_info[0])+1;
                        String targetAddress = node_info[1];

                        if (targetAddress.equals(machine.getAddress().toString())) {
                            // self -- then delete the file locally
                            try {
                                FileClientService.deleteLocalFile(machine, sdfsFileName);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            // Send individual requests to these members, ask them to delete this file.
                            try {
                                Socket targetSocket = new Socket(targetAddress, port);
                                PrintWriter targetOut = new PrintWriter(targetSocket.getOutputStream(), true);
                                BufferedReader targetIn = new BufferedReader(new InputStreamReader(targetSocket.getInputStream()));
                                // send remove request
                                targetOut.println("remove "+sdfsFileName);

                                targetIn.close();
                                targetOut.close();
                                targetSocket.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    // Delete the file entry in the list
                    fileStore.remove(sdfsFileName);
                    out.println("ack");
                } else {
                    out.println("null");
                }
            }

            // use in all nodes, delete actual files
            else if (command.equals("remove")){
                log.info("receive command remove");
                String sdfsFileName=strs[1];
                // Receive the 'remove' request from master, delete the file locally.
                try {
                    FileClientService.deleteLocalFile(machine, sdfsFileName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // ask a node to fetch the file put it into sdfs archive
            else if (command.equals("fch")){
                log.info("receive command fch");
                String fileName = strs[1];
                FileClientService service=new FileClientService();
                try {
                    service.getFile(machine,fileName,fileName,false);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }



            else if (command.equals("election")){
                log.info("receive command election");
                StringBuffer buffer= new StringBuffer();
                for(int i=1;i<strs.length;i++){
                    buffer.append(strs[i]);
                    if (i!=strs.length-1)
                        buffer.append(' ');
                }

                String id = buffer.toString();

                while (true) {
                    try {
                        if ((inputLine = in.readLine()) == null) break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (".".equals(inputLine)) {
                        break;
                    }
                    if(machine.getFileStore().containsKey(inputLine)) {
                        machine.getFileStore().get(inputLine).add(id);
                    }
                    else{
                        machine.getFileStore().put(inputLine,new HashSet<String>());
                        machine.getFileStore().get(inputLine).add(id);
                    }

                }

            }
            // others tell that he's the master
            else if(command.equals("master")){
                    log.info("receive master command, current master is "+strs[1]+strs[2]+strs[3]);
                    machine.setMasterPort(Integer.valueOf(strs[1])+1);
                    machine.setMasterAddress(strs[2]);
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
