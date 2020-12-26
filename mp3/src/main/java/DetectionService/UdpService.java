package DetectionService;//@author: Haohan Cheng
import Entity.Machine;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

//this class is used for methods related to UDP connection
public class UdpService {
    //use buffer to store the string we received

    Logger log = Logger.getLogger(UdpService.class.getName());


    public void send(Machine machine, DatagramSocket socket, InetAddress targetAddress, int targetPort) throws IOException{
        Gson gson= new Gson();
        ConcurrentHashMap members=machine.getMembers();
        String jsonStr=gson.toJson(members);
        byte[] buf = jsonStr.getBytes();
        DatagramPacket packet=new DatagramPacket(buf,buf.length,targetAddress,targetPort);

        double sendByte = packet.getLength();
        // log.info("Send package of size: " + sendByte + " bytes.");
        machine.setSendTotal(sendByte + machine.getSendTotal());

        socket.send(packet);
    }

    // Static method:  Get the IP address of introducer (default: node 10)
    public static String getIntroducerIP() throws UnknownHostException {
        InetAddress introducer = InetAddress.getLocalHost();
//
//         InetAddress introducer = InetAddress.getByName("fa20-cs425-g28-10.cs.illinois.edu");

        return introducer.getHostAddress();
    }

    //join the network, send the message to introducer
    public void join(Machine machine) throws IOException {

        DatagramSocket socket=new DatagramSocket();
        //find the introducer's host and port
        //InetAddress targetAddress=InetAddress.getByName("localhost");
        String introducerIP = UdpService.getIntroducerIP();
        InetAddress targetAddress=InetAddress.getByName(introducerIP);

        int targetPort = 4445;
        send(machine,socket,targetAddress,targetPort);
        socket.close();
    }

    //listen to other's heartbeat
    public void listen(Machine machine) throws IOException {
        DatagramSocket socket = new DatagramSocket(machine.getPort());

        byte[] buf = new byte[51200];
        while (machine.isRunning()){
            DatagramPacket packet = new DatagramPacket(buf,buf.length);
            socket.receive(packet);
            double receiveByte = packet.getLength();
            // System.out.println("Receive bytes: " + receiveByte);
            String received
                    = new String(packet.getData(), 0, (int) receiveByte);

            log.debug("Received string: " + received);

            // Defined package loss rate
            double loss_rate = machine.getErrorRate();
            double random_num = Math.random();

            // If random less than loss_rate, then discard the package.
            if(!received.equals("") && random_num >= loss_rate) {
                //store the receive string in the buffer list
                machine.getBuffer().add(received);
                //we abadon some early list
                if (machine.getBuffer().size() > 10) {
                    machine.getBuffer().removeFirst();
                }

                // log.info("Receive package of size: " + receiveByte + " bytes.");
                machine.setReceiveTotal(receiveByte + machine.getReceiveTotal());
                log.debug(machine.getPort()+"received packet");
            }
        }
        socket.close();
    }


    // send message to others

    public void switch_mode(Machine machine) throws IOException {
        machine.setIf_boardcast(!machine.isIf_boardcast());
        HashMap<String,HashMap<String,String>> map= new HashMap<String, HashMap<String, String>>();
        HashMap<String,String> holder=new HashMap<String, String>();
        holder.put("1","1");
        if (machine.isIf_boardcast()==true){
            map.put("broadcast",holder);
        }
        else
            map.put("gossip",holder);

        DatagramSocket socket=new DatagramSocket();

        Gson gson= new Gson();
        String jsonStr=gson.toJson(map);
        byte[] buf = jsonStr.getBytes();
        for (Map.Entry<String, HashMap> entry : machine.getMembers().entrySet()) {
            // Entry ID
            String id = entry.getKey();
            // Self
            HashMap<String,String> tmp=entry.getValue();
            if(id.equals(machine.getId())){continue;}

            // Get dict info
            InetAddress targetAddress = InetAddress.getByName(tmp.get("address"));
            int targetPort = Integer.parseInt(tmp.get("port"));
            DatagramPacket packet=new DatagramPacket(buf,buf.length,targetAddress,targetPort);
            socket.send(packet);
            socket.send(packet);
        }
        socket.close();
    }


    // here we use a leave to determine whether a machine will leave the group, if leave== False,
    //then we add the counter, if leave==True, we set counter to -1 and send the table
    // if_boardcast: If we want to use boardcast or Gossip
    public void gossip(Machine machine, boolean leave) throws IOException {
        boolean if_boardcast=machine.isIf_boardcast();
        log.debug("gossip"+machine.getPort());

        //transfer to json
        DatagramSocket socket = new DatagramSocket();
        Gson gson = new Gson();

        // update the counter number here
        ConcurrentHashMap<String, HashMap> members = machine.getMembers();
        HashMap<String, String> member = members.get(machine.getId());
        List<String> toAddCleanList = new LinkedList<String>();

        if (!leave)
            machine.setCounter(machine.getCounter() + 1);
        if (leave)
            machine.setCounter(-1);

        member.put("counter", String.valueOf(machine.getCounter()));
        String jsonStr = gson.toJson(members);
        byte[] buf = jsonStr.getBytes();

        //iterate members list, and select some random to transfer --- NOTE: Exclude self.
        int member_size = members.size() - 1;
        // Number of nodes to which Gossip will send info at each period
        int b_value = 3;
        // --------------------------------------------------------- Modified Gossip Procedure
        if(!if_boardcast && member_size > b_value){
            // Gossip
            // Pick random numbers
            log.debug("Gossip");
            Random rand = new Random();
            HashSet<Integer> sendList = new HashSet<>();
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

            // Gossip to these nodes
            for (Map.Entry<String, HashMap> entry : members.entrySet()) {
                // Entry ID
                String id = entry.getKey();
                // Self
                if(id.equals(machine.getId())){continue;}

                // Get dict info
                Map<String, String> tmp = (Map<String, String>) entry.getValue();
                // Entry that should be cleaned
                if (tmp.get("counter").equals("-1") && !id.equals(machine.getId())){
                    toAddCleanList.add(id);
                }

                // If index matches, send info
                if (sendList.contains(send_counter)){
                    InetAddress targetAddress = InetAddress.getByName(tmp.get("address"));
                    int targetPort = Integer.parseInt(tmp.get("port"));
                    send(machine,socket,targetAddress,targetPort);
                }
                send_counter++;
            }

        } else{
            // Boardcast
            if (member_size <= b_value){
                log.debug("Boardcast (due to insufficient members, less than Gossip target limit)");
            } else {
                log.debug("Boardcast");
            }

            for (Map.Entry<String, HashMap> entry : members.entrySet()) {
                // Entry ID
                String id = entry.getKey();
                // Self
                if(id.equals(machine.getId())){continue;}

                // Get dict info
                Map<String, String> tmp = (Map<String, String>) entry.getValue();
                // Entry that should be cleaned
                if (tmp.get("counter").equals("-1") && !id.equals(machine.getId())){
                    toAddCleanList.add(id);
                }

                // Send to node
                InetAddress targetAddress = InetAddress.getByName(tmp.get("address"));
                int targetPort = Integer.parseInt(tmp.get("port"));
                send(machine,socket,targetAddress,targetPort);
            }
        }

        //--------------------------------------------------------------//


        // ----------------------------------------------------------- Deprecated code
        /*
        for (Map.Entry<String, HashMap> entry : members.entrySet()) {
            // Entry ID
            String id = entry.getKey();
            // Self
            if(id.equals(machine.getId())){continue;}

            // Get dict info
            Map<String, String> tmp = (Map<String, String>) entry.getValue();
            // Entry that should be cleaned
            if (tmp.get("counter").equals("-1") && !id.equals(machine.getId())){
                toAddCleanList.add(id);
            }
            int random = (int) (Math.random() * 10);

            if (random <= 4 || tmp.get("counter").equals("-1")) {
                InetAddress targetAddress = InetAddress.getByName(tmp.get("address"));
                int targetPort = Integer.parseInt(tmp.get("port"));
                send(machine,socket,targetAddress,targetPort);
            }
        }
        */
        // -------------------------------------------------------------

        socket.close();
        //if we find a counter is -1, which means is leaves the group but not the failure, we
        // first gossip to others, then put it to cleanList.
        for (String id :toAddCleanList){
            log.info("add"+id+"to cleanList");
            machine.getCleanList().put(id,members.get(id));
            members.remove(id);
        }




    }


}
