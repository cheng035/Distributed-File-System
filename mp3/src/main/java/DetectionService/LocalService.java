package DetectionService;

import Entity.Machine;
import FileService.FileClientService;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LocalService {
    Logger log = Logger.getLogger(Machine.class.getName());

    // start a new machine use this service, configuring port, initialize member table, counter
    public Machine initialize(int port, double errorRate) throws IOException {
        InetAddress address = InetAddress.getByName(getIpAddress());
        log.info("the address is" +String.valueOf(address));
        String id=generateId(address,port);
        Machine machine=new Machine(id, port, address, errorRate);
        HashMap<String,String> map = new HashMap<String, String>();
        String addressString=address.toString();
        addressString=addressString.substring(1,addressString.length());
        //put id and a new counter into a new map
        map.put("counter","1");
        map.put("address",addressString);
        map.put("port",String.valueOf(machine.getPort()));
        map.put("time",getCurrentTime());
        //put this map to member list
        ConcurrentHashMap<String, HashMap> members=machine.getMembers();
        members.put(machine.getId(),map);
        return machine;
    }

    //write buffer data to member
    public List<String> updateMembers(Machine machine) {
        LinkedList<String> received=machine.getBuffer();
        LinkedList<String> addedMember=new LinkedList<>();
        Gson gson = new Gson();
        if (received.size()==0)
            return addedMember;

        int n = received.size() -1;
        // For all the received information
        for (int i = n; i >=0; i-- ){
            LinkedTreeMap<String,LinkedTreeMap>  receivedMembers=gson.fromJson(received.removeLast(), LinkedTreeMap.class);
            ConcurrentHashMap<String, HashMap> members = machine.getMembers();
            ConcurrentHashMap<String, HashMap> cleanList = machine.getCleanList();
            String time = getCurrentTime();

            //check if this is a switch information
            if (receivedMembers.containsKey("broadcast")){
                    machine.setIf_boardcast(true);
                log.info("switch the mode"+"broadcast:"+"broadcast");
                    continue;
            }
            if(receivedMembers.containsKey("gossip")){
                    machine.setIf_boardcast(false);
                log.info("switch the mode"+"broadcast:"+"gossip" );
                continue;
            }

            for (Map.Entry<String, LinkedTreeMap> entry : receivedMembers.entrySet()) {
                String id = entry.getKey();

                if(id==machine.getId()){
                    continue;
                }

                log.debug("Update from: "+id);
                Map<String, String> receivedMap = (Map<String, String>) entry.getValue();
                int counter = Integer.valueOf(receivedMap.get("counter"));
                //add a new record to our member table
                if (!members.containsKey(id)&& !cleanList.containsKey(id)&&counter!=-1 ) {
                    log.info("new member join"+id);

                    if(machine.isIf_master()==true)
                        addedMember.add(id);



                    receivedMap.put("time", time);
                    HashMap<String,String> newMap = new HashMap<String, String>();
                    for(Map.Entry<String,String> entry1 : receivedMap.entrySet() ){
                        newMap.put(entry1.getKey(),entry1.getValue());
                    }
                    members.put(id,newMap);
                    //remove the member in removed list
                    if(machine.getRemovedMembers().contains(id))
                        machine.getRemovedMembers().remove(id);

                }

                else if(counter==-1&&members.containsKey(id)){
                    HashMap<String,String> member = members.get(id);
                    member.put("counter","-1");
                }

                else if(members.containsKey(id)) {  //check if the counter is updated

                    HashMap<String, String> member = members.get(id);
                    int old_counter = Integer.parseInt(member.get("counter"));
                    if (counter > old_counter) {
                        log.debug("Update from："+id);
                        member.put("time", time);
                        member.put("counter", String.valueOf(counter));
                        members.put(id, member);
                    }
                }

                else{//we check if the member in clean list is updated.
                    HashMap<String, String> member = cleanList.get(id);
                    int old_counter = Integer.parseInt(member.get("counter"));

                    if (counter > old_counter && old_counter!=-1) {
                        log.debug("update"+id);
                        member.put("time", time);
                        member.put("counter", String.valueOf(counter));
                        members.put(id, member);
                        cleanList.remove(id);

                    }
                }
            }
            log.debug("Member-list size: "+members.size());
        }
        return addedMember;

    }

    // the local 'suspect' list
    public void addToCleanList(Machine machine) throws ParseException {
        String time=getCurrentTime();
        ConcurrentHashMap<String, HashMap> members=machine.getMembers();
        ConcurrentHashMap<String, HashMap> cleanList=machine.getCleanList();
        for (Map.Entry<String, HashMap> entry : machine.getMembers().entrySet()) {
            String id = entry.getKey();
            HashMap<String, String> member = entry.getValue();
            // if this is the local machine's id or the this member leaves group by itself, we continue
            if (id.equals(machine.getId()) || member.get("counter").equals("-1")) {continue;}


            long difference = getDate(time).getTime() - getDate(member.get("time")).getTime();

            if(difference>3000){
                log.debug("=put the"+id+"to clean list");
                cleanList.put(id,member);
                members.remove(id);

            }
        }
    }

    public int remove(Machine machine) throws ParseException {
        //detect whether a node is failed;
        int flag=0;

        //detect whether a master node is failed;
        int master_flag=0;

        String time=getCurrentTime();
        ConcurrentHashMap<String, HashMap> cleanList=machine.getCleanList();
        if (cleanList.size()==0)
            return 0;

        for (Map.Entry<String, HashMap> entry : cleanList.entrySet()) {
            String id = entry.getKey();

            HashMap<String, String> member = entry.getValue();
            long difference = getDate(time).getTime() - getDate(member.get("time")).getTime();
            if( difference >6000){
                log.info("remove the"+id);
                cleanList.remove(id);
                machine.getRemovedMembers().add(id);
                machine.setTotal_errors(machine.getTotal_errors()+1);
                log.info("current error numbers are"+machine.getTotal_errors());
                flag=-1;
                //if master file is failed.
                if(member.get("address").equals(machine.getMasterAddress())&&Integer.valueOf(member.get("port"))==machine.getMasterPort()-1){
                    master_flag=1;

                    log.info("detected file master fail");
                }
//                log.info(member.get("address"));
//                log.info(member.get("port"));
//                log.info(machine.getMasterAddress());
//                log.info(machine.getMasterPort());



            }

            if (master_flag!=0)
                return 1;
            if(flag==0)
                return 0;
            return -1;
        }

        return flag;
    }

    private String generateId(InetAddress address,int port){ //use to generate a id
        String time=getCurrentTime();
        String addr = address.toString().replace("/", "");
        return port +" "+addr+" "+ time;
    }

    public String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat();// set time format
        sdf.applyPattern("yyyyMMddHHmmss");
        Date date = new Date();// get time
        String time = String.valueOf(sdf.format(date));
        return time;
    }

    private Date getDate(String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat();// set time format
        sdf.applyPattern("yyyyMMddHHmmss");
        return sdf.parse(time);
    }

    private String getIpAddress() throws IOException {

        InetAddress IP = InetAddress.getLocalHost();
        return IP.getHostAddress();

    }
}
