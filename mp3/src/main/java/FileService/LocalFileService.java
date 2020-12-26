package FileService;


import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import DetectionService.UdpService;
import Entity.Machine;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.log4j.Logger;

public class LocalFileService {
    // File operations ---------------------------------------
    private final FileClientService fileClientService;
    private final Machine machine;
    Logger log = Logger.getLogger(LocalFileService.class.getName());

    public LocalFileService(Machine machine) throws IOException {
        this.fileClientService = new FileClientService ();
        this.machine = machine;
        this.createCacheFolder();
        this.recoverFromFailure();
    }

    public void newJoinMasterSelection(Machine machine) throws IOException, InterruptedException {
        Thread.sleep(5000); // wait the master to contact itself
        if(machine.getMasterPort()!=-1)
            return;
        else
            electMaster(machine,true);

    }

    // Create local file cache folder
    public void createCacheFolder() throws IOException {
        Files.createDirectories(Paths.get("./sdfs_" + this.machine.getPort()));
    }

    public void recoverFromFailure(){
        // Wipe the sdfs files before failure
        // traverse the directory, and remove the files.
        for(File file : Objects.requireNonNull(new File("./sdfs_" + machine.getPort()).listFiles())){
            if (!file.isDirectory()){
                boolean result = file.delete();
            }
        }
    }

    public void detectedFailure(Machine machine, int flag) throws IOException, InterruptedException {
        log.info("detected,failure, try to copy the files");
        if(flag==1){
            log.info("new master selecting");
            electMaster(machine,false);

        }

        if(machine.isIf_master()==true){
            Thread.sleep(1000);
            log.info("put the file into new nodes");
            // check the the number of nodes of each file;
            for (Map.Entry<String, HashSet<String>> entry : machine.getFileStore().entrySet()) {
                HashSet<String> nodes=entry.getValue();
                for (String m:machine.getRemovedMembers())
                    if(nodes.contains(m))
                        nodes.remove(m);
                int b_value=Math.min(4,machine.getMembers().size()+1);
                while(nodes.size()<b_value){
                    ConcurrentHashMap members=machine.getMembers();
                    String[] keys = (String[]) members.keySet().toArray(new String[0]);
                    Random random = new Random();
                    String randomKey = keys[random.nextInt(keys.length)];
                    if(nodes.contains(randomKey))
                        continue;
                    else{
                        if(fileClientService.askToFetch(randomKey,entry.getKey())==true)
                            log.info("send file to"+entry.getKey());
                             Thread.sleep(1500);
                            nodes.add(randomKey);
                    }
                }

            }

            machine.getRemovedMembers().clear();
        }

    }

    public void electMaster(Machine machine, boolean is_start) throws IOException {
        String newMasterid = selectMaxId(machine);
        String[] strs=newMasterid.split(" ");
        int filePort=Integer.valueOf(strs[0])+1;
        String fileAddress=strs[1];

        machine.setMasterAddress(fileAddress);
        machine.setMasterPort(filePort);

        if (newMasterid==machine.getId()) {
            machine.setIf_master(true);
            FileClientService.sendFileList(machine);
            log.info("this node become the new master");
        }
        else if(is_start==false) {
            FileClientService.sendFileList(machine);
            log.info("the new master is" + newMasterid);
        }
    }

        public String selectMaxId(Machine machine){
        ConcurrentHashMap<String, HashMap> members = machine.getMembers();
        String maxId = "";

        System.out.println(members.size());
        for (Map.Entry<String, HashMap> entry : members.entrySet()) {
            System.out.println("current"+entry.getKey());
            System.out.println("max"+maxId);

            if (maxId.compareTo(entry.getKey()) < 0) {
                maxId = entry.getKey();
            }
        }
        return maxId;
    }


}
