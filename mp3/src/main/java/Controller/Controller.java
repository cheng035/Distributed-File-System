package Controller;

import DetectionService.LocalService;
import DetectionService.UdpService;
import Entity.Machine;
import FileService.FileClientService;
import FileService.FileServerService;
import FileService.LocalFileService;
import MapJuice.FilePartition;
import MapJuice.MapJuiceListener;
import MapJuice.MapleJuiceMaster;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Controller {
    Logger log = Logger.getLogger(Controller.class.getName());
    private Machine machine;
    private LocalService localService;
    private UdpService udpService;
    private LocalFileService localFileService;
    private FileClientService fileClientService;
    private FileServerService fileServerService;

    private MapJuiceListener mapJuiceListener;
    private MapleJuiceMaster mapleJuiceMaster;


    private Thread listenThread;
    private Thread gossipThread;
    private Thread updateThread;
    private Thread joinThread;
    // private Thread failThread;
    private Thread cleanThread;
    private Thread removeThread;
    private Thread fileThread;

     // Map juice
    private Thread mapThread;

    public void intialize(int port, double errorRate) throws IOException {
        this.localService=new LocalService();
        this.udpService=new UdpService();
        this.machine=this.localService.initialize(port, errorRate);
        System.out.println(this.machine.getPort());
        this.localFileService = new LocalFileService(this.machine);
        this.fileClientService = new FileClientService();
        this.fileServerService = new FileServerService();
        this.mapJuiceListener = new MapJuiceListener();
        this.mapleJuiceMaster = new MapleJuiceMaster();
    }

    public void start() throws IOException, InterruptedException {
        listenThread= new Thread(new Listen());
        gossipThread = new Thread(new Gossip());
        updateThread = new Thread(new Update());
        joinThread   = new Thread(new Join());
        cleanThread = new Thread(new Clean());
        removeThread = new Thread(new Remove());
        fileThread = new Thread(new FileSystem());
        mapThread = new Thread(new MapListen());
        // failThread = new Thread(new Fail());
        listenThread.start();
        gossipThread.start();
        updateThread.start();
        if (machine.getPort()!=4445)
            joinThread.start();
        cleanThread.start();
        removeThread.start();
        fileThread.start();

        this.localFileService.newJoinMasterSelection(machine);
        // failThread.start();
        mapThread.start();
    }

    public void displaySelfID () {
        System.out.println("Self ID: " + machine.getId());
    }

    public void switch_mode() throws IOException{
        udpService.switch_mode(machine);
        log.info("Switch the mode to broadcast:"+machine.isIf_boardcast());
    }

    public void leave() throws IOException {
        log.info("leaving the system");
        udpService.gossip(machine,true);
        machine.setRunning(false);
    }
    //map reduce operations --------------------
    public void map(String fileName, String mapName,int map_size ) throws IOException, InterruptedException {
        log.info("start the map ");
        mapleJuiceMaster.maple(fileName,mapName,map_size,machine);

    }

    public void map2(String fileName, String mapName,int map_size ) throws IOException, InterruptedException {
        log.info("start the map ");
        mapleJuiceMaster.maple2(fileName,mapName,map_size,machine);

    }

//reduce

    public void reduce(String fileName, String targetName, int reduce_size) throws IOException, InterruptedException {
        mapleJuiceMaster.reduce(fileName,targetName,reduce_size,machine);
    }

    public void reduce2(String fileName1,String fileName2,String targetName,int reduce_size) throws IOException, InterruptedException {
        mapleJuiceMaster.reduce2(fileName1,fileName2,targetName,reduce_size,machine);
    }

    public void reduce3(String fileName1,String fileName2,String targetName,int reduce_size) throws IOException, InterruptedException {
        mapleJuiceMaster.reduce3(fileName1,fileName2,targetName,reduce_size,machine);
    }


    // File system operations --------------
    public void getFile(String localFileName, String sdfsFileName) throws IOException, InterruptedException {
        try{
        boolean result = this.fileClientService.getFile(this.machine, sdfsFileName, localFileName,true);
        if (result) {
            log.info("Get file: " + sdfsFileName);
        } else {
            log.warn("File not exist");
        }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void putFile(String localFileName, String sdfsFileName) throws IOException, InterruptedException {
        log.info("Put file: " + sdfsFileName);
        this.fileClientService.putFile(this.machine, sdfsFileName, localFileName);
    }

    public void deleteFile(String sdfsFileName) throws IOException {
        log.info("Delete file: " + sdfsFileName);
        this.fileClientService.deleteFile(this.machine, sdfsFileName);
    }

    public void listFileNodes(String sdfsFileName)  throws IOException {
        LinkedList<String> members = this.fileClientService.queryFile(this.machine, sdfsFileName);
        if (members == null) {
            log.warn("No such file");
        } else {
            log.info("Members having file " + sdfsFileName + " :");
            int counter = 0;
            for (String member: members){
                System.out.println(counter + " Member " + member);
                counter ++;
            }
        }
    }

    public void listAllLocalFiles() {
        // traverse local sdfs files.
        int counter = 0;
        System.out.println("Local files:");
        for(File file : Objects.requireNonNull(new File("./sdfs_" + machine.getPort()).listFiles())) {
            if (!file.isDirectory()){
                log.info("No. "+counter+", name: "+file.getName());
                counter ++;
            }
        }

        if (counter == 0) {
            log.info("No file in the storage");
        }
    }

    /*
    //this thread use to simulate the fail situation
    private class Fail implements Runnable{
        Thread t;
        int random_num= (int) (Math.random() * 1000);
        String cur_time = getCurrentTime();
        public void run() {
            while(random_num>-1) {
                try {
                    t.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                random_num=(int) (Math.random() * 1000);
            }

            listenThread.stop();
            gossipThread.stop();
            updateThread.stop();
            joinThread.stop();
            cleanThread.stop();
            removeThread.stop();
            log.fatal("The machine "+machine.getId()+" failed.");
        }
    }*/

    private class MapListen implements Runnable{
        public void run() {
            try {
                mapJuiceListener.start(machine);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class Join implements Runnable{
        Thread t;
        public void run(){
            while (machine.isRunning()) {
                try {
                    udpService.join(machine);

                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    if (machine.getBuffer().size()>0)
                    t.sleep(4000);
                    else{t.sleep(1000);}
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Listen implements Runnable{
        public void run() {
            try {
                udpService.listen(machine);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class FileSystem implements Runnable{
        public void run() {
            try {
                fileServerService.start(machine);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private class Update implements Runnable{
        Thread t;
        @SneakyThrows
        public void run() {
            while(machine.isRunning()) {
                try {
                    t.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                List<String> member=localService.updateMembers(machine);
                if(member.size()>0){
                    for (String id:member)
                        fileClientService.tellMaster(machine,id);
                }
            }
        }
    }

    private class Gossip implements Runnable{
        Thread t;
        public void run(){
            while(machine.isRunning()) {
                try {
                    udpService.gossip(machine,false);

                    log.debug("Send package of size: " + machine.getSendTotal() + " bytes.");
                    log.debug("Receive package of size: " + machine.getReceiveTotal() + " bytes.");
                    machine.setTotalBytes(machine.getSendTotal() + machine.getReceiveTotal());
                    log.debug("Total Bytes: " + machine.getTotalBytes() + " bytes.");

                    t.sleep(1000);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Clean implements Runnable{
        Thread t;
        public void run(){
            while(machine.isRunning()) {
                try {
                    localService.addToCleanList(machine);
                    t.sleep(500);
                } catch (InterruptedException | ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Remove implements Runnable{
        Thread t;
        public void run(){
            while(machine.isRunning()) {
                try {
                    int result = localService.remove(machine);
                    if (result != 0) {
                        localFileService.detectedFailure(machine, result);
                    }
                    t.sleep(500);
                } catch (InterruptedException | IOException | ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void displayCurrentMembers(){
        this.machine.displayMembers();
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat();// set time format
        sdf.applyPattern("yyyyMMddHHmmss");
        Date date = new Date();// get time
        String time = String.valueOf(sdf.format(date));
        return time;
    }

    public Machine getMachine() {
        return this.machine;
    }

    public void setMachine(Machine machine) {
        this.machine = machine;
    }

    public LocalService getLocalService() {
        return localService;
    }

    public void setLocalService(LocalService localService) {
        this.localService = localService;
    }

    public UdpService getUdpService() {
        return udpService;
    }

    public void setUdpService(UdpService udpService) {
        this.udpService = udpService;
    }

    public LocalFileService getLocalFileService() {
        return localFileService;
    }

    public void setLocalFileService(LocalFileService localFileService) {
        this.localFileService = localFileService;
    }
}
