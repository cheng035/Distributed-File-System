package Entity;

import DetectionService.UdpService;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Machine {
    private String id;
    private int counter;
    private ConcurrentHashMap<String, HashMap> members;
    private int port;
    private InetAddress address;
    private ConcurrentHashMap<String, HashMap> cleanList;
    private LinkedList<String> buffer = new LinkedList<String>(); //store the received string
    private volatile boolean running;
    private double errorRate;

    private int total_errors;
    // Statistic of bandwidth
    private double receiveTotal;
    private double sendTotal;
    private double totalBytes;
    private boolean if_boardcast;

    public boolean isIf_master() {
        return if_master;
    }

    public void setIf_master(boolean if_master) {
        this.if_master = if_master;
    }


    //attribute used for file
    // flag for master identification
    private boolean if_master;

    private int filePort;// Use this port to listen to other node's requests


    private String masterAddress;
    private int masterPort;// Record the current master Port
    private ConcurrentHashMap<String, HashSet<String>> fileStore;// record the file location,string is the first name, list is the location list
//    private ConcurrentHashMap<String,ArrayList<String>> MembersFiles; // which member have which files

    private HashSet<String> removedMembers;//record which member is removed
    public HashSet<String> getRemovedMembers() {
        return removedMembers;
    }



    public Machine(String id, int port, InetAddress address, double errorRate) throws IOException {
        this.id=id;
        this.counter=1;
        this.members=new ConcurrentHashMap<String, HashMap>();
        this.port=port;
        this.address=address;
        this.running=true;
        this.buffer=new LinkedList<String>();
        this.cleanList=new ConcurrentHashMap<String, HashMap>();
        this.if_boardcast=false;
        this.errorRate = errorRate;
        this.receiveTotal = 0;
        this.sendTotal = 0;
        this.totalBytes = 0;
        //file systems
        this.filePort = port + 1;
        this.if_master = false;
        this.masterPort=-1;
        this.removedMembers=new HashSet<>();
        this.fileStore = new ConcurrentHashMap<String, HashSet<String>>();
//        this.MembersFiles = new ConcurrentHashMap<String,ArrayList<String>>();
        this.masterAddress = "";

    }

    // display current members
    public void displayMembers(){
        Enumeration keys = this.members.keys();
        System.out.println("Current members: ");
        int mem_counter = 0;
        // Displaying the Enumeration
        while (keys.hasMoreElements()) {
            mem_counter++;
            System.out.println("Number " + mem_counter + " member: " + keys.nextElement());
        }
        System.out.println("Total members: " + mem_counter);
    }



    // Getter and setters ------------------------------------

    public int getTotal_errors() {
        return total_errors;
    }
    public void setTotal_errors(int total_errors) {
        this.total_errors = total_errors;
    }
    public double getTotalBytes() {
        return this.totalBytes;
    }

    public void setTotalBytes(double totalBytes) {
        this.totalBytes = totalBytes;
    }

    public double getSendTotal() {
        return sendTotal;
    }

    public void setSendTotal(double sendTotal) {
        this.sendTotal = sendTotal;
    }

    public double getReceiveTotal() {
        return receiveTotal;
    }

    public void setReceiveTotal(double receiveTotal) {
        this.receiveTotal = receiveTotal;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }

    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public synchronized int getCounter() {
        return counter;
    }

    public synchronized void setCounter(int counter) {
        this.counter = counter;
    }

    public void setMembers(ConcurrentHashMap<String, HashMap> members) {
        this.members = members;
    }

    public ConcurrentHashMap<String, HashMap> getMembers() {
        return members;
    }


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    public ConcurrentHashMap<String, HashMap> getCleanList() {
        return cleanList;
    }

    public synchronized LinkedList<String> getBuffer() {
        return buffer;
    }

    public void setBuffer(LinkedList<String> buffer) {
        this.buffer = buffer;
    }
    public void setCleanList(ConcurrentHashMap<String, HashMap> cleanList) {
        this.cleanList = cleanList;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }


    public boolean isIf_boardcast() {
        return if_boardcast;
    }

    public void setIf_boardcast(boolean if_boardcast) {
        this.if_boardcast = if_boardcast;
    }

    public int getFilePort() {
        return filePort;
    }

    public void setFilePort(int filePort) {
        this.filePort = filePort;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public ConcurrentHashMap<String, HashSet<String>> getFileStore() {
        return fileStore;
    }

//
//    public ConcurrentHashMap<String, ArrayList<String>> getMembersFiles() {
//        return MembersFiles;
//    }
//
//    public void setMembersFiles(ConcurrentHashMap<String, ArrayList<String>> membersFiles) {
//        MembersFiles = membersFiles;
//    }


}
