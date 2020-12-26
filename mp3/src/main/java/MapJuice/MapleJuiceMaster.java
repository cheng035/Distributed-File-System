package MapJuice;

import Entity.Machine;
import FileService.FileClientService;
import FileService.TcpService;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
// used in master node, to allocate each node's job
public class MapleJuiceMaster {
    public LinkedList<String> mapList = new LinkedList<>();
    public LinkedList<String> juiceList = new LinkedList<>();
    Logger log = Logger.getLogger(MapleJuiceMaster.class.getName());
    CommandSender sender = new CommandSender();
    String mapName = null;
    String reduceName = null;
    // assign task
    // assign the map task
    int total_count=0;
    //map for the tree calculate
    public void maple(String fileName, String mapName, int map_size, Machine machine) throws IOException, InterruptedException {
        log.info("start the map task");
        LinkedList list = new LinkedList();
        list.add(fileName);
        assign(list, "map", mapName, map_size, machine);
        this.mapName = mapName;
    }

    //map for change the name
    public void maple2(String fileName, String mapName, int map_size, Machine machine) throws IOException, InterruptedException {
        log.info("start the map task");
        LinkedList list = new LinkedList();
        list.add(fileName);
        assign(list, "map2", mapName, map_size, machine);
        this.mapName = mapName;
    }


    //assign the reduce task for tree task
    public void reduce(String fileName, String targetName, int reduce_size, Machine machine) throws IOException, InterruptedException {
        LinkedList list = new LinkedList();
        list.add(fileName);
        assign(list, "reduce", targetName, reduce_size, machine);
    }

    // assign the reduce task to see which area have positive in a peroid of time
    public void reduce2(String fileName1, String fileName2, String targetName, int reduce_size, Machine machine) throws IOException, InterruptedException {
        LinkedList list = new LinkedList();
        list.add(fileName1);
        list.add(fileName2);
        assign(list, "reduce2", targetName, reduce_size, machine);
    }

    public void reduce3(String fileName1, String fileName2, String targetName, int reduce_size, Machine machine) throws IOException, InterruptedException {
        LinkedList list = new LinkedList();
        list.add(fileName1);
        list.add(fileName2);
        assign(list, "reduce3", targetName, reduce_size, machine);
    }

    //@fileName, the original file,
    //@ targetName, the output file after the task

    public void assign(List<String> sourceNames, String taskCommand, String targetName, int segSize, Machine machine) throws IOException, InterruptedException {
        FilePartition partition = new FilePartition();
        FileClientService fileClientService = new FileClientService();
        ConcurrentHashMap<String, HashMap> members = machine.getMembers();

        long start = System.currentTimeMillis();


        if (members.size() - 1 < segSize) {
            segSize = members.size() - 1;
        }
        //get the files

        LinkedList<LinkedList<String>> fileList = new LinkedList<>();

        int file_number = 0;

        for (String sourceFile : sourceNames) {
            String partitionName = "partition_" + total_count;
            total_count+=1;
            System.out.println("sgesize is "+ segSize);
            LinkedList<String> fileNames = partition.partition(sourceFile, partitionName, segSize, machine);
            fileList.add(new LinkedList<>(fileNames));
            file_number++;
        }


        // record which the result of partition files
        HashMap<String, LinkedList<String>> fileReflection = new HashMap<>();

        //record  which member should give which result file
        HashMap<String,String> memberReflection = new HashMap<>();

        //record the name of map/reduce result
        LinkedList<String> resFileName = new LinkedList<>();

        int count = 0;


        for (Map.Entry<String, HashMap> entry : members.entrySet()) {
            if (count == segSize)
                break;
            if (machine.getId().equals(entry.getKey()))
                continue;

            // this is the name of files after the map task
            // there are mutliple res files
            String command = taskCommand;
            String resName = "intermediateRes" + total_count + ".csv";
            total_count+=1;

            fileReflection.put(resName, new LinkedList<>());
            memberReflection.put(entry.getKey(),resName);
            resFileName.add(resName);

            for (int k = 0; k < fileList.size(); k++) {
                String fileName = fileList.get(k).get(count);
                command = command + " " + fileName;
                fileReflection.get(resName).add(fileName);
            }
            // get one of the member in member list
            Map<String, String> member = (Map<String, String>) entry.getValue();
            String address = member.get("address");
            int port = Integer.valueOf(member.get("port"));
            command = command + " " + resName;
            //send the map command to the worker
            sender.send(command, address, port + 2);
            count++;
        }

        log.info("partition done");

        //use this hashset to record which
        LinkedList<String> fileRecord = new LinkedList(resFileName);

        Thread.sleep(5000);
        // now fetch the result files
        ArrayList<String> memberList = new ArrayList<>();
        for (Map.Entry<String, HashMap> entry : members.entrySet()) {
            if(entry.getKey().equals(machine.getId()))
                continue;
            memberList.add(entry.getKey());
        }
        Random r = new Random();


        //check if all the tasks are finished

        while (fileRecord.size() > 0) {
            String cur = fileRecord.remove(0);
            if (fileClientService.getFile(machine, cur, cur, true) != true) {
                fileRecord.add(cur);
            }

            HashSet<String> removedMembers= machine.getRemovedMembers();
            HashSet<String> visited = new HashSet<>();
            for(String rMember:removedMembers){
                if(visited.contains(rMember))
                    continue;

                if(!memberReflection.containsKey(rMember))
                    continue;

                if(!fileRecord.contains(memberReflection.get(rMember)))
                    continue;

                visited.add(rMember);
                log.info("qweqweqweqweqweqweqwe"+rMember);
                if (memberReflection.containsKey(rMember)){
                    if(memberList.contains(rMember))
                        memberList.remove(rMember);
                    String targetFile=memberReflection.get(rMember);
                    LinkedList<String> curFileNames = fileReflection.get(targetFile);
                    removedMembers.remove(rMember);
                    int n = r.nextInt(memberList.size());
                    String key = memberList.get(n);
                    HashMap<String, String> map = members.get(key);
                    System.out.println("the key is+ "+key);
                    int port = Integer.valueOf(map.get("port"))+2;
                    String address = map.get("address");

                    memberReflection.put(key,targetFile);

                    String command = taskCommand;
                    for (String curFile : curFileNames) {
                        command = command + " " + curFile;
                    }

                    command = command + " " + cur;
                    Thread.sleep(3500);

                    sender.send(command, address, port);

                }
            }


        }


//        //check if all the tasks are finished
//        while (fileRecord.size() > 0) {
//            String cur = fileRecord.remove(0);
//            if (fileClientService.getFile(machine, cur, cur, true) != true) {
//                fileRecord.add(cur);
//                int n = r.nextInt(memberList.size());
//                String key = memberList.get(n);
//                HashMap<String, String> map = members.get(key);
//                int port = Integer.valueOf(map.get("port"));
//                String address = map.get("address");
//                LinkedList<String> curFileNames = fileReflection.get(cur);
//                String command = taskCommand;
//                for (String curFile : curFileNames) {
//                    command = command + " " + curFile;
//                }
//
//                command = command + " " + cur;
//                sender.send(command, address, port);
//            }
//        }

        File file = new File(targetName);

        BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
        LinkedList<String> resultList = new LinkedList<>();
//        HashSet<String> duplicate = new HashSet<>();
        for (String rf : resFileName) {
            BufferedReader reader = new BufferedReader(new FileReader(rf));
            String line = null;
            while ((line = reader.readLine()) != null) {
//                if(!duplicate.contains(line)) {
                    resultList.add(line);
//                    duplicate.add(line);
//                }
            }


            //delete this file
            fileClientService.deleteFile(machine, rf);
            reader.close();
        }
        Collections.sort(resultList);
        for (String l : resultList) {
            bw.write(l);
            bw.newLine();

        }


        log.info("map juice task done");
        log.info("Map Juice Time consuming: " + (System.currentTimeMillis() - start) + " ms");

        Thread.sleep(500);
        for (String rf : resFileName) {
            File file1 = new File(rf);
            file1.delete();
        }


        bw.close();

        fileClientService.putFile(machine, targetName, targetName);


        Thread.sleep(1500);
        for (String rf : resFileName) {
            File file1 = new File(rf);
            file1.delete();
        }
    }
}



//    public void assign(String fileName,String taskCommand,String targetName,int segSize,Machine machine) throws IOException, InterruptedException {
//        FilePartition partition = new FilePartition();
//        FileClientService fileClientService=new FileClientService();
//
//        //get the files
//        LinkedList<String> fileNames=partition.partition(fileName,segSize,machine);
//        ConcurrentHashMap<String, HashMap> members=machine.getMembers();
//        int count=0;
//
//        // record the filename and its result file's name
//        HashMap<String,String> fileReflection = new HashMap<>();
//
//        //record the name of map result
//
//        LinkedList<String> resFileName = new LinkedList<>();
//
//        for (Map.Entry<String, HashMap> entry : members.entrySet()) {
//            if (count==fileNames.size())
//                break;
//            if(machine.getId().equals(entry.getKey()))
//                continue;
//
//            // this is the name of files after the map task
//            // there are mutliple res files
//            String resName = "intermediateRes" + count +".csv";
//            fileReflection.put(resName,fileNames.get(count));
//            resFileName.add(resName);
//
//            // get one of the member in member list
//            Map<String, String> member = (Map<String, String>) entry.getValue();
//            String address=member.get("address");
//            int port=Integer.valueOf(member.get("port"));
//            String key=port+" "+address;
//            //ask this member to fetch the file
////            fileClientService.askToFetch(key,fileNames.get(count));
//            String command=taskCommand+" "+fileNames.get(count)+" "+resName;
//            //send the map command to the worker
//            sender.send(command,address,port+2);
//            count++;
//
//        }
//
//        log.info("partition done");
//
//        //use this hashset to record which
//        LinkedList<String> fileRecord= new LinkedList(resFileName);
//
//        Thread.sleep(10000);
//        // now fetch the result files
//        ArrayList<String> memberList= new ArrayList<>();
//        for(Map.Entry<String, HashMap> entry : members.entrySet())
//            memberList.add(entry.getKey());
//        Random r = new Random();
//        //check if all the tasks are finished
//        while(fileRecord.size()>0){
//            String cur=fileRecord.remove(0);
//            if (fileClientService.getFile(machine,cur,cur,true)!=true) {
//                fileRecord.add(cur);
//                int n = r.nextInt(memberList.size());
//                String key = memberList.get(n);
//                HashMap<String,String> map = members.get(key);
//                int port=Integer.valueOf(map.get("port"));
//                String address=map.get("address");
//                String curFileName=fileReflection.get(cur);
//                String command=taskCommand+" "+curFileName+" "+cur;
//                sender.send(command,address,port);
//            }
//        }
//
//        File file = new File(targetName);
//
//        BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));
//        LinkedList<String> resultList = new LinkedList<>();
//        for (String rf:resFileName){
//            BufferedReader reader = new BufferedReader(new FileReader(rf));
//            String line = null;
//            while((line=reader.readLine())!=null)
//                resultList.add(line);
//
//
//            //delete this file
//            fileClientService.deleteFile(machine,rf);
//
//        }
//
//        for(String l : resultList) {
//            System.out.println(l);
//            bw.write(l);
//            bw.newLine();
//
//        }
//
//        for(String rf: resFileName){
//            File file1=new File(rf);
//            file1.delete();
//        }
//
//
//        bw.close();
//
//        fileClientService.putFile(machine,targetName,targetName);
//        log.info("task done");
//    }



