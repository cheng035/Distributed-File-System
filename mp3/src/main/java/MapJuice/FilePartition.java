package MapJuice;

import Entity.Machine;
import FileService.FileClientService;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class FilePartition {

    public static LinkedList<String> partition(String fileName,String outputName, int n, Machine machine) throws IOException, InterruptedException {
        FileClientService fileClientService = new FileClientService();
        TreeMap<Integer, LinkedList<String>> groupPartition = new TreeMap<>(); // different
        for (int i = 0; i < n; i++) {
            groupPartition.put(i, new LinkedList<>());
        }
        //read the file and partition
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            reader.readLine();//first row is the filed information
            String line = null;
            //put different line into different group
            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",");//split the information
                String id = item[0];
                int hash_value = Math.abs(id.hashCode()); //calculate the hash_value based on id;
                LinkedList list = groupPartition.get(hash_value%n);
//                System.out.println(hash_value);
                list.add(line);
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        LinkedList<String> fileNames = new LinkedList<>();
        //write to local file

        for (Map.Entry<Integer, LinkedList<String>> entry : groupPartition.entrySet()) {
            int id = entry.getKey();
            LinkedList<String> list = entry.getValue();
//            System.out.println(list.get(0));
            File csv = new File(outputName + id + ".csv");
            System.out.println("the name is");
            System.out.println(outputName + id + ".csv");
            fileNames.add(outputName + id + ".csv");
            BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
            for (String l : list) {
                bw.write(l);
                bw.newLine();
            }
            bw.close();
        }


        //send file to sdfs
        for (String name:fileNames){
            //put the file into sdfs system
            fileClientService.putFile(machine,name,name);

            //delete the file
            File file=new File(name);
            file.delete();
        }

        return fileNames;


    }
}
