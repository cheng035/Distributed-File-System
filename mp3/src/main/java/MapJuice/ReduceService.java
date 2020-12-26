package MapJuice;

import Entity.Machine;
import FileService.FileClientService;

import java.io.*;
import java.util.*;

public class ReduceService {
    FileClientService fileClientService = new FileClientService();

    //reduce for tree task
    public void reduce(String fileName, Machine machine, String targetName) throws IOException, InterruptedException {
        HashMap<String,Integer> result=new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            reader.readLine();//first row is the filed information
            String line = null;

            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",");
                String street=item[0];
                int num=result.getOrDefault(street,0)+1;//get current number
                result.put(street,num);

            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        File csv = new File(targetName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        for (Map.Entry<String,Integer> entry : result.entrySet()) {

            String name = entry.getKey();
            if(name=="")
                continue;
            int value = entry.getValue();
            bw.write(name);
            bw.write(",");
            bw.write(String.valueOf(value));
            bw.newLine();
        }

        //delete and upload the file
        bw.close();

        fileClientService.deleteFile(machine,fileName);
        fileClientService.putFile(machine,targetName,targetName);
        csv.delete();
    }

// reduce task for covid, to see which place have infected persons in a time
    //fileName 1 is the original data, fileName 2 is infected list
    public void reduce2(String fileName1, String fileName2, String targetName,Machine machine) throws IOException, InterruptedException {
        BufferedReader reader2 = new BufferedReader(new FileReader(fileName2));
        HashSet<String> infected =new HashSet<>();
        String line = null;

        while ((line = reader2.readLine()) != null) {
            System.out.println(line);
            infected.add(line);
        }

        LinkedList<String> result= new LinkedList<>();

        BufferedReader reader = new BufferedReader(new FileReader(fileName1));
        line = null;
        while ((line = reader.readLine()) != null){
            String item[] = line.split(",");
            String name=item[0];
            if(infected.contains(name)){
                String res=item[1]+","+item[2]+","+item[3];
                System.out.println("res is "+res);
                result.add(res);
            }
        }


        File csv = new File(targetName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));

        for(String res:result){
            bw.write(res);
            bw.newLine();
        }

        bw.close();
        reader.close();
        reader2.close();
        fileClientService.deleteFile(machine,fileName1);
        fileClientService.deleteFile(machine,fileName2);
        fileClientService.putFile(machine,targetName,targetName);
        csv.delete();

    }


//reduce task, to see which people are in the same area with those people.
    //D1 is the new_data after the map,res is the infeced area list
    public void reduce3(String D1, String res,String targetName,Machine machine) throws IOException, InterruptedException {
        BufferedReader reader1 = new BufferedReader(new FileReader(res));
        HashMap<String, List<String[]>> map = new HashMap<>();

        String line1 = null;

        while ((line1 = reader1.readLine()) != null) {
            String item1[] = line1.split(","); // 0: location, 1: start_time, 2: end_time

            String[] interval = new String[2];
            interval[0] = item1[1];
            interval[1] = item1[2];

            if (!map.containsKey(item1[0])) {
                map.put(item1[0], new ArrayList<String[]>());
            }

            map.get(item1[0]).add(interval);
        }

        BufferedReader reader2 = new BufferedReader(new FileReader(D1));
        List<String> names = new LinkedList<>();

        String line2 = null;

        while ((line2 = reader2.readLine()) != null) {
            String item2[] = line2.split(","); // 0: location, 1: name, 2: start_time, 3: end_time
            if (!map.containsKey(item2[0])) continue;

            List<String[]> intervals = map.get(item2[0]);

            for (int i = 0; i < intervals.size(); i++) {
                String[] interval = intervals.get(i);
                if (item2[3].compareTo(interval[0]) < 0 || item2[2].compareTo(interval[1]) > 0) {
                    continue;
                } else {
                    names.add(item2[1]);
                    break;
                }
            }
        }

        File csv = new File(targetName);
        HashSet<String> duplicate = new HashSet<String>();
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        for(String n:names){
            if(duplicate.contains(n))
                continue;
            duplicate.add(n);
            bw.write(n);
            bw.newLine();
        }
        bw.close();
        fileClientService.deleteFile(machine,res);
        fileClientService.deleteFile(machine,D1);
        fileClientService.putFile(machine,targetName,targetName);
        reader1.close();
        reader2.close();
        csv.delete();


    }


}

