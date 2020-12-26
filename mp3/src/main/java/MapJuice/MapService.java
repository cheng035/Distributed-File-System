package MapJuice;

import Entity.Machine;
import FileService.FileClientService;


import java.io.*;
import java.util.LinkedList;

public class MapService {

    public static void map(String fileName, Machine machine, String targetName) throws IOException, InterruptedException {
        // use hashmap to record the map results, and write the result to a new file
        LinkedList<String> pairList = new LinkedList<>();
        FileClientService fileClientService = new FileClientService();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            reader.readLine();//first row is the filed information
            String line = null;

            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",",-1);//split the information


                if (item.length<16)
                    continue;
                String condition = item[16];
                String street=item[6].toUpperCase();
                System.out.println("street");

                if(street.equals(""))
                    continue;
                if(condition.equals("Good")) {
                    String cur=street+","+1;
                    System.out.println(cur);
                    pairList.add(cur);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        File csv = new File(targetName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv,true));

        for(String pair:pairList){
            bw.write(pair);
            bw.newLine();
        }

        bw.close();


        fileClientService.deleteFile(machine,fileName);
        fileClientService.putFile(machine,targetName,targetName);
        csv.delete();
    }
//@Test
//    public void test() throws IOException {
//        map("trees.csv");
//    }

    // to put the location in the front.

//    public static void map1(String fileName, Machine machine, String targetName) throws IOException, InterruptedException {
//        // use hashmap to record the map results, and write the result to a new file
//        LinkedList<String> result = new LinkedList<String>();
//        FileClientService fileClientService = new FileClientService();
//
//
//        BufferedReader reader = new BufferedReader(new FileReader(fileName));
//        reader.readLine();//first row is the filed information
//        String line = null;
//
//        while ((line = reader.readLine()) != null) {
//            String item[] = line.split(",");//split the information
//            String tmp=item[0];
//            item[0]=item[1];
//            item[1]=tmp;
//            String curLine=item[0];
//            for (int i=1;i<item.length;i++){
//                curLine+=",";
//                curLine+=item[1];
//            }
//            result.add(curLine);
//        }
//
//
//        File csv = new File(targetName);
//        BufferedWriter bw = new BufferedWriter(new FileWriter(csv,true));
//
//        for(String res: result){
//            bw.write(res);
//            bw.newLine();
//        }
//
//        bw.close();
//        reader.close();
//
//        fileClientService.deleteFile(machine,fileName);
//        fileClientService.putFile(machine,targetName,targetName);
//        csv.delete();
//    }

    // change the original data's key to the place
    public static void map2(String fileName1, String targetName, Machine machine) throws IOException, InterruptedException {

        FileClientService fileClientService = new FileClientService();
        BufferedReader reader = new BufferedReader(new FileReader(fileName1));
        LinkedList<String> result= new LinkedList<>();
        String line = null;

        while ((line = reader.readLine()) != null) {
            String item[] = line.split(",");
            String cur=item[1]+","+item[0]+","+item[2]+","+item[3];
            result.add(cur);
        }


        File csv = new File(targetName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
        for(String res:result){
            bw.write(res);
            bw.newLine();
        }
        bw.close();
        reader.close();
        fileClientService.deleteFile(machine,fileName1);

        fileClientService.putFile(machine,targetName,targetName);
        csv.delete();
    }


}
