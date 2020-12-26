import Controller.Controller;

import java.io.IOException;
import java.util.Scanner;
public class Start {
    public static void main(String[] args) throws IOException, InterruptedException {
        Controller controller = new Controller();
        Scanner in = new Scanner(System.in);
        while (true) {
            // Input argument: Indicates if this node is the introducer.
//                boolean if_introducer = Boolean.parseBoolean(args[0]);
            // Caution: deal with multiple cases
            System.out.println("Input: ");
            String input = in.next();
            // String[] input_list = input.split(" ");

            // Join with the given port number: Node: 1231, Introducer: 4445
            // Usage: join + port_number + error_rate
            if (input.equals("join")) {

                controller = new Controller();
                int b = in.nextInt();
                try{
                double error_rate = in.nextDouble();

                controller.intialize(b, error_rate);
                controller.start();}
                catch(Exception e){
                    System.out.println("run input try again");
                }
            }
            // Usage: all + port_number + error_rate
            if (input.equals("all")) {
                controller = new Controller();
                int b = in.nextInt();
                double error_rate = in.nextDouble();
                controller.intialize(b, error_rate);
                controller.getMachine().setIf_boardcast(true);
                controller.start();
            }
            // Manually leave
            if (input.equals("l")) {
                controller.leave();
            }
            // Display current members
            if (input.equals("d")) {
                controller.displayCurrentMembers();
            }
            // Switch mode
            if (input.equals("s")) {
                controller.switch_mode();
            }
            // Display self ID
            if (input.equals("i")) {
                controller.displaySelfID();
            }

            // --------------------------- File system section
            // "put localFileName sdfsFileName"
            if (input.equals("put")) {
                String localName = in.next();
                String sdfsName = in.next();
                controller.putFile(localName, sdfsName);
            }

            // "get sdfsFileName localFileName"
            if (input.equals("get")) {
                String sdfsName = in.next();
                String localName = in.next();
                controller.getFile(localName, sdfsName);
            }

            if (input.equals("delete")) {
                String sdfsName = in.next();
                controller.deleteFile(sdfsName);
            }

            if (input.equals("ls")) {
                String sdfsName = in.next();
                controller.listFileNodes(sdfsName);
            }

            if (input.equals("store")) {
                controller.listAllLocalFiles();
            }

            // --------------------------- File system section
            if (input.equals("map")) {
                String fileName= in.next();
                String mapName = in.next();
                int size =Integer.valueOf(in.next());
                controller.map(fileName,mapName,size);
            }

            if (input.equals("map2")) {
                String fileName= in.next();
                String mapName = in.next();
                int size =Integer.valueOf(in.next());
                controller.map2(fileName,mapName,size);
            }

            if(input.equals("reduce")){
                String fileName= in.next();
                String mapName = in.next();
                int size =Integer.valueOf(in.next());
                controller.reduce(fileName,mapName,size);
            }

            if(input.equals("reduce2")){
                String fileName1= in.next();
                String fileName2=in.next();
                String mapName = in.next();
                int size =Integer.valueOf(in.next());
                controller.reduce2(fileName1,fileName2,mapName,size);
            }

            if(input.equals("reduce3")){
                String fileName1= in.next();
                String fileName2=in.next();
                String mapName = in.next();
                int size =Integer.valueOf(in.next());
                controller.reduce3(fileName1,fileName2,mapName,size);
            }
            else{
                System.out.println("try again");
            }

        }

    }
}
