# MP3_Maple_jUICE_System
This project aims at building a simple distributed file system including the Gossip / broadcast style error detection.

## Classes
1. udpService: contains methods for UDP connections. 
2. localService: update and modify the local database.
3. Machine: Local database, store the information like port, member list
4. Controller: add the services in thread, controller for threads, performing thread operations.
5. FileClientService: client-side methods to deal with user operations.
6. FileServerService: server-side methods to deal with client requests through TCP.
7. LocalFileService: local file system methods
8. TcpService: methods handling the TCP connections among nodes.
9. MapleMaster: the master which assign the task
10. MapleMaster: the master which assign the task
11. Map and Reduce: the map and reduce functions which implemnt tasks
12. MapListener: listen to the command from maple_juice master
13. Command Sender: utill class to send the command
14. File partition: method to split a file into  different files
15. Start: main entrance of the program.
 
## Usage
Please compile the project as well as the dependencies into a jar file in order to run the program on Linux platforms.

=======
java -jar mp3.jar

### Run the program
Use the command 'java -cp <jar_name> Start' in the command line to run the program jar.

(5) To kill / fail a node manually, please insert ctrl-C.

 -- File system\

(1) **put local_file_name sdfs_file_name **       Upload file to the system\
(2) **get sdfs_file_name local_file_name **       Save file to the local system\
(3) **delete sdfs_file_name **                    delete sdfs file\
(4) **ls sdfs_file_name        **                 lookup machines storing the given sdfs_file_name\
(5) **store  **                                   list all the sdfs files that storing at this node

### for maple juice functions
to find the infected persons

map2 org.csv new.csv 4
reduce2 org.csv inf.csv loc.csv 4
reduce3 new.csv loc.csv res.csv 4

to count the good trees
map trees.csv asd.csv 4
reduce asd.csv res2.csv 4

you can change the last number to set the number of nodes to take the task
