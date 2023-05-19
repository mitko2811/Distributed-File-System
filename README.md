# Distributed-File-System Server
This Distributed File Server is using a Controller class to service TCP Client Requests using Java. The DFS server is part of a university coursework.<br>
Tested and working with Java SDK 14.0.2 and up.<br>


## [Introduction]
This Java program is a distributed file server, composed of Controller, Dstore and Client. One can setup many Dstores and Clients to connect to a single Controller. The suplied Client(by SouthamptonUni) can be modified to execute a set of commands at runtime. A Controller is setup on a host machine and it waits for other Dstores to connect and reach a number equal or bigger of the replication factor for files. After the Controller has a valid bank of Dstores, it can then service any requests by Clients who can Store files, Delete files and see the current list of files by sending TCP Requests. If any Dstore fails(crashes) the Controller has a Rebalance function that should sort the internal indexing structure and send requests to Dstores to redistribute files.

## [File Structure]

For running the **Controller** on a machine one must have all files from Controller folder:
  * Controller.java
  * ControllerLogger.java
  * Logger.java
  * Protocol.java

For running a **Dstore** on a machine one must have all files from Dstore folder:
  * Dstore.java
  * DstoreLogger.java
  * Logger.java
  * Protocol.java

For running a **Client** for file requests on a machine one must have all files from Client folder:
  * ClientMain.java 
  * client-1.0.2.jar

## [Compilation]
For Client Compilation:
  * First run: `javac -cp client-1.0.2.jar ClientMain.java`<br>
  * Then run FOR Linux: `java -cp client-1.0.2.jar:. ClientMain <cport> <timeout>`<br>
  * Or run FOR Windows: `java -cp client-1.0.2.jar;. ClientMain <cport> <timeout>`<br>

To setup a Dstore, one must first configure the IP of the Controller host by entering it on line 39, by replacing the `localhost` specification.<br>
For Controller and Dstores run the javac command for the given component(Controller,Dstore or Client).<br>
You can then run each component with the following paramenters:<br>
  * Controller: `java Controller cport R timeout rebalance_period`<br>
  * Dstore:  `java Dstore port cport timeout file_folder`<br>


<b>R</b> is the replication factor - among how many Dstores a file is stored. <br>
<b>cport</b> is the port of the Controller<br>
<b>rebalance_period</b> is the period of the function which will check if there were broken stores with missing files and fix any problems.<br>

An example for Running the Components(on different machines):
  * Controller: `java Controller 12345 3 2000 15000`<br>
  * Dstore:  `java Dstore 10001 12345 2000 newFolder1`<br>
  * Client:  `java -cp client-1.0.2.jar;. ClientMain 12345 2000`<br>
