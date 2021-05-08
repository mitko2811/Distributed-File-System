import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
	Integer cport;
	Integer R; // Replication factor - number of Dstores
	Integer timeout;
	Integer rebalance_period;
	AtomicInteger Dstore_count = new AtomicInteger(0);

	List<Dstore> Dstore_list; // list of Dstores
	ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<Integer, ArrayList<String>>();
	ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<String, Integer>();
	ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<Integer, Socket>();
	ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores
	List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes

	public Controller(int cport, int R, int timeout, int rebalance_period) {
		this.cport = cport;
		this.R = R;
		this.timeout = timeout;
		this.rebalance_period = rebalance_period;
		this.startController();

	}

	public void startController() {
		try {
			ServerSocket ss = new ServerSocket(cport);
			for (;;) {
				try {
					System.out.println("Waiting for connection");
					final Socket client = ss.accept();

					new Thread(() -> {
						try {
							System.out.println("Connected");
							BufferedReader inClient = new BufferedReader(
									new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream());

							ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
							String dataline = null;
							Integer dstoreport = 0;
							boolean isDstore = false;
							for (;;) {

								dataline = inClient.readLine();
								if (dataline != null) {
									String[] data = dataline.split(" ");
									System.out.println(data);
									String command;
									if (data.length==1) {
										command = dataline.trim();
										data[0] = command;
										dataline = null;
									} else {
										command = data[0];
										data[data.length-1] = data[data.length-1].trim();
										dataline = null;
									}

									System.out.println("COMMAND RECIEVED \"" + command + "\" and data size is " + data.length);
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_TOKEN)) { // CLIENT STORE
										if(data.length!=3){continue;} // log error and continue
										System.out.println("ENTERED STORE FROM CLIENT for : " + data[1]);
										String filename = data[1];
										Integer filesize = Integer.parseInt(data[2]);
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE ERROR");
										} else if (file_filesize.get(filename) != null
												|| files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
											outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
											outClient.flush();
											System.out.println("SEND FILE ALREADY EXISTS ERROR");
										} else {
											synchronized (this) {
												if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
													outClient.flush();
													continue;
												} else {
													files_activeStore.add(filename);// ADD FILE STORING INDEX
												}
											}
											System.out.println("STORING INITIATED : " + filename);
											for (Integer port : dstore_port_numbfiles.keySet()) {
												System.out.println("port: " + port + " , files: "
														+ dstore_port_numbfiles.get(port));
											}
											String portsToStore[] = getPortsToStore();
											String portsToStoreString = String.join(" ", portsToStore);
											fileToStore_ACKPorts.put(filename, new ArrayList<Integer>());// initialize store file acks
											outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
											outClient.flush();

											boolean success_Store = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() < timeout_time) {
												if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
													outClient.println(Protocol.STORE_COMPLETE_TOKEN);
													outClient.flush();
													System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
													System.out.println("ACK PORTS NOW FOR FILE: (" + filename + ") ("
															+ fileToStore_ACKPorts.get(filename) + ")");
													dstore_file_ports.put(filename, fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
													file_filesize.put(filename, filesize); // add new file's filesize
													for (Integer port : dstore_file_ports.get(filename)) {
														dstore_port_files.get(port).add(filename); //update dstore_port_files
														dstore_port_numbfiles.put(port,
																dstore_port_numbfiles.get(port) + 1); //increase dstore_port_numbfiles
													}
													success_Store = true;
													break;
												}
											}

											if (!success_Store) {
												System.out.println("FAILED STORE: " + filename);
											}

											fileToStore_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
											files_activeStore.remove(filename);// FILE STORED REMOVE INDEX
											System.out.println("EXITED STORE FOR: " + filename);
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										if(data.length!=2){continue;} // log error and continue
										System.out.println("RECIEVED ACK FOR: " + data[1] + " FROM PORT: " + dstoreport);
										String filename = data[1];
										fileToStore_ACKPorts.get(filename).add(dstoreport); // add ack port inside chmap
										dstore_port_numbfiles.put(dstoreport,
												dstore_port_numbfiles.get(dstoreport) + 1);
										System.out.println("acknowlefgements for: " + filename + " are now "
												+ fileToStore_ACKPorts.get(filename));

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN) || command.equals(Protocol.RELOAD_TOKEN)) { // Client LOAD
										if(data.length!=2){continue;} // log error and continue
										System.out.println("ENTERED LOAD/RELOAD FROM CLIENT for file: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR for: " + filename);
										} else {
											if (!file_filesize.containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												outClient.flush();
												System.out.println("SEND ERROR_FILE_DOES_NOT_EXIST_TOKEN for: " + filename);
											} else {
												synchronized (this) {
													if (files_activeStore.contains(filename) || files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
														outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														outClient.flush();
														continue;
													} 
												}
												if(command.equals(Protocol.LOAD_TOKEN)){
													System.out.println("IT WAS LOAD FROM CLIENT for file: " + data[1]);
													dstore_file_portsLeftReload.put(filename,new ArrayList<>(dstore_file_ports.get(filename)));
													outClient.println(Protocol.LOAD_FROM_TOKEN + " " + dstore_file_portsLeftReload.get(filename).get(0) + " " + file_filesize.get(filename));
													outClient.flush();
													dstore_file_portsLeftReload.get(filename).remove(0);
												} else{
													System.out.println("IT WAS RELOAD FROM CLIENT for file: " + data[1]);
													if (dstore_file_portsLeftReload.get(filename) != null && !dstore_file_portsLeftReload.get(filename).isEmpty()) {
														outClient.println(Protocol.LOAD_FROM_TOKEN + " " + dstore_file_portsLeftReload.get(filename).get(0) + " " + file_filesize.get(filename));
														outClient.flush();
														dstore_file_portsLeftReload.get(filename).remove(0);
														System.out.println("********AFTERRRportsLeftReload******** is " + filename);
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														outClient.flush();
														System.out.println("SEND ERROR LOAD TO CLIENT FOR FILE: " + filename);
													}
												}

											}
											System.out.println("EXITED LOAD/reload FROM CLIENT for file: " + filename);
										}
									} else
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_TOKEN)) { // CLIENT REMOVE
										if(data.length!=2){continue;} // log error and continue
										System.out.println("ENTERED REMOVE FOR FILE: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println(
													"SEND NOT ENOUGH DSTORES FOR REMOVE ERROR for: " + filename);
										} else if (dstore_file_ports.get(filename) == null
												|| dstore_file_ports.get(filename).isEmpty()
												|| files_activeStore.contains(filename)) {
											outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											outClient.flush();
											System.out.println("SEND FILE DOES NOT EXIST FOR: " + filename);
										} else {
											synchronized (this) {
												if (files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
													outClient.flush();
													continue;
												} else {
													files_activeRemove.add(filename);
												}
											}
											file_filesize.remove(filename);// remove file_filesize so if broken rebalance should fix
											fileToRemove_ACKPorts.put(filename,
													new ArrayList<>(dstore_file_ports.get(filename))); // initializes the ports that wait for remove
											ArrayList<Integer> tempACKPORTS = new ArrayList<Integer>(
													fileToRemove_ACKPorts.get(filename));

											for (Integer port : tempACKPORTS) { // send ports file to delete
												Socket dstoreSocket = dstore_port_Socket.get(port);//MUST SYNC THIS LOOP
												PrintWriter outDstore = new PrintWriter(dstoreSocket.getOutputStream());
												outDstore.println(Protocol.REMOVE_TOKEN + " " + filename);
												outDstore.flush();
												System.out.println("************ASKED FOR REMOVE OF FILE: " + filename
														+ " on port " + port);
											}

											boolean success_Remove = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() < timeout_time) {
												if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
													System.out.println("ENTERED LOOP TIMES");
													outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
													outClient.flush();
													System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
													dstore_file_ports.remove(filename);
													success_Remove = true;
													break;
												}
											}

											if (!success_Remove) {
												System.out.println("FAILED STORE: " + filename);
											}

											fileToRemove_ACKPorts.remove(filename);
											files_activeRemove.remove(filename); // remove file ActiveRemove from INDEX
											System.out.println("EXITED REMOVE for: " + filename);
										}

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_ACK_TOKEN)
											|| command.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) { // Dstore REMOVE_ACK filename
										if(data.length!=2){continue;} // log error and continue
										System.out.println("ENTERED REMOVE ACK for: " + data[1]);
										String filename = data[1];
										if (fileToRemove_ACKPorts.get(filename) != null
												&& !fileToRemove_ACKPorts.get(filename).isEmpty()) {
											fileToRemove_ACKPorts.get(filename).remove(dstoreport); // removing dstore with ack from list
										}
										dstore_port_files.get(dstoreport).remove(filename); //removes file from map of port
										dstore_port_numbfiles.put(dstoreport,
												dstore_port_numbfiles.get(dstoreport) - 1); // suspend 1 from file count
										dstore_file_ports.get(filename).remove(dstoreport); // remove port from file - ports map
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && !isDstore) { // Client LIST -> Client LIST file_list
										if(data.length!=1){continue;} // log error and continue
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else {
											String filesList = String.join(" ", file_filesize.keySet());
											outClient.println(Protocol.LIST_TOKEN + " " + filesList);
											outClient.flush();
											System.out.println("asked list from client");
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && isDstore) { // DSTORE LIST
										ArrayList<String> filelist = new ArrayList<String>(Arrays.asList(data));
										filelist.remove(Protocol.LIST_TOKEN); // remove command entry
										dstore_port_numbfiles.put(dstoreport, filelist.size()); // updates port/numbfiles hashmap
										dstore_port_files.put(dstoreport, filelist); // puts list in hashmap

										for (String string : filelist) {
											if (dstore_file_ports.get(string) == null) {
												dstore_file_ports.put(string, new ArrayList<Integer>());
											}
											dstore_file_ports.get(string).add(dstoreport); // puts the given file the port that its in
											System.out.println("Dstore port: " + dstoreport + " File: " + string);
										}

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port
										if(data.length!=2){continue;} // log error and continue
										System.out.println("Entered Join from Dstore port is " + data[1]);
										dstoreport = Integer.parseInt(data[1]);
										if (dstore_port_numbfiles.containsKey(dstoreport)) {
											System.out.println("DStore port already used: " + dstoreport);
											System.out.println("Connection refused for port: " + dstoreport);
											client.close();
											break;
										}
										dstore_port_files.put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
										dstore_port_numbfiles.put(dstoreport, 0); // initialize port/numbfiles hashmap
										dstore_port_Socket.put(dstoreport, client);
										isDstore = true;
										Dstore_count.incrementAndGet();

										outClient.println(Protocol.LIST_TOKEN); // delete later call rebalance
										outClient.flush();// delete later
										System.out.println("Send list to dstore");

									} else{
										System.out.println("Unrecognised Command!");
										continue; // log error
									}
								} else {
									if (isDstore)
										Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
									client.close();
									break;
								}
							}
						} catch (Exception e) {
							System.out.println("error1 " + e);
							e.printStackTrace();
						}
					}).start();
				} catch (Exception e) {
					System.out.println("error2 " + e);
				}
			}
		} catch (Exception e) {
			System.out.println("error3 " + e);
		}
	}

	public static void main(String[] args) throws IOException {
		int cport = Integer.parseInt(args[0]);
		int R = Integer.parseInt(args[1]);
		int timeout = Integer.parseInt(args[2]);
		int rebalance_period = Integer.parseInt(args[3]);
		Controller controller = new Controller(cport, R, timeout, rebalance_period);
	}

	private String[] getPortsToStore() { // finds R ports with least files
		Integer ports[] = new Integer[R];

		for (Integer port : dstore_port_numbfiles.keySet()) {
			int max = 0;

			for (int i = 0; i < R; i++) {
				if (ports[i] == null) {
					max = i;
					ports[i] = port;
					break;
				}
				if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max])) {
					max = i;
				}
			}
			if (dstore_port_numbfiles.get(port) < dstore_port_numbfiles.get(ports[max])) {
				ports[max] = port;
			}
		}

		String returnPorts[] = new String[R];
		for (int i = 0; i < R; i++) {
			returnPorts[i] = ports[i].toString();
		}
		return returnPorts;
	}

}