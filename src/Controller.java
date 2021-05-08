import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
	static Integer cport;
	static Integer R; // Replication factor - number of Dstores
	static Integer timeout;
	static Integer rebalance_period;
	static AtomicInteger Dstore_count = new AtomicInteger(0);

	List<Dstore> Dstore_list; // list of Dstores
	static ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<Integer, ArrayList<String>>();
	static ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	static ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<String, Integer>();
	static ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<Integer, Socket>();
	static ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>());
	static List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>());

	public static void main(String[] args) throws IOException {
		cport = Integer.parseInt(args[0]);
		R = Integer.parseInt(args[1]);
		timeout = Integer.parseInt(args[2]);
		rebalance_period = Integer.parseInt(args[3]);

		String index = "Waiting";

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
							String data = null;
							Integer dstoreport = 0;
							boolean isDstore = false;
							for (;;) {

								data = inClient.readLine();
								if (data != null) {
									int firstSpace = data.indexOf(" ");
									String command;

									if (firstSpace == -1) {
										command = data;
										data = null;
									} else {
										command = data.substring(0, firstSpace);
										data = data.substring(firstSpace + 1);
									}

									System.out.println("COMMAND RECIEVED \"" + command + "\"");
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_TOKEN)) { // CLIENT STORE
										String following[] = data.split(" ");
										String filename = following[0];
										Integer filesize = Integer.parseInt(following[1]);
										System.out.println("ENTERED STORE FROM CLIENT for : " + data);
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE ERROR");
										} else if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
											outClient.println("ERROR ALREADY_EXISTS");
											outClient.flush();
										} else {
											System.out.println("Test2");
											if (file_filesize.get(filename) != null
													|| files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
												outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
												outClient.flush();
												System.out.println("SEND FILE ALREADY EXISTS ERROR");
											} else {
												files_activeStore.add(filename);// ADD FILE STORING INDEX
												System.out.println("Test3");
												for (Integer port : dstore_port_numbfiles.keySet()) {
													System.out.println("port: " + port + " , files: "
															+ dstore_port_numbfiles.get(port));
												}
												String portsToStore[] = getPortsToStore();
												String portsToStoreString = String.join(" ", portsToStore);
												System.out.println("Test4");
												fileToStore_ACKPorts.put(filename, new ArrayList<Integer>());// initialize store file acks
												outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
												outClient.flush();
												System.out.println("TIMES");
												boolean success_Store = false;
												long timeout_time = System.currentTimeMillis() + timeout;
												while (System.currentTimeMillis() < timeout_time) {
													if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
														outClient.println(Protocol.STORE_COMPLETE_TOKEN);
														outClient.flush();
														System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
														dstore_file_ports.put(filename,
																fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
														fileToStore_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
														file_filesize.put(filename, filesize); // add new file's filesize
														for (Integer port : dstore_file_ports.get(filename)) {
															dstore_port_files.get(port).add(filename); //update dstore_port_files
															dstore_port_numbfiles.put(port,
																	dstore_port_numbfiles.get(port) + 1); //increase dstore_port_numbfiles
														}

														success_Store = true;
														System.out.println("BEFORE BREAK");
														break;
													}
												}
												System.out.println("EXITED LOOP TIMES");
												if (!success_Store) {
													System.out.println("FAILED STORE: " + filename);
													fileToStore_ACKPorts.remove(filename);
												}
												files_activeStore.remove(filename);// FILE STORED REMOVE INDEX
											}
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										System.out.println("ENTERED STORE ACK for: " + data);

										String filename = data;
										System.out.println("RECIEVED ACK FOR: " + filename);
										fileToStore_ACKPorts.get(filename).add(dstoreport); // add ack port inside chmap
										System.out.println("acknowlefgements for filename are now "
												+ fileToStore_ACKPorts.get(filename));

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN)) { // Client LOAD
										System.out.println("ENTERED LOAD FROM CLIENT for file: " + data);
										String filename = data;
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else {
											if (!dstore_file_ports.containsKey(filename)
													|| files_activeStore.contains(filename)) { // CHECKS FILE CONTAINS AND INDEX
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												outClient.flush();
											} else {
												dstore_file_portsLeftReload.put(filename,
														new ArrayList<>(dstore_file_ports.get(filename)));
												outClient.println(Protocol.LOAD_FROM_TOKEN + " "
														+ dstore_file_portsLeftReload.get(filename).get(0) + " "
														+ file_filesize.get(filename));
												outClient.flush();
												dstore_file_portsLeftReload.get(filename).remove(0);
											}
										}
									} else
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_TOKEN)) { // CLIENT REMOVE
										System.out.println("ENTERED REMOVE FOR FILE: " + data);
										String filename = data;
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else {
											if (files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
												outClient.println("ERROR ALREADY_EXISTS");
												outClient.flush();
											} else if (dstore_file_ports.get(filename) == null
													|| dstore_file_ports.get(filename).isEmpty()
													|| files_activeStore.contains(filename)) {
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												outClient.flush();
												System.out.println("SEND FILE DOES NOT EXIST FOR: " + filename);
											} else {
												files_activeRemove.add(filename);
												fileToRemove_ACKPorts.put(filename,
														new ArrayList<>(dstore_file_ports.get(filename))); // initializes the ports that wait for remove
												ArrayList<Integer> tempACKPORTS = new ArrayList<Integer>(
														fileToRemove_ACKPorts.get(filename));

												for (Integer port : tempACKPORTS) { // send ports file to delete
													Socket dstoreSocket = dstore_port_Socket.get(port);//MUST SYNC THIS LOOP
													PrintWriter outDstore = new PrintWriter(
															dstoreSocket.getOutputStream());
													outDstore.println(Protocol.REMOVE_TOKEN + " " + filename);
													outDstore.flush();
													System.out
															.println("********************* ASKED FOR REMOVE OF FILE: "
																	+ filename + " on port " + port);
												}

												boolean success_Remove = false;
												long timeout_time = System.currentTimeMillis() + timeout;
												while (System.currentTimeMillis() < timeout_time) {
													if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
														System.out.println("ENTERED LOOP TIMES");
														outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
														outClient.flush();
														System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
														fileToRemove_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
														//file_filesize.remove(filename); // add new file's filesize
														dstore_file_ports.remove(filename);
														success_Remove = true;
														break;
													}
												}
												System.out.println("EXITED LOOP TIMES");
												if (!success_Remove) {
													System.out.println("FAILED STORE: " + filename);
													//fileToRemove_ACKPorts.remove(filename); rebalance will fix this
												}
												file_filesize.remove(filename); // remove file_filesize so if broken rebalance should fix
												files_activeRemove.add(filename); // remove file ActiveRemove from INDEX
											}
										}

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_ACK_TOKEN)
											|| command.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) { // Dstore REMOVE_ACK filename
										System.out.println("ENTERED REMOVE ACK for: " + data);
										String filename = data;
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
									if (command.equals(Protocol.RELOAD_TOKEN)) { // Client RELOAD
										System.out.println("ENTERED RELOAD FROM CLIENT for: " + data);
										String filename = data;

										if (Dstore_count.get() < R || (files_activeStore.contains(filename)
												|| files_activeRemove.contains(filename))) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else {
											if (dstore_file_portsLeftReload.get(filename) != null
													&& !dstore_file_portsLeftReload.get(filename).isEmpty()) {
												outClient.println(Protocol.LOAD_FROM_TOKEN + " "
														+ dstore_file_portsLeftReload.get(filename).get(0) + " "
														+ file_filesize.get(filename));
												outClient.flush();
												dstore_file_portsLeftReload.get(filename).remove(0);
												System.out.println(
														"********AFTERRRportsLeftReload******** is " + filename);
											} else {
												outClient.println(Protocol.ERROR_LOAD_TOKEN);
												outClient.flush();
												System.out.println("SEND ERROR LOAD TO CLIENT FOR FILE: " + filename);
											}
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && data == null) { // Client LIST -> Client LIST file_list
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else {
											System.out.println("asked list from client");
											String filesList = String.join(" ", file_filesize.keySet());
											outClient.println(Protocol.LIST_TOKEN + " " + filesList);
											outClient.flush();
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && data != null) { // DSTORE LIST
										ArrayList<String> filelist = new ArrayList<String>(
												Arrays.asList(data.split(" ")));
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
										System.out.println("Entered Join from Dstore port is " + data);
										dstoreport = Integer.parseInt(data);
										if (dstore_port_numbfiles.containsKey(dstoreport)) {
											System.out.println("DStore port already used: " + data);
											System.out.println("Connection refused for port: " + data);
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

									} else
										System.out.println("Unrecognised command!");
								} else {
									if (isDstore)
										Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
									client.close();
									break;
								}
							}
						} catch (Exception e) {
							System.out.println("error " + e);
						}
					}).start();
				} catch (Exception e) {
					System.out.println("error " + e);
				}
			}
		} catch (Exception e) {
			System.out.println("error " + e);
		}
		System.out.println();
	}

	private static String[] getPortsToStore() { // finds R ports with least files
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