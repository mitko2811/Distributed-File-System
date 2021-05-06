import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {
	static int cport;
	static int R; // Replication factor - number of Dstores
	static int timeout;
	static int rebalance_period;
	static int Dstore_count = 0;

	List<Dstore> Dstore_list; // list of Dstores
	static ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<Integer, ArrayList<String>>();
	static ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	static ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<String, Integer>();

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
					Socket client = ss.accept();

					new Thread(() -> {
						try {
							System.out.println("Connected");
							BufferedReader inFromClient = new BufferedReader(
									new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream());

							ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
							String data = null;
							int dstoreport = 0;
							boolean isDstore = false;
							String fileToStore = "";
							for (;;) {

								data = inFromClient.readLine();
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
									if (command.equals(Protocol.STORE_TOKEN)) { // STORE FROM CLIENT
										System.out.println("ENTERED STORE FROM CLIENT");
										if (Dstore_count < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE ERROR");
										} else {
											System.out.println("Test1");
											String following[] = data.split(" ");
											String filename = following[0];
											int filesize = Integer.parseInt(following[1]);
											System.out.println("Test2");
											if (dstore_file_ports.get(filename) != null) {
												outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
												outClient.flush();
												System.out.println("SEND FILE ALREADY EXISTS ERROR");
											} else {
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
														System.out
																.println("SEND STORE COMPLETE ACK FOR: " + fileToStore);
														dstore_file_ports.put(filename,
																fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
														fileToStore_ACKPorts.remove(fileToStore); // remove stored file from fileToStore_ACKPorts queue
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

											}
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										System.out.println("ENTERED STORE ACK");

										String filename = data;
										System.out.println("RECIEVED ACK FOR: " + filename);
										fileToStore_ACKPorts.get(filename).add(dstoreport); // add ack port inside chmap
										System.out.println("acknowlefgements for filename are now "
												+ fileToStore_ACKPorts.get(filename));

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN)) { // Client LOAD filename -> Client LOAD_FROM port filesize
										System.out.println("ENTERED LOAD FROM CLIENT for file: " + data);
										String filename = data;
										if (!dstore_file_ports.containsKey(filename)) {
											outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											outClient.flush();
										} else {

											System.out.println("LOAD 1");
											dstore_file_portsLeftReload.put(filename, new ArrayList<>(dstore_file_ports.get(filename)));
											System.out.println("LOAD 2 : " + dstore_file_ports.get(filename).size() + " ports left");
											for (Integer port : dstore_file_portsLeftReload.get(filename)) {
												outClient.println(Protocol.LOAD_FROM_TOKEN + " " + port + " "
														+ file_filesize.get(filename));
												outClient.flush();
												dstore_file_portsLeftReload.get(filename).remove(port);
												System.out.println("Testing loop");
												System.out.println("LOAD 2 : " + dstore_file_ports.get(filename).size() + " ports left after loop");
												break;
											}
										}
										System.out.println("OUT OF LOAD?");

									} else

									if (command.equals(Protocol.RELOAD_TOKEN)) { // Client LOAD filename -> Client LOAD_FROM port filesize
										System.out.println("ENTERED RELOAD FROM CLIENT");
										String filename = data;

										if (dstore_file_portsLeftReload.get(filename) != null) {
											for (Integer port : dstore_file_ports.get(filename)) {
												outClient.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + file_filesize.get(filename));
												outClient.flush();
												dstore_file_portsLeftReload.get(filename).remove(port);
												break;
											}
										} else {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											outClient.flush();
										}

									} else
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && data == null) { // Client LIST -> Client LIST file_list

										System.out.println("asked list from client");
										String filesList = String.join(" ", file_filesize.keySet());
										outClient.println(Protocol.LIST_TOKEN + " " + filesList);
										outClient.flush();
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
										System.out.println("entered Join in controller");
										System.out.println("port is " + data);
										dstoreport = Integer.parseInt(data);
										if (dstore_port_numbfiles.containsKey(dstoreport)) {
											System.out.println("DStore port already used: " + data);
											System.out.println("Connection refused for port: " + data);
											client.close();
											break;
										}
										dstore_port_files.put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
										dstore_port_numbfiles.put(dstoreport, 0); // initialize port/numbfiles hashmap
										isDstore = true;
										Dstore_count++;

										outClient.println(Protocol.LIST_TOKEN); // delete later call rebalance
										outClient.flush();// delete later
										System.out.println("Send list to dstore");

									} else
										System.out.println("unrecognised command");
								}
							}
							// outToClient.close();
						} catch (Exception e) {
							System.out.println("error " + e);
						}
					}).start();
					// client.close();
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