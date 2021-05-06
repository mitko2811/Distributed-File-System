import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Controller {
	static int cport;
	static int R; // Replication factor - number of Dstores
	static int timeout;
	static int rebalance_period;
	static int Dstore_count = 0;

	List<Dstore> Dstore_list; // list of Dstores
	static ConcurrentHashMap<Integer, List<String>> dstore_port_files = new ConcurrentHashMap<Integer, List<String>>();
	static ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	static ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>();
	static ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>();
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
							BufferedReader inFromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream());
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
											String following[] = data.split(" ");
											String filename = following[0];
											int filesize = Integer.parseInt(following[1]);

											if (dstore_file_ports.get(filename) != null) {
												outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
												outClient.flush();
												System.out.println("SEND FILE ALREADY EXISTS ERROR");
											} else {

												String portsToStore[] = getPortsToStore(dstore_port_numbfiles);
												String portsToStoreString = String.join(" ", portsToStore);

												fileToStore_ACKPorts.put(filename, new CopyOnWriteArrayList<Integer>());// initialize store file acks
												outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
												outClient.flush();

												System.out.println("TIMES");
												boolean success_Store = false;
												long timeout_time = System.currentTimeMillis() + timeout;
												while (System.currentTimeMillis() < timeout_time) {
													if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
														outClient.println(Protocol.STORE_COMPLETE_TOKEN);
														outClient.flush();
														System.out.println("SEND STORE COMPLETE ACK FOR: " + fileToStore);
														dstore_file_ports.put(filename,fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
														fileToStore_ACKPorts.remove(fileToStore); // remove stored file from fileToStore_ACKPorts queue
														file_filesize.put(filename,filesize); // add new file's filesize
														for (Integer port : dstore_file_ports.get(filename)) {
															dstore_port_files.get(port).add(filename); //update dstore_port_files
															dstore_port_numbfiles.put(port,dstore_port_numbfiles.get(port)+1); //increase dstore_port_numbfiles
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
										System.out.println("acknowlefgements for filename are now " + fileToStore_ACKPorts.get(filename));

									} else


									// if (command.equals(Protocol.LOAD_TOKEN)) { // Client LOAD filename -> Client
									// LOAD_FROM port
									// // filesize
									// System.out.println("ENTERED LOAD");
									// int secondSpace = data.indexOf(" ", firstSpace + 1);
									// String fileName = data.substring(firstSpace + 1, secondSpace);
									// System.out.println("fileName " + fileName);
									// File inputFile = new File(fileName);
									// FileInputStream inf = new FileInputStream(inputFile);
									// OutputStream out = client.getOutputStream();
									// while ((buflen = inf.read(buf)) != -1) {
									// System.out.print("*");
									// out.write(buf, 0, buflen);
									// }
									// in.close();
									// inf.close();
									// client.close();
									// out.close();
									// } else

									// if (command.equals(Protocol.REMOVE_TOKEN)) { // Client REMOVE filename ->
									// Dstores REMOVE
									// // filename |> Dstores REMOVE_ACK filename
									// // -> Client REMOVE_COMPLETE
									// System.out.println("ENTERED REMOVE");
									// int secondSpace = data.indexOf(" ", firstSpace + 1);
									// String fileName = data.substring(firstSpace + 1, secondSpace);
									// System.out.println("fileName " + fileName);
									// File inputFile = new File(fileName);
									// FileInputStream inf = new FileInputStream(inputFile);
									// OutputStream out = client.getOutputStream();
									// while ((buflen = inf.read(buf)) != -1) {
									// System.out.print("*");
									// out.write(buf, 0, buflen);
									// }
									// in.close();
									// inf.close();
									// client.close();
									// out.close();
									// } else
//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && data == null) { // Client LIST -> Client LIST file_list

										System.out.println("asked list from client");
										String filesList = String.join(" ", file_filesize.keySet()); 
										outClient.println(Protocol.LIST_TOKEN + " " + filesList);
										outClient.flush();
									} else

//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && data != null) { // DSTORE LIST
										ArrayList<String> filelist = new ArrayList<String> (Arrays.asList(data.split(" "))); // not thread safe array
										CopyOnWriteArrayList<String> filelist_safe = new CopyOnWriteArrayList<String> (filelist); // thread safe array


										dstore_port_numbfiles.put(dstoreport, filelist_safe.size()); // updates port/numbfiles hashmap
										dstore_port_files.put(dstoreport, filelist_safe); // puts list in hashmap

										for (String string : filelist_safe) {
											if (dstore_file_ports.get(string) == null) {
												dstore_file_ports.put(string, new CopyOnWriteArrayList<Integer>());
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
										dstore_port_files.put(dstoreport, new CopyOnWriteArrayList<String>()); // initialize port number of dstore
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

	private static String[] getPortsToStore(ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles) { // finds R
																											// ports
																											// with
																											// least
																											// number of
																											// files for
																											// store
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