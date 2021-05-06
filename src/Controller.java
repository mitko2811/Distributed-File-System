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
	// static HashMap<ArrayList<String>, Integer> dstore_files_ports = new
	// HashMap<ArrayList<String>,Integer>();
	static ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	static ConcurrentHashMap<String, Integer> fileToStore_ACK = new ConcurrentHashMap<String, Integer>();

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
					System.out.println("waiting for connection");
					Socket client = ss.accept();

					new Thread(() -> {
						try {
							System.out.println("connected");
							BufferedReader inFromClient = new BufferedReader(
									new InputStreamReader(client.getInputStream()));
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

									System.out.println("command \"" + command + "\" size is " + command.length());

									if (command.equals(Protocol.STORE_TOKEN)) {
										System.out.println("ENTERED STORE");
										if (Dstore_count < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE COMMAND");
										} else {
											String following[] = data.split(" ");
											String filename = following[0];
											int filesize = Integer.parseInt(following[1]);

											if (dstore_file_ports.get(filename) != null) {
												outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
												outClient.flush();
											} else {

												String portsToStore[] = getPortsToStore(dstore_port_numbfiles);
												String portsToStoreString = String.join(" ", portsToStore);

												fileToStore_ACK.put(filename, 0);// initialize store file acks
												outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
												outClient.flush();

												long current_time = System.currentTimeMillis();
												long timeout_time = current_time + timeout;
												boolean success_Store = false;
												while (current_time < timeout_time) {
													if (fileToStore_ACK.get(filename) >= R) { // checks if file to store
																								// has completed
																								// acknowledgements
														outClient.println(Protocol.STORE_COMPLETE_TOKEN);
														outClient.flush();
														System.out
																.println("SEND STORE COMPLETE ACK FOR :" + fileToStore);
														fileToStore_ACK.remove(fileToStore);
														success_Store = true;
													}
												}
												if (!success_Store) {
													fileToStore_ACK.remove(filename);
												}

											}
										}
									} else

									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										System.out.println("ENTERED STORE ACK");

										String filename = data;
										System.out.println("filename is " + filename);
										fileToStore_ACK.put(filename, fileToStore_ACK.get(filename) + 1); // increase
																											// ack
																											// number
																											// inside
																											// chmap
										System.out.println("acknowlefgements for filename are now "
												+ fileToStore_ACK.get(filename));

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

									if (command.equals(Protocol.LIST_TOKEN) && data == null) { // Client LIST -> Client
																								// LIST file_list

										System.out.println("asked list from client");
										outClient.println(Protocol.LIST_TOKEN + " " + "Deez.nuts da.baby");
										outClient.flush();
									} else

									if (command.equals(Protocol.LIST_TOKEN) && data != null) { // DSTORE LIST
										ArrayList<String> filelist = new ArrayList<String>(
												Arrays.asList(data.split(" ")));
										dstore_port_numbfiles.put(dstoreport, filelist.size()); // updates
																								// port/numbfiles
																								// hashmap
										dstore_port_files.put(dstoreport, filelist); // puts list in hashmap

										for (String string : filelist) {
											if (dstore_file_ports.get(string) == null) {
												dstore_file_ports.put(string, new ArrayList<Integer>());
											}
											dstore_file_ports.get(string).add(dstoreport); // puts the given file the
																							// port that its in
											System.out.println("Dstore port: " + dstoreport + " File: " + string);
										}

									} else

									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port -> Dstores LIST |>
																				// Dsotres
																				// LIST file_list -> Dstores REBALANCE
																				// files_to_send files_to_remove
										System.out.println("entered Join in controller");
										System.out.println("port is " + data);
										dstoreport = Integer.parseInt(data);
										dstore_port_files.put(dstoreport, new ArrayList<String>()); // initialize port
																									// number of dstore
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