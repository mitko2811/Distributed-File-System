import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
	//private Logger;
	private Integer cport;
	private Integer R; // Replication factor - number of Dstores
	private Integer timeout;
	private Integer rebalance_period;
	private AtomicInteger Dstore_count = new AtomicInteger(0);
	private AtomicInteger rebalanceCompleteACK = new AtomicInteger(0);
	private volatile Boolean activeRebalance = false;
	private volatile Boolean activeList = false;
	private volatile Long rebalance_time;
	private Object lock = new Object();
	private Object removeLock = new Object();
	private Object storeLock = new Object();
	private Object DstoreJoinLock = new Object();
	private List<String> timedoutStore = Collections.synchronizedList(new ArrayList<String>());
	private ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<Integer, ArrayList<String>>();
	private ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	private ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	private ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	private ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<Integer, Socket>();
	private ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	private List<Integer> listACKPorts = Collections.synchronizedList(new ArrayList<Integer>()); // index of active file stores
	private List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores
	private List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes
	private ConcurrentHashMap<String, Integer> files_addCount = new ConcurrentHashMap<String, Integer>();
	private ConcurrentHashMap<String, Integer> files_RemoveCount = new ConcurrentHashMap<String, Integer>();

	public Controller(int cport, int R, int timeout, int rebalance_period) {
		this.cport = cport;
		this.R = R;
		this.timeout = timeout;
		this.rebalance_period = rebalance_period;
		try {
			ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.startController();
	}

	public void startController() {
		try {
			ServerSocket ss = new ServerSocket(cport);

			new Thread(() -> { // REBALANCE OPERATION THREAD
				while (true) {
					this.rebalance_time = System.currentTimeMillis() + rebalance_period;
					while (System.currentTimeMillis() <= this.rebalance_time) {
						continue;
					}
					rebalanceOperation();
					activeRebalance = false;
				}
			}).start();

			for (;;) {
				try {
					System.out.println("Waiting for connection");
					Socket client = ss.accept();

					new Thread(() -> {
						boolean isDstore = false;
						Integer dstoreport = 0;
						try {
							System.out.println("Connected");
							BufferedReader inClient = new BufferedReader(
									new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream(), true);
							ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
							String dataline = null;
							for (;;) {

								dataline = inClient.readLine();
								ControllerLogger.getInstance().messageReceived(client, dataline); // log recieved messages
								if (dataline != null) {
									String[] data = dataline.split(" ");
									String command;
									if (data.length == 1) {
										command = dataline.trim();
										data[0] = command;
									} else {
										command = data[0];
									}

									System.out.println("COMMAND RECIEVED \"" + command + "\"");
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_TOKEN)) { // CLIENT STORE
										if (data.length != 3) {
											System.err.println("Malformed message received for STORE");
											continue;
										} // log error and continue
										String filename = data[1];
										Integer filesize = Integer.parseInt(data[2]);

										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
										} else if (file_filesize.get(filename) != null
												|| files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
											outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
										} else {
											synchronized (lock) {
												if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
													ControllerLogger.getInstance().messageSent(client,
															Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
													continue;
												} else {
													while (activeRebalance) { // waiting for rebalance to end
														continue;
													}
													files_activeStore.add(filename);// ADD FILE STORING INDEX
												}
											}

											String portsToStore[] = getPortsToStore(R);
											String portsToStoreString = String.join(" ", portsToStore);
											fileToStore_ACKPorts.put(filename, new ArrayList<Integer>());// initialize store file acks
											outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
											timedoutStore.add(filename);

											boolean success_Store = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
													outClient.println(Protocol.STORE_COMPLETE_TOKEN);
													ControllerLogger.getInstance().messageSent(client,
															Protocol.STORE_COMPLETE_TOKEN);
													synchronized (storeLock) {
														timedoutStore.remove(filename);
													}
													dstore_file_ports.put(filename, fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
													file_filesize.put(filename, filesize); // add new file's filesize
													success_Store = true;
													break;
												}
											}

											if (!success_Store) {
												System.err.println("Store timed out for: " + filename);
											}

											synchronized (storeLock) {
												fileToStore_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
											}
											files_activeStore.remove(filename);// FILE STORED REMOVE INDEX
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										synchronized (storeLock) {
											if (fileToStore_ACKPorts.containsKey(data[1]))
												fileToStore_ACKPorts.get(data[1]).add(dstoreport);// add ack port inside chmap
										}
										dstore_port_files.get(dstoreport).add(data[1]);
										dstore_port_numbfiles.put(dstoreport,
												dstore_port_numbfiles.get(dstoreport) + 1);

									} else

									//---------------------------------------------------------------------------------------------------------
									if ((command.equals(Protocol.REMOVE_ACK_TOKEN)
											|| command.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN))) { // Dstore REMOVE_ACK filename
										synchronized (removeLock) {
											if (fileToRemove_ACKPorts.containsKey(data[1])) {
												fileToRemove_ACKPorts.get(data[1]).remove(dstoreport);
											} // removing dstore with ack from list
										}
										dstore_port_files.get(dstoreport).remove(data[1]); //removes file from map of port
										dstore_file_ports.get(data[1]).remove(dstoreport);// remove port from file - ports map
										dstore_port_numbfiles.put(dstoreport,
												dstore_port_numbfiles.get(dstoreport) - 1); // suspend 1 from file count
									} else

									if (command.equals(Protocol.REBALANCE_COMPLETE_TOKEN) && activeRebalance) { // Dstore REMOVE_ACK filename
										rebalanceCompleteACK.incrementAndGet();
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN) || command.equals(Protocol.RELOAD_TOKEN)) { // Client LOAD
										if (data.length != 2) {
											System.err.println("Malformed message received for LOAD/RELOAD");
											continue;
										} // log error and continue
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
										} else {
											if (!file_filesize.containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												ControllerLogger.getInstance().messageSent(client,
														Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											} else {
												if (files_activeStore.contains(filename)
														|| files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													if (command.equals(Protocol.LOAD_TOKEN)) {
														outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														ControllerLogger.getInstance().messageSent(client,
																Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														continue;
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														ControllerLogger.getInstance().messageSent(client,
																Protocol.ERROR_LOAD_TOKEN);
														continue;
													}
												}

												while (activeRebalance) {
													continue;
												}

												if (command.equals(Protocol.LOAD_TOKEN)) {
													dstore_file_portsLeftReload.put(filename,
															new ArrayList<>(dstore_file_ports.get(filename)));
													outClient.println(Protocol.LOAD_FROM_TOKEN + " "
															+ dstore_file_portsLeftReload.get(filename).get(0) + " "
															+ file_filesize.get(filename));
													ControllerLogger.getInstance().messageSent(client,
															Protocol.LOAD_FROM_TOKEN + " "
																	+ dstore_file_portsLeftReload.get(filename).get(0)
																	+ " " + file_filesize.get(filename));
													dstore_file_portsLeftReload.get(filename).remove(0);
												} else {
													if (dstore_file_portsLeftReload.get(filename) != null
															&& !dstore_file_portsLeftReload.get(filename).isEmpty()) {
														outClient.println(Protocol.LOAD_FROM_TOKEN + " "
																+ dstore_file_portsLeftReload.get(filename).get(0) + " "
																+ file_filesize.get(filename));
														ControllerLogger.getInstance().messageSent(client,
																Protocol.LOAD_FROM_TOKEN
																		+ " " + dstore_file_portsLeftReload
																				.get(filename).get(0)
																		+ " " + file_filesize.get(filename));
														dstore_file_portsLeftReload.get(filename).remove(0);
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														ControllerLogger.getInstance().messageSent(client,
																Protocol.ERROR_LOAD_TOKEN);
													}
												}

											}
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_TOKEN)) { // CLIENT REMOVE
										if (data.length != 2) {
											System.err.println("Malformed message received for REMOVE");
											continue;
										} // log error and continue
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
										} else if (!file_filesize.containsKey(filename)
												|| files_activeStore.contains(filename)) {
											outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
										} else {
											synchronized (lock) {
												if (files_activeRemove.contains(filename)
														|| !file_filesize.containsKey(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
													ControllerLogger.getInstance().messageSent(client,
															Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
													continue;
												} else {
													while (activeRebalance) { // waiting for rebalance to end
														continue;
													}
													files_activeRemove.add(filename);
													file_filesize.remove(filename);// remove file_filesize so if broken rebalance should fix
												}
											}
											fileToRemove_ACKPorts.put(filename,
													new ArrayList<>(dstore_file_ports.get(filename))); // initializes the ports that wait for remove
											files_addCount.remove(filename);

											synchronized (removeLock) {
												for (Integer port : fileToRemove_ACKPorts.get(filename)) { // send ports file to delete
													Socket dstoreSocket = dstore_port_Socket.get(port);//MUST SYNC THIS LOOP
													PrintWriter outDstore = new PrintWriter(
															dstoreSocket.getOutputStream(), true);
													outDstore.println(Protocol.REMOVE_TOKEN + " " + filename);
													ControllerLogger.getInstance().messageSent(dstoreSocket,
															Protocol.REMOVE_TOKEN + " " + filename);
												}
											}

											boolean success_Remove = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
													outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
													ControllerLogger.getInstance().messageSent(client,
															Protocol.REMOVE_COMPLETE_TOKEN);
													dstore_file_ports.remove(filename);
													success_Remove = true;
													break;
												}
											}

											if (!success_Remove) {
												System.err.println("REMOVE timed out for: " + filename);
											}
											fileToRemove_ACKPorts.remove(filename);
											files_activeRemove.remove(filename); // remove file ActiveRemove from INDEX
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && isDstore && activeList) {// DSTORE LIST //for rebalance add && activeList
										ArrayList<String> filelist = new ArrayList<String>(Arrays.asList(data));
										filelist.remove(0); // remove command entry
										dstore_port_numbfiles.put(dstoreport, filelist.size()); // updates port/numbfiles hashmap
										dstore_port_files.put(dstoreport, filelist); // puts list in hashmap
										dstore_port_Socket.put(dstoreport, client);
										for (String string : filelist) {
											if (dstore_file_ports.get(string) == null) {
												dstore_file_ports.put(string, new ArrayList<Integer>());
											}
											if (!dstore_file_ports.get(string).contains(dstoreport))
												dstore_file_ports.get(string).add(dstoreport); // puts the given file the port that its in
										}
										listACKPorts.add(dstoreport);
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && !isDstore) { // Client LIST -> Client LIST file_list
										if (data.length != 1) {
											System.err.println("Malformed message received for LIST by Client");
											continue;
										} // log error and continue
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.ERROR_LOAD_TOKEN);
										} else if (file_filesize.size() != 0) {
											String filesList = String.join(" ", file_filesize.keySet());
											outClient.println(Protocol.LIST_TOKEN + " " + filesList);
											ControllerLogger.getInstance().messageSent(client,
													Protocol.LIST_TOKEN + " " + filesList);
										} else {
											outClient.println(Protocol.LIST_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.LIST_TOKEN);
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port
										if (data.length != 2) {
											System.err.println("Malformed message for Dstore Joining");
											continue;
										} // log error and continue
										dstoreport = Integer.parseInt(data[1]);
										if (dstore_port_numbfiles.containsKey(dstoreport)) { // checks if dstore port is already used
											System.err.println(
													"Connection refused, DStore port already used: " + dstoreport);
											client.close();
											break;
										}
										synchronized (DstoreJoinLock) {
											while (activeRebalance) {
												continue;
											}
											ControllerLogger.getInstance().dstoreJoined(client, dstoreport);
											dstore_port_files.put(dstoreport, new ArrayList<String>()); // initialize port number of dstore
											dstore_port_numbfiles.put(dstoreport, 0); // initialize port/numbfiles hashmap
											dstore_port_Socket.put(dstoreport, client);
											isDstore = true;
											Dstore_count.incrementAndGet();
											activeRebalance = true;
											this.rebalance_time = System.currentTimeMillis(); // turn on rebalance if not running
										}
									} else {
										System.err.println("Unrecognised or Timed Out Command! - " + dataline);
										continue; // log error
									}
								} else {
									if (isDstore) {
										System.err.println("DSTORE Disconnected!");
										Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
										while (activeRebalance) {
											continue;
										}
										synchronized (lock) {
											clearPort(dstoreport); // clear port data if dstore disonnected
										}
									}
									client.close();
									break;
								}
							}
						} catch (Exception e) {
							if (isDstore) {
								System.err.println("DSTORE CRASHED! -" + e);
								Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
								while (activeRebalance) {
									continue;
								}
								synchronized (lock) {
									clearPort(dstoreport); // clear port data if dstore disonnected
								}
							}
							System.err.println("Fatal error in client: " + e);
						}
					}).start();
				} catch (Exception e) {
					System.err.println("Could not create Socket: " + e);
				}
			}
		} catch (Exception e) {
			System.err.println("Could not create server socket: " + e);
		}
	}

	public static void main(String[] args) throws IOException {
		int cport = Integer.parseInt(args[0]);
		int R = Integer.parseInt(args[1]);
		int timeout = Integer.parseInt(args[2]);
		int rebalance_period = Integer.parseInt(args[3]);
		Controller controller = new Controller(cport, R, timeout, rebalance_period);
	}

	private String[] getPortsToStore(int R) { // finds R ports with least files
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

	public synchronized void rebalanceOperation() {
		try {
			if (Dstore_count.get() < R) {
				System.out.println("*********************Not Enough Dstore for REBALANCE*********************");
				return;
			}

			synchronized (lock) {
				activeRebalance = true;
				System.out.println("*********************Rebalance waiting for store/remove*********************");
				while (files_activeRemove.size() != 0 && files_activeStore.size() != 0) {
					continue;
				}
			}
			files_RemoveCount.clear();
			System.out.println("*********************Rebalance started*********************");
			Integer newDSCount = dstore_port_Socket.size(); // before rebalance count
			ArrayList<Integer> failedPorts = new ArrayList<>();

			//DO HERE
			System.out.println("*********************Sending LIST to all Dstores************************");
			activeList = true;
			for (Integer port : dstore_port_Socket.keySet()) { // send LIST command to each dstore
				try {
					PrintWriter outDSP = new PrintWriter(dstore_port_Socket.get(port).getOutputStream(), true);
					outDSP.println(Protocol.LIST_TOKEN);
					ControllerLogger.getInstance().messageSent(dstore_port_Socket.get(port), Protocol.LIST_TOKEN);
				} catch (Exception e) {
					System.err.println("Disconnected DSTORE " + port + "  ERROR:" + e);
					newDSCount--; //if dstore disconnected lower list asks
					failedPorts.add(port);
				}
			}

			listACKPorts.clear();
			long timeout_time = System.currentTimeMillis() + timeout;
			while (System.currentTimeMillis() <= timeout_time) {// checks if file to store has completed acknowledgements
				if (listACKPorts.size() >= newDSCount) {
					System.out.println("*********************Confirmed LIST from all*********************");
					break;
				}
			}
			activeList = false;

			for (Integer port : failedPorts) { // cleanup broken disconnected DStores
				//dstore_port_Socket.get(port).close();
				clearPort(port);
			}

			for (String file : dstore_file_ports.keySet()) { // clear extra files from failed removes
				if (dstore_file_ports.get(file).size() > R) {
					int remove = dstore_file_ports.get(file).size() - R;
					files_RemoveCount.put(file, remove);
				}
			}

			System.out.println("*********************End of LIST section*********************");
			// SECTION FOR SENDING REBALANCE OPERATION TO DSTORES WITH FILES TO SPREAD AND REMOVE
			sendRebalance();
			System.out.println("*********************Send REBALANCE commands to all*********************");
			Integer rebalanceExpected = dstore_port_Socket.size();
			timeout_time = System.currentTimeMillis() + timeout;
			while (System.currentTimeMillis() <= timeout_time) {
				if (rebalanceCompleteACK.get() >= rebalanceExpected) { // checks if file to store has completed acknowledgements
					System.out.println("*********************REBALANCE SUCCESSFULL*********************");
					break;
				}
			}
			System.out.println("*********************Rebalance END*********************");
			rebalanceCompleteACK.set(0);
			activeRebalance = false;
		} catch (Exception e) {
			activeRebalance = false;
			rebalanceCompleteACK.set(0);
			System.err.println("Rebalance Fatal Error: " + e);
		}
	}

	private void sendRebalance() {
		ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_filesTMP = new ConcurrentHashMap<Integer, ArrayList<String>>();
		for (Integer port : dstore_port_Socket.keySet()) { // function for sorting the REBALANCE files_to_send files_to_remove
			String files_to_send = "";
			String files_to_remove = "";
			Integer files_to_send_count = 0;
			Integer files_to_remove_count = 0;
			ArrayList<String> dstore_filesTMP = new ArrayList<String>();
			for (String file : dstore_port_files.get(port)) {
				if (files_addCount.containsKey(file)) {
					Integer[] portsToSendFile = getPortsToStoreFile(files_addCount.get(file), file);
					for (Integer pAdd : portsToSendFile) { // add files to structure
						dstore_filesTMP.add(file);
						dstore_port_numbfiles.put(pAdd, dstore_port_numbfiles.get(pAdd) + 1);
						dstore_file_ports.get(file).add(pAdd);
					}

					String[] portsToSendFileStr = new String[portsToSendFile.length];
					for (int i = 0; i < portsToSendFile.length; i++) {
						portsToSendFileStr[i] = portsToSendFile[i].toString();
					}
					String portcount = Integer.toString(portsToSendFile.length);
					String portsToSendStr = String.join(" ", portsToSendFileStr);

					files_addCount.remove(file);
					files_to_send_count++;
					files_to_send = files_to_send + " " + file + " " + portcount + " " + portsToSendStr;
				}

				if (!file_filesize.containsKey(file)) {
					files_to_remove = files_to_remove + " " + file;
					files_to_remove_count++;
					if (dstore_port_numbfiles.containsKey(port)) {
						dstore_port_numbfiles.put(port, dstore_port_numbfiles.get(port) - 1);
					}
					if (files_RemoveCount.containsKey(file)) {
						files_RemoveCount.remove(file);
					}
					if (dstore_file_ports.containsKey(file)) {
						dstore_file_ports.remove(file);
					}
				} else {
					if (files_RemoveCount.containsKey(file) && files_RemoveCount.get(file) > 0) {
						files_to_remove = files_to_remove + " " + file;
						files_to_remove_count++;
						files_RemoveCount.put(file, files_RemoveCount.get(file) - 1);
						if (dstore_port_numbfiles.containsKey(port)) {
							dstore_port_numbfiles.put(port, dstore_port_numbfiles.get(port) - 1);
						}
						if (dstore_file_ports.containsKey(file)) {
							if (dstore_file_ports.get(file).contains(port)) {
								dstore_file_ports.get(file).remove(port);
							}
						}
					}
				}
			}

			if (!dstore_port_filesTMP.containsKey(port)) {
				dstore_port_filesTMP.put(port, new ArrayList<>());
			}
			dstore_port_filesTMP.get(port).addAll(dstore_filesTMP);
			String message = "";
			message = " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove;
			try {
				Socket dsRebalance = dstore_port_Socket.get(port);
				PrintWriter outDSREBALANCE = new PrintWriter(dsRebalance.getOutputStream(), true);
				outDSREBALANCE.println(Protocol.REBALANCE_TOKEN + message);
				ControllerLogger.getInstance().messageSent(dsRebalance, Protocol.REBALANCE_TOKEN + message);
			} catch (IOException e) {
				System.err.println("Fatal Error while sending Rebalance - " + e);
			}
		}
		for (Integer port : dstore_port_filesTMP.keySet()) {
			dstore_port_files.get(port).addAll(dstore_port_filesTMP.get(port));
		}
	}

	private synchronized void clearPort(Integer port) {
		System.out.println("CLEARING DISCONNECTED PORT " + port);
		for (String file : dstore_port_files.get(port)) {
			if (files_addCount.get(file) == null) {
				files_addCount.put(file, 1);
			} else {
				files_addCount.put(file, files_addCount.get(file) + 1);
			}
		}
		dstore_port_files.remove(port);
		dstore_port_numbfiles.remove(port);
		dstore_port_Socket.remove(port);
		ConcurrentHashMap<String, ArrayList<Integer>> tempFilePorts = new ConcurrentHashMap<String, ArrayList<Integer>>(
				dstore_file_ports);
		for (String file : tempFilePorts.keySet()) {
			if (!file_filesize.keySet().contains(file)) {
				dstore_file_ports.remove(file);
			} else if (dstore_file_ports.get(file).contains(port)) {
				dstore_file_ports.get(file).remove(port);
			}
		}
		System.out.println("CLEARED PORT " + port);
	}

	private synchronized Integer[] getPortsToStoreFile(int n, String file) { // finds R ports with least files
		Integer ports[] = new Integer[n];

		for (Integer port : dstore_port_numbfiles.keySet()) {
			int max = 0;
			for (int i = 0; i < n; i++) {
				if (ports[i] == null) {
					max = i;
					ports[i] = port;
					break;
				}
				if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max])
						&& !dstore_port_files.get(ports[i]).contains(file)) {
					max = i;
				}
			}
			if (dstore_port_numbfiles.get(port) <= dstore_port_numbfiles.get(ports[max])
					&& !dstore_port_files.get(port).contains(file)) {
				ports[max] = port;
			} else if (dstore_port_numbfiles.get(port) > dstore_port_numbfiles.get(ports[max])
					&& dstore_port_files.get(ports[max]).contains(file)) {
				ports[max] = port;
			}
		}

		return ports;
	}

}