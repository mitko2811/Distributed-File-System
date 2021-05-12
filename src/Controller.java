import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller{
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
										System.out.println("ENTERED STORE FROM CLIENT for : " + data[1]);
										String filename = data[1];
										Integer filesize = Integer.parseInt(data[2]);

										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE ERROR");
										} else if (file_filesize.get(filename) != null
												|| files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
											outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
											System.out.println("SEND FILE ALREADY EXISTS ERROR");
										} else {
											synchronized (lock) {
												if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
													ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
													continue;
												} else {
													while (activeRebalance) { // waiting for rebalance to end
														continue;
													}
													files_activeStore.add(filename);// ADD FILE STORING INDEX
												}
											}

											System.out.println("STORING INITIATED : " + filename);
											for (Integer port : dstore_port_numbfiles.keySet()) {
												System.out.println("port: " + port + " , files: "
														+ dstore_port_numbfiles.get(port));
											}
											String portsToStore[] = getPortsToStore(R);
											String portsToStoreString = String.join(" ", portsToStore);
											fileToStore_ACKPorts.put(filename, new ArrayList<Integer>());// initialize store file acks
											outClient.println(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
											ControllerLogger.getInstance().messageSent(client, Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
											timedoutStore.add(filename);

											boolean success_Store = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
													outClient.println(Protocol.STORE_COMPLETE_TOKEN);
													ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
													synchronized (storeLock) {
														timedoutStore.remove(filename);
													}
													System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
													System.out.println("ACK PORTS NOW FOR FILE: (" + filename + ") ("
															+ fileToStore_ACKPorts.get(filename) + ")");
													dstore_file_ports.put(filename, fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
													file_filesize.put(filename, filesize); // add new file's filesize
													success_Store = true;
													break;
												}
											}

											if (!success_Store) {
												System.err.println("Store timed out for: "+filename );
											}

											synchronized (storeLock) {
												fileToStore_ACKPorts.remove(filename); // remove stored file from fileToStore_ACKPorts queue
											}
											files_activeStore.remove(filename);// FILE STORED REMOVE INDEX
											System.out.println("EXITED STORE FOR: " + filename);
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										synchronized (storeLock) {
											if (fileToStore_ACKPorts.containsKey(data[1]))fileToStore_ACKPorts.get(data[1]).add(dstoreport);// add ack port inside chmap
										}
										dstore_port_files.get(dstoreport).add(data[1]);
										dstore_port_numbfiles.put(dstoreport, dstore_port_numbfiles.get(dstoreport)+1);
										System.out.println("RECIEVED ACK FOR: " + data[1] + " FROM PORT: " + dstoreport);

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
										System.out.println("RECIEVED REMOVE ACK for: " + data[1]);
									} else

									if (command.equals(Protocol.REBALANCE_COMPLETE_TOKEN) && activeRebalance) { // Dstore REMOVE_ACK filename
										System.out.println("ENTERED REBALANCE COMPLETE BY Dsport: " + dstoreport);
										rebalanceCompleteACK.incrementAndGet();
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN) || command.equals(Protocol.RELOAD_TOKEN)) { // Client LOAD
										if (data.length != 2) {
											System.err.println("Malformed message received for LOAD/RELOAD");
											continue;
										} // log error and continue
										System.out.println("ENTERED LOAD/RELOAD FROM CLIENT for file: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											System.out.println("NOT ENOUGH DSTORES FOR LOAD ERROR for: " + filename);
										} else {
											if (!file_filesize.containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												System.out.println(
														"SEND ERROR_FILE_DOES_NOT_EXIST_TOKEN for: " + filename);
											} else {
												if (files_activeStore.contains(filename)
														|| files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													if (command.equals(Protocol.LOAD_TOKEN)) {
														outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														continue;
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);
														continue;
													}
												}

												while (activeRebalance) {
													continue;
												}

												if (command.equals(Protocol.LOAD_TOKEN)) {
													System.out.println("IT WAS LOAD FROM CLIENT for file: " + data[1]);
													dstore_file_portsLeftReload.put(filename,
															new ArrayList<>(dstore_file_ports.get(filename)));
													outClient.println(Protocol.LOAD_FROM_TOKEN + " "+ dstore_file_portsLeftReload.get(filename).get(0) + " "+ file_filesize.get(filename));
													ControllerLogger.getInstance().messageSent(client, Protocol.LOAD_FROM_TOKEN + " "+ dstore_file_portsLeftReload.get(filename).get(0) + " "+ file_filesize.get(filename));
													dstore_file_portsLeftReload.get(filename).remove(0);
												} else {
													System.out
															.println("IT WAS RELOAD FROM CLIENT for file: " + data[1]);
													if (dstore_file_portsLeftReload.get(filename) != null
															&& !dstore_file_portsLeftReload.get(filename).isEmpty()) {
														outClient.println(Protocol.LOAD_FROM_TOKEN + " "
																+ dstore_file_portsLeftReload.get(filename).get(0) + " "
																+ file_filesize.get(filename));
														ControllerLogger.getInstance().messageSent(client, Protocol.LOAD_FROM_TOKEN + " "
														+ dstore_file_portsLeftReload.get(filename).get(0) + " "
														+ file_filesize.get(filename));
														dstore_file_portsLeftReload.get(filename).remove(0);
														System.out.println("********AFTERRRportsLeftReload******** is "
																+ filename);
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);
														System.out.println(
																"SEND ERROR LOAD TO CLIENT FOR FILE: " + filename);
													}
												}

											}
											System.out.println("EXITED LOAD/reload FROM CLIENT for file: " + filename);
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_TOKEN)) { // CLIENT REMOVE
										if (data.length != 2) {
											System.err.println("Malformed message received for REMOVE");
											continue;
										} // log error and continue
										System.out.println("ENTERED REMOVE FOR FILE: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											System.out.println("NOT ENOUGH DSTORES FOR REMOVE ERROR for: " + filename);
										} else if (!file_filesize.containsKey(filename)
												|| files_activeStore.contains(filename)) {
											outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											System.out.println("SEND FILE DOES NOT EXIST FOR: " + filename);
										} else {
											synchronized (lock) {
												if (files_activeRemove.contains(filename)
														|| !file_filesize.containsKey(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
													ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
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
													ControllerLogger.getInstance().messageSent(dstoreSocket, Protocol.REMOVE_TOKEN + " " + filename);
													System.out.println("************ASKED FOR REMOVE OF FILE: "
															+ filename + " on port " + port);
												}
											}

											boolean success_Remove = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
													System.out.println("ENTERED LOOP TIMES");
													outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
													ControllerLogger.getInstance().messageSent(client, Protocol.REMOVE_COMPLETE_TOKEN);
													System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
													dstore_file_ports.remove(filename);
													success_Remove = true;
													break;
												}
											}

											if (!success_Remove) {
												System.err.println("REMOVE timed out for: "+filename );
											}

											for (String string : file_filesize.keySet()) {
												System.out.println("FILES AFTER REMOVE - " + string);
											}
											fileToRemove_ACKPorts.remove(filename);
											files_activeRemove.remove(filename); // remove file ActiveRemove from INDEX
											System.out.println("EXITED REMOVE for: " + filename);
										}

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && !isDstore) { // Client LIST -> Client LIST file_list
										if (data.length != 1) {
											System.err.println("Malformed message received for LIST by Client");
											continue;
										} // log error and continue
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else if (file_filesize.size() != 0) {
											String filesList = String.join(" ", file_filesize.keySet());
											outClient.println(Protocol.LIST_TOKEN + " " + filesList);
											ControllerLogger.getInstance().messageSent(client, Protocol.LIST_TOKEN + " " + filesList);
											System.out.println("asked list from client");
										} else {
											outClient.println(Protocol.LIST_TOKEN);
											ControllerLogger.getInstance().messageSent(client, Protocol.LIST_TOKEN);
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && isDstore && activeList) {// DSTORE LIST //for rebalance add && activeList
										ArrayList<String> filelist = new ArrayList<String>(Arrays.asList(data));
										filelist.remove(Protocol.LIST_TOKEN); // remove command entry
										dstore_port_numbfiles.put(dstoreport, filelist.size()); // updates port/numbfiles hashmap
										dstore_port_files.put(dstoreport, filelist); // puts list in hashmap
										dstore_port_Socket.put(dstoreport, client);
										for (String string : filelist) {
											if (dstore_file_ports.get(string) == null) {
												dstore_file_ports.put(string, new ArrayList<Integer>());
											}
											if(!dstore_file_ports.get(string).contains(dstoreport)) dstore_file_ports.get(string).add(dstoreport); // puts the given file the port that its in
											System.out.println("Dstore port: " + dstoreport + " File: " + string);
										}
										listACKPorts.add(dstoreport);

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port
										if (data.length != 2) {
											System.err.println("Malformed message for Dstore Joining");
											continue;
										} // log error and continue
										System.out.println("Entered Join from Dstore port is " + data[1]);
										dstoreport = Integer.parseInt(data[1]);
										if (dstore_port_numbfiles.containsKey(dstoreport)) { // checks if dstore port is already used
											System.out.println("DStore port already used: " + dstoreport);
											System.out.println("Connection refused for port: " + dstoreport);
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
								System.err.println("DSTORE CRASHED!");
								Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
								while (activeRebalance) {
									continue;
								}
								synchronized (lock) {
									clearPort(dstoreport); // clear port data if dstore disonnected
								}
							}
							System.err.println("Fatal error in client: " + e);
							e.printStackTrace();
						}
					}).start();
				} catch (Exception e) {
					System.err.println("Could not create Socket: " + e);
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println("Could not create server socket: " + e);
			e.printStackTrace();
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
				System.out.println("NOT ENOUGH DSTORES FOR REBALANCE");
				return;
			}

			synchronized (lock) {
				activeRebalance = true;
				System.out.println("*********************Rebalance waiting store/remove************************");
				while (files_activeRemove.size() != 0 && files_activeStore.size() != 0) {
					continue;
				}
			}
			files_RemoveCount.clear();
			System.out.println("*********************Rebalance started************************");
			Integer newDSCount = dstore_port_Socket.size(); // before rebalance count
			ArrayList<Integer> failedPorts = new ArrayList<>();

			//DO HERE
			activeList = true;
			for (Integer port : dstore_port_Socket.keySet()) { // send LIST command to each dstore
				try {
					PrintWriter outDSP = new PrintWriter(dstore_port_Socket.get(port).getOutputStream(), true);
					outDSP.println(Protocol.LIST_TOKEN);
					ControllerLogger.getInstance().messageSent(dstore_port_Socket.get(port), Protocol.LIST_TOKEN);
					for (String file : dstore_port_files.get(port)) {
						System.out.println("FILES IN PORT: " + port + " FILE: " + file);
					}
				} catch (Exception e) {
					System.out.println("Disconnected DSTORE " + port + "  ERROR:" + e);
					newDSCount--; //if dstore disconnected lower list asks
					failedPorts.add(port);
					e.printStackTrace();
				}
				System.out.println("END FILES OF: " + port);
			}

			System.out.println("*********************SEND LIST TO ALL DSTORE************************");
			listACKPorts.clear();

			long timeout_time = System.currentTimeMillis() + timeout;
			while (System.currentTimeMillis() <= timeout_time) {// checks if file to store has completed acknowledgements
				if (listACKPorts.size() >= newDSCount) {
					System.out.println("ENTERED LIST FROM ALL");
					break;
				}
			}
			activeList = false;

			for (Integer port : failedPorts) { // cleanup broken disconnected DStores
				//dstore_port_Socket.get(port).close();
				clearPort(port);
			}

			for (Integer prt : dstore_port_numbfiles.keySet()) {
				System.out.println("===================port: "+ prt+"=========numbfiles: " + dstore_port_numbfiles.get(prt)+"===============================================================");
			}

			for (String file : dstore_file_ports.keySet()) {
				if(dstore_file_ports.get(file).size()>R){
					int remove = dstore_file_ports.get(file).size() - R;
					files_RemoveCount.put(file,remove);
					System.out.println("=============================file: "+file+"====================times remove: "+remove+"===========================================================");
				}
			}

			System.out.println("*********************REVIEVED/TIMEDOUT LIST************************");
			// SECTION FOR SENDING REBALANCE OPERATION TO DSTORES WITH FILES TO SPREAD AND REMOVE
			sendRebalance();
			System.out.println("*********************SEND REBALANCE TO ALL************************");
			Integer rebalanceExpected = dstore_port_Socket.size();
			timeout_time = System.currentTimeMillis() + timeout;
			while (System.currentTimeMillis() <= timeout_time) {
				if (rebalanceCompleteACK.get() >= rebalanceExpected) { // checks if file to store has completed acknowledgements
					System.out.println(
							"********************************REBALANCE SUCCESSFULL********************************************");
					break;
				}
			}

			for (String string : file_filesize.keySet()) {
				System.out.println("FILES AFTER REBALANCE - " + string);
			}
			System.out.println("*********************Rebalance END************************");
			rebalanceCompleteACK.set(0);
			activeRebalance = false;
		} catch (Exception e) {
			activeRebalance = false;
			System.err.println("Rebalance Fatal Error: " + e);
			e.printStackTrace();
		}
	}

	private void sendRebalance() {
		ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_filesTMP = new ConcurrentHashMap<Integer, ArrayList<String>>();
		for (Integer port : dstore_port_Socket.keySet()) { // function for sorting the REBALANCE files_to_send files_to_remove
			System.out.println("REEEEEEEEEEEEEEEEEEEEEEEEEEEEE PORT: " + port);
			String files_to_send = "";
			String files_to_remove = "";
			Integer files_to_send_count = 0;
			Integer files_to_remove_count = 0;
			System.out.println("*********************SEND1***********************");
			ArrayList<String> dstore_filesTMP = new ArrayList<String>();
			for (String file : dstore_port_files.get(port)) {
				System.out.println("*********************SEND2***********************");
				if (files_addCount.containsKey(file)) {
					System.out.println("*********************SEND3***********************");
					Integer[] portsToSendFile = getPortsToStoreFile(files_addCount.get(file), file);
					System.out.println("*********************SEND4***********************");
					System.out.println(Arrays.toString(portsToSendFile) + " " + file);
					for (Integer pAdd : portsToSendFile) { // add files to structure
						dstore_filesTMP.add(file);
						dstore_port_numbfiles.put(pAdd, dstore_port_numbfiles.get(pAdd) + 1);
						dstore_file_ports.get(file).add(pAdd);
					}

					String[] portsToSendFileStr = new String[portsToSendFile.length];
					for (int i = 0; i < portsToSendFile.length; i++) {
						portsToSendFileStr[i] = portsToSendFile[i].toString();
					}

					System.out.println("*********************SEND5***********************");
					System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOO SIZE OF PORTSTOSENDFILE : " + portsToSendFile.length);
					String portcount = Integer.toString(portsToSendFile.length);
					String portsToSendStr = String.join(" ", portsToSendFileStr);
					System.out.println("*********************SEND6***********************");
					System.out.println("UUUUUUUUUUUUUUUUUUUUUU portsToSendStr : " + portsToSendFile.length);

					files_addCount.remove(file);
					System.out.println("*********************SEND7***********************");
					files_to_send_count++;
					files_to_send = files_to_send + " " + file + " " + portcount + " " + portsToSendStr;
				}
				System.out.println("*********************SEND8***********************");

				if (!file_filesize.containsKey(file)) {
					System.out.println("LLALALALLALALALALLAAALALLALALAL : " + file);
					files_to_remove = files_to_remove + " " + file;
					files_to_remove_count++;
					if(dstore_port_numbfiles.containsKey(port)){
						dstore_port_numbfiles.put(port,dstore_port_numbfiles.get(port)-1);
					}
					if(files_RemoveCount.containsKey(file)){files_RemoveCount.remove(file);}
					if(dstore_file_ports.containsKey(file)){dstore_file_ports.remove(file);}
				} else {
					if (files_RemoveCount.containsKey(file) && files_RemoveCount.get(file)>0){
					files_to_remove = files_to_remove + " " + file;
					files_to_remove_count++;
					files_RemoveCount.put(file,files_RemoveCount.get(file)-1);
					if(dstore_port_numbfiles.containsKey(port)){
						dstore_port_numbfiles.put(port,dstore_port_numbfiles.get(port)-1);
					}
					if(dstore_file_ports.containsKey(file)){
						if(dstore_file_ports.get(file).contains(port)){
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

			System.out.println("*********************SEND9***********************");
			String message = "";
			message = " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove;
			try {
				Socket dsRebalance = dstore_port_Socket.get(port);
				PrintWriter outDSREBALANCE = new PrintWriter(dsRebalance.getOutputStream(), true);
				outDSREBALANCE.println(Protocol.REBALANCE_TOKEN + message);
				ControllerLogger.getInstance().messageSent(dsRebalance, Protocol.REBALANCE_TOKEN + message);
				System.out.println("@@@@@@@@@@@@@@@@@@@@" + Protocol.REBALANCE_TOKEN + message
						+ "@@@@@@@@@@@@@@@@@@@@ for port: " + port);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("*********************SEND10***********************");
		for (Integer port : dstore_port_filesTMP.keySet()) {
			dstore_port_files.get(port).addAll(dstore_port_filesTMP.get(port));
		}
		//this.dstore_port_files = new ConcurrentHashMap<>(dstore_port_filesTMP); // update hashmap

	}

	private synchronized void clearPort(Integer port) {
		System.out.println("CLEARING PORT " + port + "********************************************");
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
		System.out.println("CLEARED PORT " + port + "********************************************");
	}

	private synchronized Integer[] getPortsToStoreFile(int n, String file) { // finds R ports with least files
		Integer ports[] = new Integer[n];

		for (Integer port : dstore_port_numbfiles.keySet()) {
			int max = 0;
			System.out.println("--------port: " + port + "------------------------------------numbfiles: "
					+ dstore_port_numbfiles.get(port));
			System.out.println(
					"PORT " + port + " CONTAINS FILE " + file + " is " + dstore_port_files.get(port).contains(file));
			for (int i = 0; i < n; i++) {
				if (ports[i] == null) {
					max = i;
					ports[i] = port;
					break;
				}
				if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max])
						&& !dstore_port_files.get(ports[i]).contains(file)) {
					max = i;
					System.out.println(
							"-------processing MAX-port: " + port + "  FILESS: " + dstore_port_files.get(ports[i]));
				}
			}
			if (dstore_port_numbfiles.get(port) <= dstore_port_numbfiles.get(ports[max])
					&& !dstore_port_files.get(port).contains(file)) {
				ports[max] = port;
				System.out
						.println("-------CURRENT MAX-port: " + port + " FILESS :" + dstore_port_files.get(ports[max]));
			} else if (dstore_port_numbfiles.get(port) > dstore_port_numbfiles.get(ports[max])
					&& dstore_port_files.get(ports[max]).contains(file)) {
				ports[max] = port;
				System.out
						.println("-------CURRENT MAX-port: " + port + " FILESS :" + dstore_port_files.get(ports[max]));
			}
		}

		return ports;
	}

}