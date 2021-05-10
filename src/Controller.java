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
	AtomicInteger rebalanceCompleteACK = new AtomicInteger(0);
	boolean activeRebalance;
	boolean activeList;
	private Object lock = new Object();

	List<Dstore> Dstore_list; // list of Dstores
	ConcurrentHashMap<Integer, ArrayList<String>> dstore_port_files = new ConcurrentHashMap<Integer, ArrayList<String>>();
	ConcurrentHashMap<Integer, Integer> dstore_port_numbfiles = new ConcurrentHashMap<Integer, Integer>();
	ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_ports = new ConcurrentHashMap<String, ArrayList<Integer>>();
	ConcurrentHashMap<String, ArrayList<Integer>> fileToStore_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	ConcurrentHashMap<String, Integer> file_filesize = new ConcurrentHashMap<String, Integer>();
	ConcurrentHashMap<Integer, Socket> dstore_port_Socket = new ConcurrentHashMap<Integer, Socket>();
	ConcurrentHashMap<String, ArrayList<Integer>> fileToRemove_ACKPorts = new ConcurrentHashMap<String, ArrayList<Integer>>();
	List<Integer> listACKPorts = Collections.synchronizedList(new ArrayList<Integer>()); // index of active file stores
	List<String> files_activeStore = Collections.synchronizedList(new ArrayList<String>()); // index of active file stores
	List<String> files_activeRemove = Collections.synchronizedList(new ArrayList<String>()); // index of active file removes
	ConcurrentHashMap<String, Integer> files_addCount = new ConcurrentHashMap<String, Integer>();

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
			//new Thread(() -> { // REBALANCE OPERATION THREAD
			//	rebalanceOperation();
			//}).start();

			for (;;) {
				try {
					System.out.println("Waiting for connection");
					final Socket client = ss.accept();

					new Thread(() -> {
						try {
							System.out.println("Connected");
							BufferedReader inClient = new BufferedReader(
									new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream(),true);

							ConcurrentHashMap<String, ArrayList<Integer>> dstore_file_portsLeftReload = new ConcurrentHashMap<String, ArrayList<Integer>>();
							String dataline = null;
							Integer dstoreport = 0;
							boolean isDstore = false;
							for (;;) {

								dataline = inClient.readLine();
								if (dataline != null) {
									String[] data = dataline.split(" ");
									String command;
									if (data.length == 1) {
										command = dataline.trim();
										data[0] = command;
										dataline = null;
									} else {
										command = data[0];
										//data[data.length - 1] = data[data.length - 1].trim();
										//dataline = null;
									}

									System.out.println(
											"COMMAND RECIEVED \"" + command + "\" and data size is " + data.length);
									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.STORE_TOKEN)) { // CLIENT STORE
										if (data.length != 3) {
											continue;
										} // log error and continue
										System.out.println("ENTERED STORE FROM CLIENT for : " + data[1]);
										String filename = data[1];
										Integer filesize = Integer.parseInt(data[2]);

										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											System.out.println("SEND NOT ENOUGH DSTORES FOR STORE ERROR");
										} else if (file_filesize.get(filename) != null
												|| files_activeRemove.contains(filename)) { // checks if file exists or in Remove INDEX
											outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
											System.out.println("SEND FILE ALREADY EXISTS ERROR");
										} else {
											synchronized (lock) {
												if (files_activeStore.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
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
											//outClient.flush();

											boolean success_Store = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToStore_ACKPorts.get(filename).size() >= R) { // checks if file to store has completed acknowledgements
													outClient.println(Protocol.STORE_COMPLETE_TOKEN);
													System.out.println("SEND STORE COMPLETE ACK FOR: " + filename);
													System.out.println("ACK PORTS NOW FOR FILE: (" + filename + ") (" + fileToStore_ACKPorts.get(filename) + ")");
													dstore_file_ports.put(filename, fileToStore_ACKPorts.get(filename)); // update dstore_file_ports
													file_filesize.put(filename, filesize); // add new file's filesize
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
										try {
											fileToStore_ACKPorts.get(data[1]).add(dstoreport);// add ack port inside chmap
										} catch (Exception e) {
											e.printStackTrace();
											//TODO: handle exception
										}
										//if (data.length != 2) {
										//	System.out.println("STORE ACK SKIPPED for: " + data[1]);
										//	continue;
										//} // log error and continue
										//fileToStore_ACKPorts.get(data[1]).add(dstoreport); // add ack port inside chmap
										dstore_port_files.get(dstoreport).add(data[1]);
										dstore_port_numbfiles.put(dstoreport, dstore_port_files.get(dstoreport).size());
										System.out.println("RECIEVED ACK FOR: " + data[1] + " FROM PORT: " + dstoreport);
										//String filename = data[1];
										//System.out.println("acknowlefgements for: " + filename + " are now "
										//		+ fileToStore_ACKPorts.get(filename));

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.REMOVE_ACK_TOKEN)) { // Dstore REMOVE_ACK filename
										try {
											fileToRemove_ACKPorts.get(data[1]).remove(dstoreport);// removing dstore with ack from list
											dstore_port_files.get(dstoreport).remove(data[1]); //removes file from map of port
											dstore_file_ports.get(data[1]).remove(dstoreport);// remove port from file - ports map
										} catch (Exception e) {
											e.printStackTrace();
											//TODO: handle exception
										}
										System.out.println("ENTERED REMOVE ACK for: " + data[1]);
										dstore_port_numbfiles.put(dstoreport, dstore_port_numbfiles.get(dstoreport) - 1); // suspend 1 from file count
									} else

									if (command.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) { // Dstore REMOVE_ACK filename
										try {
											fileToRemove_ACKPorts.get(data[1]).remove(dstoreport);// removing dstore with ack from list
											{dstore_port_files.get(dstoreport).remove(data[1]);} //removes file from map of port
											{dstore_file_ports.get(data[1]).remove(dstoreport);}// remove port from file - ports map
										} catch (Exception e) {
											//TODO: handle exception
										}
										System.out.println("ENTERED REMOVE ACK BY ERROR for: " + data[1]);
										dstore_port_numbfiles.put(dstoreport, dstore_port_numbfiles.get(dstoreport) - 1); // suspend 1 from file count
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LOAD_TOKEN) || command.equals(Protocol.RELOAD_TOKEN)) { // Client LOAD
										if (data.length != 2) {
											continue;
										} // log error and continue
										System.out.println("ENTERED LOAD/RELOAD FROM CLIENT for file: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											//outClient.flush();
											System.out.println(
													"SEND NOT ENOUGH DSTORES FOR REMOVE ERROR for: " + filename);
										} else {
											if (!file_filesize.containsKey(filename)) { // CHECKS FILE CONTAINS AND INDEX
												outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
												//outClient.flush();
												System.out.println(
														"SEND ERROR_FILE_DOES_NOT_EXIST_TOKEN for: " + filename);
											} else {
												if (files_activeStore.contains(filename)
														|| files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													if (command.equals(Protocol.LOAD_TOKEN)) {
														outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
														//outClient.flush();
														continue;
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														//outClient.flush();
														continue;
													}
												}
												if (command.equals(Protocol.LOAD_TOKEN)) {
													System.out.println("IT WAS LOAD FROM CLIENT for file: " + data[1]);
													dstore_file_portsLeftReload.put(filename,
															new ArrayList<>(dstore_file_ports.get(filename)));
													outClient.println(Protocol.LOAD_FROM_TOKEN + " "
															+ dstore_file_portsLeftReload.get(filename).get(0) + " "
															+ file_filesize.get(filename));
													//outClient.flush();
													dstore_file_portsLeftReload.get(filename).remove(0);
												} else {
													System.out
															.println("IT WAS RELOAD FROM CLIENT for file: " + data[1]);
													if (dstore_file_portsLeftReload.get(filename) != null
															&& !dstore_file_portsLeftReload.get(filename).isEmpty()) {
														outClient.println(Protocol.LOAD_FROM_TOKEN + " "
																+ dstore_file_portsLeftReload.get(filename).get(0) + " "
																+ file_filesize.get(filename));
														//outClient.flush();
														dstore_file_portsLeftReload.get(filename).remove(0);
														System.out.println("********AFTERRRportsLeftReload******** is "
																+ filename);
													} else {
														outClient.println(Protocol.ERROR_LOAD_TOKEN);
														//outClient.flush();
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
											continue;
										} // log error and continue
										System.out.println("ENTERED REMOVE FOR FILE: " + data[1]);
										String filename = data[1];
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
											//outClient.flush();
											System.out.println(
													"SEND NOT ENOUGH DSTORES FOR REMOVE ERROR for: " + filename);
										} else if (dstore_file_ports.get(filename) == null
												|| dstore_file_ports.get(filename).isEmpty()
												|| files_activeStore.contains(filename)) {
											outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
											//outClient.flush();
											System.out.println("SEND FILE DOES NOT EXIST FOR: " + filename);
										} else {
											synchronized (lock) {
												if (files_activeRemove.contains(filename)) { // INDEX CHECKS FOR CONCURENT FILE STORE
													outClient.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
													//outClient.flush();
													continue;
												} else {
													while (activeRebalance) { // waiting for rebalance to end
														continue;
													}
													files_activeRemove.add(filename);
													file_filesize.remove(filename);// remove file_filesize so if broken rebalance should fix
												}
											}
											fileToRemove_ACKPorts.put(filename,new ArrayList<>(dstore_file_ports.get(filename))); // initializes the ports that wait for remove
											ArrayList<Integer> tempACKPORTS = new ArrayList<Integer>(
													fileToRemove_ACKPorts.get(filename));

											for (Integer port : tempACKPORTS) { // send ports file to delete
												Socket dstoreSocket = dstore_port_Socket.get(port);//MUST SYNC THIS LOOP
												PrintWriter outDstore = new PrintWriter(dstoreSocket.getOutputStream(),true);
												outDstore.println(Protocol.REMOVE_TOKEN + " " + filename);
												//outDstore.flush();
												System.out.println("************ASKED FOR REMOVE OF FILE: " + filename
														+ " on port " + port);
											}

											boolean success_Remove = false;
											long timeout_time = System.currentTimeMillis() + timeout;
											while (System.currentTimeMillis() <= timeout_time) {
												if (fileToRemove_ACKPorts.get(filename).size() == 0) { // checks if file to store has completed acknowledgements
													System.out.println("ENTERED LOOP TIMES");
													outClient.println(Protocol.REMOVE_COMPLETE_TOKEN);
													//outClient.flush();
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
									if (command.equals(Protocol.LIST_TOKEN) && !isDstore) { // Client LIST -> Client LIST file_list
										if (data.length != 1) {
											continue;
										} // log error and continue
										if (Dstore_count.get() < R) {
											outClient.println(Protocol.ERROR_LOAD_TOKEN);
											//outClient.flush();
											System.out.println("SEND NOT ENOUGH DSTORES FOR REMOVE ERROR");
										} else if(file_filesize.size()!=0){
											String filesList = String.join(" ", file_filesize.keySet());
											outClient.println(Protocol.LIST_TOKEN + " " + filesList);
											//outClient.flush();
											System.out.println("asked list from client");
										} else {
											outClient.println(Protocol.LIST_TOKEN);
											//outClient.flush();
										}
									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.LIST_TOKEN) && isDstore) { // DSTORE LIST //for rebalance add && activeList
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
										listACKPorts.add(dstoreport);

									} else

									//---------------------------------------------------------------------------------------------------------
									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port
										if (data.length != 2) {
											continue;
										} // log error and continue
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
										//outClient.flush();// delete later
										System.out.println("Send list to dstore");

									} else {
										System.out.println("Unrecognised Command!");
										continue; // log error
									}
								} else {
									if (isDstore){
										while(activeRebalance){
											continue;
										}
										Dstore_count.decrementAndGet(); //decrease count if dstore disconnected
										if(dstore_port_Socket.containsKey(dstoreport)){clearPort(dstoreport);} // clear port data if dstore disonnected
										client.close(); break;
									} else {client.close(); break;}
								}
							}
						} catch (Exception e) {
							System.out.println("error1 " + e);
							e.printStackTrace();
						}
					}).start();
				} catch (Exception e) {
					System.out.println("error2 " + e);
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.out.println("error3 " + e);
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

		synchronized (lock) {
			activeRebalance = true;
		}

		while (files_activeRemove.size() != 0 && files_activeRemove.size() != 0) {
			continue;
		}


		Integer newDSCount = dstore_port_Socket.size(); // before rebalance count
		ArrayList<Integer> failedPorts = new ArrayList<>();
		
		//DO HERE
		activeList=true;
		for (Integer port : dstore_port_Socket.keySet()) { // send LIST command to each dstore
			//Socket dspSocket = dstore_port_Socket.get(port);
			try{
				//BufferedReader inDSP = new BufferedReader(new InputStreamReader(dspSocket.getInputStream()));
				PrintWriter outDSP = new PrintWriter(dstore_port_Socket.get(port).getOutputStream(),true);
				outDSP.println(Protocol.LIST_TOKEN);
			}
			catch (Exception e) {
				System.out.println("Disconnected DSTORE " + port+  "  ERROR:"+ e);
				newDSCount--; //if dstore disconnected lower list asks
				failedPorts.add(port);
				//e.printStackTrace();
			}
		}

		boolean listPorts = false;
		listACKPorts.clear();

		long timeout_time = System.currentTimeMillis() + timeout;
		while (System.currentTimeMillis() <= timeout_time) {// checks if file to store has completed acknowledgements
			if (listACKPorts.size()>=newDSCount) {
				System.out.println("ENTERED LIST FROM ALL");
				listPorts = true;
				break;
			}
		}
		activeList=false;

		for (Integer port : failedPorts) { // cleanup broken disconnected DStores
			//dstore_port_Socket.get(port).close();
			dstore_port_Socket.remove(port);
		}

		// SECTION FOR SENDING REBALANCE OPERATION TO DSTORES WITH FILES TO SPREAD AND REMOVE
		sendRebalance();

		Integer rebalanceExpected = dstore_port_Socket.size();
		timeout_time = System.currentTimeMillis() + timeout;
		while (System.currentTimeMillis() <= timeout_time) {
			if (rebalanceCompleteACK.get()>=rebalanceExpected) { // checks if file to store has completed acknowledgements
				System.out.println("********************************REBALANCE SUCCESSFULL********************************************");
				break;
			}
		}

		activeRebalance = false; //reset state
	}

	private void sendRebalance(){
		for (Integer port : dstore_port_Socket.keySet()) {  // function for sorting the REBALANCE files_to_send files_to_remove
			String files_to_send="";
			String files_to_remove="";
			Integer files_to_send_count=0;
			Integer files_to_remove_count = 0;
			for (String file : dstore_port_files.get(port)) {
				if(files_addCount.keySet().contains(file)){
					String[] portsToSendFile = getPortsToStoreFile(files_addCount.get(file),file);
					String portcount = Integer.toString(portsToSendFile.length);
					String portsToSendStr = String.join(" ", portsToSendFile);
					files_to_send_count++;
					files_to_send = files_to_send + " " + file + " " + portcount + " " + portsToSendStr;
				}
				if(!file_filesize.contains(file)){
					files_to_remove = files_to_remove + " " + file;
					files_to_remove_count++;
				}	
			}

			String message = "";
			message = " " + files_to_send_count + files_to_send + " " + files_to_remove_count + files_to_remove;
			try {
				PrintWriter outDSREBALANCE = new PrintWriter(dstore_port_Socket.get(port).getOutputStream(),true);
				outDSREBALANCE.println(Protocol.REBALANCE_TOKEN + message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private synchronized void clearPort(Integer port){

		for (String file : dstore_port_files.get(port) ) {
			if(files_addCount.get(file)==null){
				files_addCount.put(file,1);
			}else {
				files_addCount.put(file,files_addCount.get(file)+1);
			}
		}
		dstore_port_files.remove(port);
		dstore_port_numbfiles.remove(port);
		dstore_port_Socket.remove(port);
		ConcurrentHashMap<String, ArrayList<Integer>> tempFilePorts = new ConcurrentHashMap<String, ArrayList<Integer>>(dstore_file_ports);
		for (String file : tempFilePorts.keySet()) {
			if(!file_filesize.keySet().contains(file)){
				dstore_file_ports.remove(file);
			}else if(dstore_file_ports.get(file).contains(port)){
				dstore_file_ports.get(file).remove(port);
			}
		}
	}

	private String[] getPortsToStoreFile(int R,String file) { // finds R ports with least files
		Integer ports[] = new Integer[R];

		for (Integer port : dstore_port_numbfiles.keySet()) {
			int max = 0;

			for (int i = 0; i < R; i++) {
				if (ports[i] == null) {
					max = i;
					ports[i] = port;
					break;
				}
				if (ports[i] != null && dstore_port_numbfiles.get(ports[i]) > dstore_port_numbfiles.get(ports[max]) && !dstore_port_files.get(ports[i]).contains(file)) {
					max = i;
				}
			}
			if (dstore_port_numbfiles.get(port) < dstore_port_numbfiles.get(ports[max]) && !dstore_port_files.get(port).contains(file)) {
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