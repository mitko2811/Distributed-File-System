import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Controller {
	static int cport;
	static int R; // Replication factor - number of Dstores
	static int timeout;
	static int rebalance_period;

	List<Dstore> Dstore_list; // list of Dstores
	static HashMap<Integer, ArrayList<String>> dstore_ports_files = new HashMap<Integer, ArrayList<String>>();
	static HashMap<ArrayList<String>, Integer> dstore_files_ports = new HashMap<ArrayList<String>,Integer>();
	static HashMap<String, Integer> dstore_file_ports = new HashMap<String,Integer>();
	static ConcurrentHashMap<String, Integer> fileToStore_ACK = new ConcurrentHashMap<String, Integer>();

	public static void main(String[] args) throws IOException {
		cport = Integer.parseInt(args[0]);
		R = Integer.parseInt(args[1]);
		timeout = Integer.parseInt(args[2]);
		rebalance_period = Integer.parseInt(args[3]);

		String index="Waiting";

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
							BufferedWriter outToClient = new BufferedWriter(
									new OutputStreamWriter(client.getOutputStream()));
							InputStream in = client.getInputStream();
							byte[] buf = new byte[10000];
							int buflen = -1;
							int dstoreport=0;
							boolean isDstore = false;
							int storeACKnumber = 0;
							String fileToStore = "";
							System.out.println("test1");
							for (;;) {

								if(fileToStore!="" && fileToStore_ACK.get(fileToStore)!=null && fileToStore_ACK.get(fileToStore)>=R){ // checks if file to store has completed acknowledgements
									outToClient.write(Protocol.STORE_COMPLETE_TOKEN);
									fileToStore="";
								}


								buflen = in.read(buf);
								if (buflen != -1) {

									String firstBuffer = new String(buf, 0, buflen);
									int firstSpace = firstBuffer.indexOf(" ");
									String command; // LIST

									if (firstSpace == -1) {
										command = firstBuffer;
										firstBuffer = null;
										System.out.println("NQMA SPACE");
									} else {
										command = firstBuffer.substring(0, firstSpace);
										firstBuffer = firstBuffer.substring(firstSpace + 1);
									}

									System.out.println("command \"" + command + "\" size is " + command.length());

									if (command.equals(Protocol.STORE_TOKEN)) { // Client STORE filename filesize -> Client STORE_TO port1 port2 ... portR |> Dstores STORE_ACK filename -> Client STORE_COMPLETE
										System.out.println("ENTERED STORE");

										String following[] = firstBuffer.split(" ");
										String filename = following[0];
										int filesize = Integer.parseInt(following[1]);
										String portsToStore[] = getPortsToStore(dstore_ports_files,filename);
										String portsToStoreString = String.join(" ", portsToStore);

										fileToStore = filename;
										outToClient.write(Protocol.STORE_TO_TOKEN + " " + portsToStoreString);
										outToClient.flush();

									} else 

									if (command.equals(Protocol.STORE_ACK_TOKEN)) { // Dstore Store_ACK filename
										System.out.println("ENTERED STORE ACK");

										String following[] = firstBuffer.split(" ");
										String filename = following[0];
										fileToStore_ACK.put(filename,fileToStore_ACK.get(filename) + 1); // increase ack number inside chmap


									} else

									// if (command.equals(Protocol.LOAD_TOKEN)) { // Client LOAD filename -> Client
									// LOAD_FROM port
									// // filesize
									// System.out.println("ENTERED LOAD");
									// int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
									// String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
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
									// int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
									// String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
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

									if (command.equals(Protocol.LIST_TOKEN) && firstBuffer==null) { // Client LIST -> Client LIST file_list

										System.out.println("asked list from client");
										outToClient.write(Protocol.LIST_TOKEN + " " + "Deez.nuts da.baby");
									} else	

									if (command.equals(Protocol.LIST_TOKEN) && firstBuffer!=null) { // DSTORE LIST
										String filelist[];
										filelist = firstBuffer.split(" ");
										//if(dstoreport!=0) dstore_ports_files.put(dstoreport,filelist);
										System.out.println("list is " + firstBuffer);
										System.out.println("list 1 is " + filelist[0]);
										System.out.println("list 2 is " + filelist[1]);
									} else



									if (command.equals(Protocol.JOIN_TOKEN)) { // Dstore JOIN port -> Dstores LIST |>
																				// Dsotres
																				// LIST file_list -> Dstores REBALANCE
																				// files_to_send files_to_remove
										System.out.println("entered Join in controller");
										System.out.println("port is " + firstBuffer);
										dstoreport = Integer.parseInt(firstBuffer);
										dstore_ports_files.put(dstoreport,null); // initialize port number of dstore
										isDstore = true;
										//outToClient.write(Protocol.LIST_TOKEN);
										//outToClient.flush();
										System.out.println("TRIED TO SEND LIST TO DSTORE");
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



	private static String[] getPortsToStore(HashMap<Integer, ArrayList<String>> dstore_ports,String filename){
		


		return null;

	}
}