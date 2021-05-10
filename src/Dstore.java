import java.io.*;
import java.net.*;

public class Dstore {
	static Integer port;
	static Integer cport;
	static Integer timeout;
	static String file_folder;
	private boolean controller_fail = false;

	public Dstore(int port, int cport, int timeout, String file_folder) {
		Dstore.port = port;
		Dstore.cport = cport;
		Dstore.timeout = timeout;
		Dstore.file_folder = file_folder;
		try {
			startDstore();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void startDstore() throws IOException {
		File folder = new File(file_folder); // folder to make
		if (!folder.exists())
			if (!folder.mkdir())
				throw new RuntimeException("Cannot create new folder in " + folder.getAbsolutePath());
		final String path = folder.getAbsolutePath(); // path of folder
		for (File file : folder.listFiles()) // delete every file in directory if any
		{
			file.delete();
		}

		Socket controller = new Socket(InetAddress.getByName("localhost"), cport);

		new Thread(() -> { // CONTROLLER
			try {
				BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
				PrintWriter outController = new PrintWriter(controller.getOutputStream(),true);
				InputStream in = controller.getInputStream();
				String dataline = null;

				outController.println(Protocol.JOIN_TOKEN + " " + port);
				outController.flush();
				System.out.println("Entering loop of Controller");

				try {
					for (;;) {
						dataline = inController.readLine();
						if (dataline != null) {
							String[] data = dataline.split(" ");
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
							System.out.println("RECIEVED CONTROLLER COMMAND: " + command);

							if (command.equals(Protocol.REMOVE_TOKEN)) { // Controller LIST -> Controller file_list
								if(data.length!=2){continue;} // log error and continue
								System.out.println("Entered Remove from CONTROLLER");
								String filename = data[1];
								File fileRemove = new File(path + File.separator + filename);
								if (!fileRemove.exists() || !fileRemove.isFile()) {
									outController.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
									//outController.flush();
									System.out.println("File to delete not existant: " + filename);
								} else {
									fileRemove.delete();
									outController.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
									//outController.flush();
									System.out.println("Send Delete ACK to Controller for: " + filename);
								}
							} else

							if (command.equals(Protocol.REBALANCE_TOKEN)) { // Controller LIST -> Controller file_list
								//if(data.length!=2){continue;} // log error and continue
								System.out.println("Entered REBALANCE from CONTROLLER");
								
							} else

							if (command.equals(Protocol.LIST_TOKEN)) { // Controller LIST -> Controller file_list
								if(data.length!=1){continue;} // log error and continue
								System.out.println("Entered list");
								String[] fileList = folder.list();
								String listToSend = String.join(" ", fileList);
								outController.println(Protocol.LIST_TOKEN + " " + listToSend);
								//outController.flush();
								System.out.println("Send list");
							} else {
								System.out.println("Unrecognised command"); //log and continue
							}
						} else {
							if (controller.isConnected())controller.close();
							controller_fail = true;
							break;
						}
					}
				} catch (Exception e) {
					System.out.println("Controller Disconnected Error: " + e);
					if (controller.isConnected())
						controller.close();
					controller_fail = true;
				}
			} catch (Exception e) {
				System.out.println("Initial Controller connection error: " + e);
				controller_fail = true;
			}
		}).start();

		if (controller_fail)return; //exit if controller connection had failed
		System.out.println("GOING TO CLIENT PART");
		/* ---------------------------------CLIENTS PART----------------------------------------------*/
		try {
			ServerSocket ss = new ServerSocket(port);
			for (;;) {
				System.out.println("Client waiting");
				Socket client = ss.accept();
				new Thread(() -> { // CLIENTS
					try {
						System.out.println("Client NEW THEREAD");
						BufferedReader inController = new BufferedReader(
								new InputStreamReader(controller.getInputStream()));
						PrintWriter outController = new PrintWriter(controller.getOutputStream(),true);
						BufferedReader inClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
						PrintWriter outClient = new PrintWriter(client.getOutputStream(),true);

						String dataline = null;
						InputStream in = client.getInputStream();
						System.out.println("Client Connected");

						for (;;) {
							try {
								dataline = inClient.readLine();
								if (dataline != null) {
									String[] data = dataline.split(" ");
									String command;
									if (data.length==1) {
										command = dataline.trim();
										data[0] = command;
										dataline = null;
									} else {
										command = data[0];
										//dataline = null;
									}
									System.out.println("RECIEVED CLIENT COMMAND: " + command);

									if (command.equals(Protocol.STORE_TOKEN)) {
										//if(data.length!=3){continue;} // log error and continue
										outClient.println(Protocol.ACK_TOKEN);
										System.out.println("ENTERED STORE FROM CLIENT");
										//String filename = data[1];
										int filesize = Integer.parseInt(data[2]);
										File outputFile = new File(path + File.separator + data[1]);
										FileOutputStream out = new FileOutputStream(outputFile);
										out.write(in.readNBytes(filesize)); // possible threadlock?? maybe
										outController.println(Protocol.STORE_ACK_TOKEN + " " + data[1]);
										out.close();
										System.out.println("Acknowleded for Wrote file : " + data[1]);
										client.close();
										return;
									} else

									if (command.equals(Protocol.LOAD_DATA_TOKEN)) { // Client LOAD_DATA filename -> file_content
										if(data.length!=2){continue;} // log error and continue
										System.out.println("ENTERED LOAD FOR FILE: " + data[1]);
										String filename = data[1];
										File existingFile = new File(path + File.separator + filename);
										if (!existingFile.exists() || !existingFile.isFile()) {
											client.close();
											return;
										} // closes connection and exits thread

										int filesize = (int) existingFile.length(); // casting long to int file size limited to fat32
										FileInputStream inf = new FileInputStream(existingFile);
										OutputStream out = client.getOutputStream();								
										out.write(inf.readNBytes(filesize));
										out.flush();
										inf.close();
										out.close();
										client.close();
										return;
									} else{
										System.out.println("Unrecognised Command!");
										continue; // log error
									}
								} else {
									client.close();
									break;
								}
							} catch (Exception e) {
								System.out.println("Client error1 " + e);
								client.close();
								break;
							}
						}

					} catch (Exception e) {
						System.out.println("Client error2 " + e);
					}
				}).start();
			}
		} catch (Exception e) {
			System.out.println("Client error3 " + e);
		}
		System.out.println();
	}

	public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
		cport = Integer.parseInt(args[1]);
		timeout = Integer.parseInt(args[2]);
		file_folder = args[3];
		Dstore dstore = new Dstore(port, cport, timeout, file_folder);
	}
}