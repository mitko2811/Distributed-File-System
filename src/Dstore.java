import java.io.*;
import java.net.*;

public class Dstore {
	static int port;
	static int cport;
	static int timeout;
	static String file_folder;
	private static Dstore dstore;
	private boolean disconnected = true;

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
		Socket controller = new Socket(InetAddress.getByName("localhost"), cport);

		new Thread(() -> { // CONTROLLER
			try {
				BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
				PrintWriter outController = new PrintWriter(controller.getOutputStream());
				InputStream in = controller.getInputStream();
				String data=null;
				String fileToWrite = "";

				if (disconnected) {
					outController.println(Protocol.JOIN_TOKEN + " " + port);
					outController.flush();
					disconnected = false;
				}

				System.out.println("Entering loop of Controller");

				for (;;) {
					try {
						if (fileToWrite != "") { // checks if client thread ready with file write
							outController.println(Protocol.STORE_ACK_TOKEN + " " + fileToWrite);
							outController.flush();
							System.out.println("SEND FILE ACK STORE : " + fileToWrite);
							fileToWrite = "";
						}

						data = inController.readLine();
						if (data != null) {
							System.out.println("PASSED READING");
							int firstSpace = data.indexOf(" ");
							String command;
							if (firstSpace == -1) {
								command = data;
								data = "";
							} else {
								command = data.substring(0, firstSpace);
								data = data.substring(firstSpace + 1);
							}
							System.out.println("command " + command);

							if (command.equals(Protocol.STORE_TOKEN)) { // Client STORE filename filesize -> Client ACK
																		// |> Client file_content -> Controller
																		// STORE_ACK filename
								System.out.println("ENTERED STORE");

								String following[] = data.split(" ");
								String filename = following[0];
								int filesize = Integer.parseInt(following[1]);

							} else

							// if (command.equals("LOAD_DATA")) { // Client LOAD_DATA filename ->
							// file_content
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

							// if (command.equals("REMOVE")) { // Controller REMOVE filename -> Controller
							// REMOVE_ACK filename
							// int secondSpace = data.indexOf(" ", firstSpace + 1);
							// String fileName = data.substring(firstSpace + 1, secondSpace);
							// System.out.println("fileName " + fileName);
							// File outputFile = new File(fileName);
							// FileOutputStream out = new FileOutputStream(outputFile);
							// out.write(buf, secondSpace + 1, buflen - secondSpace - 1);
							// while ((buflen = in.read(buf)) != -1) {
							// System.out.print("*");
							// out.write(buf, 0, buflen);
							// }
							// in.close();
							// client.close();
							// out.close();
							// } else

							if (command.equals(Protocol.LIST_TOKEN)) { // Controller LIST -> Controller file_list
								System.out.print("Entered list");
								String[] fileList = folder.list();
								String listToSend = String.join(" ", fileList);
								outController.println(Protocol.LIST_TOKEN + " " + listToSend);
								outController.flush();
								System.out.println("Send list");
								// outController.close();
							} else
								System.out.println("unrecognised command");

							// if (command.equals("REBALANCE")) { // Controller REBALANCE files_to_send
							// files_to_remove ->
							// // Controller file_list
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
							// System.out.println("unrecognised command");
						}
					} catch (Exception e) {
						System.out.println("error " + e);
					}
				}
			} catch (Exception e) {
				System.out.println("error " + e);
			}
		}).start();

		System.out.println("GOING TO CLIENT PART");
/* ---------------------------------CLIENTS PART----------------------------------------------*/
		try {
			ServerSocket ss = new ServerSocket(port);
			for (;;) {
				System.out.println("For");
				if (disconnected == false) {
					Socket client = ss.accept();
					System.out.println("Client 1");
					new Thread(() -> { // CLIENTS
						try {
							System.out.println("Client 2");
							BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
							PrintWriter outController = new PrintWriter(controller.getOutputStream());
							BufferedReader inClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
							PrintWriter outClient = new PrintWriter(client.getOutputStream());

							String data=null;
							InputStream in = client.getInputStream();
							System.out.println("Client Connected");

							for (;;) {
								try {
									data = inClient.readLine();
									if (data != null) {
										int firstSpace = data.indexOf(" ");
										String command;

										if (firstSpace == -1) {
											command = data;
											data = "";
										} else {
											command = data.substring(0, firstSpace);
											data = data.substring(firstSpace + 1);
										}
										System.out.println("command " + command);

										if (command.equals(Protocol.STORE_TOKEN)) {
											System.out.println("ENTERED STORE FROM CLIENT");

											String following[] = data.split(" ");
											String filename = following[0];
											int filesize = Integer.parseInt(following[1]);
											outClient.println(Protocol.ACK_TOKEN);
											outClient.flush();

											File outputFile = new File(path + File.separator + filename);
											FileOutputStream out = new FileOutputStream(outputFile);
											out.write(in.readNBytes(filesize));
											out.flush();
											out.close();
											outController.println(Protocol.STORE_ACK_TOKEN + " " + filename);
											outController.flush();
											System.out.println("Acknowleded for Wrote file : " + filename);
										} else

										// if (command.equals("LOAD_DATA")) { // Client LOAD_DATA filename ->
										// file_content
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

										// if (command.equals("REMOVE")) { // Controller REMOVE filename ->
										// Controller
										// REMOVE_ACK filename
										// int secondSpace = data.indexOf(" ", firstSpace + 1);
										// String fileName = data.substring(firstSpace + 1, secondSpace);
										// System.out.println("fileName " + fileName);
										// File outputFile = new File(fileName);
										// FileOutputStream out = new FileOutputStream(outputFile);
										// out.write(buf, secondSpace + 1, buflen - secondSpace - 1);
										// while ((buflen = in.read(buf)) != -1) {
										// System.out.print("*");
										// out.write(buf, 0, buflen);
										// }
										// in.close();
										// client.close();
										// out.close();
										// } else

										if (command.equals(Protocol.LIST_TOKEN)) { // Controller LIST ->
																					// Controller
																					// file_list
											String[] fileList = folder.list();
											String listToSend = String.join(" ", fileList);
											outClient.println(Protocol.LIST_TOKEN + " " + listToSend);
											outClient.flush();
											// outController.close();
										} else
											System.out.println("unrecognised command");

										// if (command.equals("REBALANCE")) { // Controller REBALANCE
										// files_to_send
										// files_to_remove ->
										// // Controller file_list
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
										// System.out.println("unrecognised command");
									}
								} catch (Exception e) {
									System.out.println("error " + e);
								}
							}

						} catch (Exception e) {
							System.out.println("error " + e);
						}
					}).start();
				}
			}
		} catch (Exception e) {
			System.out.println("error " + e);
		}
		System.out.println();
	}

	public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
		cport = Integer.parseInt(args[1]);
		timeout = Integer.parseInt(args[2]);
		file_folder = args[3];
		dstore = new Dstore(port, cport, timeout, file_folder);
	}
}