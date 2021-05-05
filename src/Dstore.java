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
				BufferedWriter outController = new BufferedWriter(new OutputStreamWriter(controller.getOutputStream()));
				InputStream in = controller.getInputStream();


				String fileToWrite = "";

				if (disconnected) {
					outController.write(Protocol.JOIN_TOKEN + " " + port);
					outController.flush();
					disconnected = false;
				}
				byte[] buf = new byte[1000];
				int buflen;
				System.out.println("Entering loop of Controller");

				for (;;) {
					try {
						if(fileToWrite!=""){ // checks if client thread ready with file write
							outController.write(Protocol.STORE_ACK_TOKEN + " " + fileToWrite);
							fileToWrite="";
						}

						buflen = in.read(buf);
						if (buflen != -1) {
							System.out.println("PASSED READING");
							String firstBuffer = new String(buf, 0, buflen);
							int firstSpace = firstBuffer.indexOf(" ");
							String command;
							System.out.println("Command Before");
							if (firstSpace == -1) {
								command = firstBuffer;
								firstBuffer = "";
								System.out.println("NQMA SPACE");
							} else {
								command = firstBuffer.substring(0, firstSpace);
								firstBuffer = firstBuffer.substring(firstSpace + 1);
							}
							System.out.println("command " + command);



							if (command.equals(Protocol.STORE_TOKEN)) { // Client STORE filename filesize -> Client ACK
																		// |> Client file_content -> Controller
																		// STORE_ACK filename
								System.out.println("ENTERED STORE");

								String following[] = firstBuffer.split(" ");
								String filename = following[0];
								int filesize = Integer.parseInt(following[1]);

							} else

							// if (command.equals("LOAD_DATA")) { // Client LOAD_DATA filename ->
							// file_content
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

							// if (command.equals("REMOVE")) { // Controller REMOVE filename -> Controller
							// REMOVE_ACK filename
							// int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
							// String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
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
								String[] fileList = folder.list();
								String listToSend = String.join(" ", fileList);
								outController.write(Protocol.LIST_TOKEN + " " + listToSend);
								outController.flush();
								// outController.close();
							} else
								System.out.println("unrecognised command");

							// if (command.equals("REBALANCE")) { // Controller REBALANCE files_to_send
							// files_to_remove ->
							// // Controller file_list
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

		new Thread(() -> { // CLIENTS
			try {
				ServerSocket ss = new ServerSocket(port);
				BufferedReader inController = new BufferedReader(new InputStreamReader(controller.getInputStream()));
				BufferedWriter outController = new BufferedWriter(new OutputStreamWriter(controller.getOutputStream()));
				for (;;) {
					if (disconnected == false) {
						Socket client = ss.accept();

						BufferedReader inClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
						BufferedWriter outClient = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

						InputStream in = client.getInputStream();
						byte[] buf = new byte[1000];
						int buflen;
						System.out.println("Entering loop");

						for (;;) {
							try {
								buflen = in.read(buf);
								if (buflen != -1) {
									String firstBuffer = new String(buf, 0, buflen);
									int firstSpace = firstBuffer.indexOf(" ");
									String command;

									if (firstSpace == -1) {
										command = firstBuffer;
										firstBuffer = "";
									} else {
										command = firstBuffer.substring(0, firstSpace);
										firstBuffer = firstBuffer.substring(firstSpace + 1);
									}
									System.out.println("command " + command);



									if (command.equals(Protocol.STORE_TOKEN)) { // Client STORE filename filesize ->
																				// Client ACK |> Client file_content ->
																				// Controller STORE_ACK filename
										System.out.println("ENTERED STORE");

										String following[] = firstBuffer.split(" ");
										String filename = following[0];
										int filesize = Integer.parseInt(following[1]);
										outClient.write(Protocol.ACK_TOKEN);
										outClient.flush();

										


										File outputFile = new File(filename);
										FileOutputStream out = new FileOutputStream(outputFile);
										out.write(in.readNBytes(filesize));
										out.flush();
										out.close();

										outController.write(Protocol.STORE_ACK_TOKEN + " " + filename);


									} else

									// if (command.equals("LOAD_DATA")) { // Client LOAD_DATA filename ->
									// file_content
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

									// if (command.equals("REMOVE")) { // Controller REMOVE filename -> Controller
									// REMOVE_ACK filename
									// int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
									// String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
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

									if (command.equals(Protocol.LIST_TOKEN)) { // Controller LIST -> Controller
																				// file_list
										String[] fileList = folder.list();
										String listToSend = String.join(" ", fileList);
										outClient.write(Protocol.LIST_TOKEN + " " + listToSend);
										outClient.flush();
										// outController.close();
									} else
										System.out.println("unrecognised command");

									// if (command.equals("REBALANCE")) { // Controller REBALANCE files_to_send
									// files_to_remove ->
									// // Controller file_list
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
									// System.out.println("unrecognised command");
								}
							} catch (Exception e) {
								System.out.println("error " + e);
							}
						}
					}
				}
			} catch (Exception e) {
				System.out.println("error " + e);
			}
		}).start();
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