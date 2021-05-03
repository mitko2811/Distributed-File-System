import java.io.*;
import java.net.*;  

public class Dstore {
    static int port;
    static int cport;
    static int timeout;
    static String file_folder;
	private static Dstore dstore;

	public Dstore(int port,int cport,int timeout,String file_folder){
		Dstore.port=port;
		Dstore.cport=cport;
		Dstore.timeout=timeout;
		Dstore.file_folder=file_folder;
		try {
			startDstore();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    public void startDstore() throws IOException {
		File folder = new File(file_folder); // folder to make
		if (!folder.exists())
			if (!folder.mkdir()) throw new RuntimeException("Cannot create new folder in " + folder.getAbsolutePath());
		String path = folder.getAbsolutePath(); // path of folder


		try{
			ServerSocket ss = new ServerSocket(port);
			for(;;){
				try{
					System.out.println("waiting for connection");
					Socket controller = new Socket(InetAddress.getByName("localhost"),cport);
					Socket client = ss.accept();
					System.out.println("connected");
					InputStream in = client.getInputStream();
					byte[] buf = new byte[1000];
					int buflen;
					buflen=in.read(buf);
					String firstBuffer=new String(buf,0,buflen);
					int firstSpace=firstBuffer.indexOf(" ");
					String command=firstBuffer.substring(0,firstSpace);
					System.out.println("command "+command);
                    
					if(command.equals("STORE")){ //Client STORE filename filesize -> Client ACK     |> Client file_content -> Controller STORE_ACK filename
						int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
						String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
						System.out.println("fileName "+fileName);
						File outputFile = new File(fileName);
						FileOutputStream out = new FileOutputStream(outputFile);
						out.write(buf,secondSpace+1,buflen-secondSpace-1);
						while ((buflen=in.read(buf)) != -1){
							System.out.print("*");
							out.write(buf,0,buflen);
						}in.close(); client.close(); out.close();
					} else 
					
					if(command.equals("LOAD_DATA")){ //Client LOAD_DATA filename -> file_content
						int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
						String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
						System.out.println("fileName "+fileName);
						File inputFile = new File(fileName);
						FileInputStream inf = new FileInputStream(inputFile);
						OutputStream out = client.getOutputStream();
						while ((buflen=inf.read(buf)) != -1){
							System.out.print("*");
							out.write(buf,0,buflen);
						}in.close(); inf.close(); client.close(); out.close();
					} else 

					if(command.equals("REMOVE")){ //Controller REMOVE filename -> Controller REMOVE_ACK filename
						int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
						String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
						System.out.println("fileName "+fileName);
						File outputFile = new File(fileName);
						FileOutputStream out = new FileOutputStream(outputFile);
						out.write(buf,secondSpace+1,buflen-secondSpace-1);
						while ((buflen=in.read(buf)) != -1){
							System.out.print("*");
							out.write(buf,0,buflen);
						}in.close(); client.close(); out.close();
					} else 

					if(command.equals("LIST")){ //Controller LIST -> Controller file_list
						int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
						String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
						System.out.println("fileName "+fileName);
						File inputFile = new File(fileName);
						FileInputStream inf = new FileInputStream(inputFile);
						OutputStream out = client.getOutputStream();
						while ((buflen=inf.read(buf)) != -1){
							System.out.print("*");
							out.write(buf,0,buflen);
						}in.close(); inf.close(); client.close(); out.close();
					} else

					if(command.equals("REBALANCE")){ //Controller REBALANCE files_to_send files_to_remove -> Controller file_list
						int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
						String fileName=firstBuffer.substring(firstSpace+1,secondSpace);
						System.out.println("fileName "+fileName);
						File inputFile = new File(fileName);
						FileInputStream inf = new FileInputStream(inputFile);
						OutputStream out = client.getOutputStream();
						while ((buflen=inf.read(buf)) != -1){
							System.out.print("*");
							out.write(buf,0,buflen);
						}in.close(); inf.close(); client.close(); out.close();
					} else System.out.println("unrecognised command");

				} catch(Exception e){
					System.out.println("error "+e);
					}
			}
		}
		catch(Exception e){System.out.println("error "+e);}
		System.out.println();
	}

	public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        file_folder = args[3];
		dstore = new Dstore(port,cport,timeout,file_folder);
	}
}