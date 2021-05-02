import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;


public class Controller {
    static int cport;
    static int R; // Replication factor - number of Dstores
    static int timeout;
    static int rebalance_period;

    List<Dstore> Dstore_list; // list of Dstores


    public static String unEscapeString(String s){
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<s.length(); i++)
            switch (s.charAt(i)){
                case '\n': sb.append("\\n"); break;
                case '\t': sb.append("\\t"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\r': sb.append("\\r"); break;
                default: sb.append(s.charAt(i));
            }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        cport=Integer.parseInt(args[0]);
        R=Integer.parseInt(args[1]);
        timeout=Integer.parseInt(args[2]);
        rebalance_period=Integer.parseInt(args[3]);



		try{
			ServerSocket ss = new ServerSocket(cport);
			for(;;){
				try{
					System.out.println("waiting for connection");
					Socket client = ss.accept();
					System.out.println("connected");
					InputStream in = client.getInputStream();
                    BufferedReader inFromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    BufferedWriter outToClient = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
					byte[] buf = new byte[1000];
					int buflen;
					buflen=in.read(buf);
					String firstBuffer=new String(buf,0,buflen);
					int firstSpace=firstBuffer.indexOf(" ");
                    String command; // LIST
                    if(firstSpace==-1) {command = firstBuffer;System.out.println("NQMA SPACE");}
					else command=firstBuffer.substring(0,firstSpace);
//                    command = unEscapeString(command);
					System.out.println("command \""+command+"\" size is " +command.length());

					if(command.equals("STORE")){
                        System.out.println("ENTERED STORE");
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
                    
                    if(command.equals("LOAD")){
                        System.out.println("ENTERED LOAD");
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

                    if(command.equals("REMOVE")){
                        System.out.println("ENTERED REMOVE");
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

     				if(command.equals("LIST")){
                        OutputStream out = client.getOutputStream();
                        System.out.println("ENTERED LIST");
                        String test = "LIST Deeznuts.exe aimbot.txt";
                        out.write(test.getBytes());
                        out.flush();
                        out.close();
						System.out.println("TRIED TO SEND BABY");
					} else System.out.println("unrecognised command");

				} catch(Exception e){
					System.out.println("error "+e);
					}
			}
		}
		catch(Exception e){System.out.println("error "+e);}
		System.out.println();
	}

}