import java.io.*;
import java.net.*;  

public class Controller {
    int cport;
    int R; // Replication factor - number of Dstores
    int timeout;
    int rebalance_period;



    public Controller(int cport, int R, int timeout, int rebalance_period){
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
    }

}