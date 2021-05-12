public class DstoreCreator {
    
    public static void main(String[] args) {
        

        new Thread(() -> {
            Dstore d1 = new Dstore(10101, Integer.parseInt(args[0]), 100, "test1");
        }).start();
        // new Thread(() -> {
        //     Dstore d2 = new Dstore(10102, Integer.parseInt(args[0]), 100, "test2");
        // }).start();
        // new Thread(() -> {
        //     Dstore d3 = new Dstore(10103, Integer.parseInt(args[0]), 100, "test3");
        // }).start();
        // new Thread(() -> {
        //     Dstore d4 = new Dstore(10104, Integer.parseInt(args[0]), 100, "test4");
        // }).start();
        // new Thread(() -> {
        //     Dstore d5 = new Dstore(10105, Integer.parseInt(args[0]), 100, "test5");
        // }).start();
        // new Thread(() -> {
        //     Dstore d6 = new Dstore(10106, Integer.parseInt(args[0]), 100, "test6");
        // }).start();
        // new Thread(() -> {
        //     Dstore d7 = new Dstore(10107, Integer.parseInt(args[0]), 100, "test7" );
        // }).start();
        // new Thread(() -> {
        //     Dstore d8 = new Dstore(10108, Integer.parseInt(args[0]), 100, "test8" );
        // }).start();
        // new Thread(() -> {
        //     Dstore d9 = new Dstore(10109, Integer.parseInt(args[0]), 100, "test9");
        // }).start();
        // new Thread(() -> {
        //     Dstore d10 = new Dstore(10110, Integer.parseInt(args[0]), 100, "test10");
        // }).start();

    }

}