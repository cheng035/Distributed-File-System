import Controller.Controller;

import java.io.IOException;
import java.net.UnknownHostException;

public class Test_main_app {
    // Push test
    public static void main(String[] args) throws IOException, InterruptedException {

        Controller a = new Controller();
        a.intialize(4445, 0.0);
        a.start();

        Controller b = new Controller();
        b.intialize(1231, 0.0);
        b.start();

        Controller c = new Controller();
        c.intialize(9993, 0.0);
        c.start();

        Controller d = new Controller();
        d.intialize(9994, 0.0);
        d.start();

        Controller e = new Controller();
        e.intialize(9995, 0.0);
        e.start();

        Controller f = new Controller();
        f.intialize(9996, 0.0);
        f.start();

        Controller g = new Controller();
        g.intialize(9997, 0.0);
        g.start();

        Controller h = new Controller();
        h.intialize(9998, 0.0);
        h.start();

        Controller i = new Controller();
        i.intialize(9999, 0.0);
        i.start();

        Controller j = new Controller();
        j.intialize(9992, 0.0);
        j.start();


        /*
        a.switch_mode();
        b.switch_mode();
        c.switch_mode();
*/
    }
}
