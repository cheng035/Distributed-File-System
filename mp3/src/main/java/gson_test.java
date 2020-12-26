import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class gson_test {
     @Test
    public void test() throws IOException {
         String localFileName = "./pom.xml";
         File file = new File(localFileName);
         FileInputStream fis = new FileInputStream(file);
         BufferedInputStream bis = new BufferedInputStream(fis);

         byte[] contents = new byte[10000];
         int result = bis.read(contents, 0, 1000);
    }
}
