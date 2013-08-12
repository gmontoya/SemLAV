package semLAV;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

public class IgnoringStream extends Thread {

    private InputStream is;

    public IgnoringStream (InputStream is) {
        this.is = is;
    } 

    public void run () {

        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line=br.readLine())!= null) {
                //System.out.println("ERROR: "+line);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
