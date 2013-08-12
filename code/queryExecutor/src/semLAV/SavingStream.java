package semLAV;

import java.io.*;
import java.util.*;

class SavingStream extends Thread {

    private InputStream[] iss;
    String fileName;

    public SavingStream (InputStream[] iss, String fn) {

        this.iss = iss;
        this.fileName = fn;
    }

    public void run () {

        try {
            InputStreamReader[] isrs = new InputStreamReader[iss.length];
            BufferedReader[] brs = new BufferedReader[isrs.length];
            HashSet<Predicate> includedViewsSet = new HashSet<Predicate>();
            boolean[] finished = new boolean[iss.length];
            for (int i = 0; i < iss.length; i++) {
                isrs[i] = new InputStreamReader(iss[i]);
                brs[i] = new BufferedReader(isrs[i]);
                finished[i] = false;
            }
            boolean finish = false;
            Predicate sentinel = new Predicate("end()");

            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(fileName, true),
                                                         "UTF-8"));

            while (!finish) {
                finish = true;
                for (int i = 0; i < brs.length; i++) {
                    String v = null;
                    if (finished[i]) {
                        continue;
                    }
                    if (!brs[i].ready()) {
                        finish = false;
                        continue;
                    }
                    if ((v=brs[i].readLine())!= null) {
                        Predicate view = new Predicate(v);
                        if (view.equals(sentinel)) {
                            finished[i] = true;
                            continue;
                        }
                        finish = false;
                        output.write(view.toString()+"\n");
                        output.flush();
                    }
                }
            }
            output.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
