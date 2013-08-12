package semLAV;

import java.util.HashSet;
import java.util.HashMap;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.BufferedWriter;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import java.util.concurrent.TimeUnit;

public class CounterStream extends Thread {

    private InputStream[] iss;
    Counter counter;

    public CounterStream (InputStream[] iss, Counter c) {

        this.iss = iss;
        this.counter = c;
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
            while (!finish) {
                finish = true;
                for (int i = 0; i < brs.length; i++) {
                    String v = null;
                    if (finished[i]) {
                        continue;
                    }
                    if (!brs[i].ready()) {
                        //System.out.println("not ready");
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
                        if (includedViewsSet.add(view)) {
                            counter.increase();
                        }
                    }
                }
            }
            //System.out.println("finish view inclusion!");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
