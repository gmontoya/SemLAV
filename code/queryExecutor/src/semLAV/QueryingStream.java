package semLAV;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.rdf.model.RDFNode;

import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.File;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class QueryingStream extends Thread {

    private Model graphUnion;
    private Reasoner reasoner;
    private Query query;
    private Timer timer;
    private Timer executionTimer;
    private Counter counter;
    private BufferedWriter info;
    private int time = 1000;
    private int lastValue = 0;
    private String dir;
    private boolean queried = false;
    private Counter ids;
    Timer wrapperTimer;
    Timer graphCreationTimer;
    HashSet<Predicate> includedViewsSet;
    private int id;
    private int tempValue;
    private int timeout;
    private boolean testing;
    private String output;

    public QueryingStream (Model gu, Reasoner r, Query q, Timer et, Timer t, 
                           Counter c, BufferedWriter i, String dir, Timer wrapperTimer, Timer graphCreationTimer, Counter ids, HashSet<Predicate> includedViewsSet, int timeout, boolean testing, String output) {
        this.graphUnion = gu;
        this.reasoner = r;
        this.query = q;
        this.executionTimer = et;
        this.timer = t;
        this.counter = c;
        this.info = i;
        this.dir = dir;
        this.wrapperTimer = wrapperTimer;
        this.graphCreationTimer = graphCreationTimer;
        this.ids = ids;
        this.includedViewsSet = includedViewsSet;
        this.timeout = timeout;
        this.testing = testing;
        this.output = output;
    }

    private void evaluateQuery() {

        Model m = graphUnion;
        if (reasoner != null) {
            m = ModelFactory.createInfModel (reasoner, m);
        }
        if (this.counter.getValue() != this.lastValue) {
            m.enterCriticalSection(Lock.READ);
            tempValue = this.counter.getValue();
            id = this.ids.getValue();
            this.ids.increase();
            String fileName = "";
            if (testing) {
                fileName = this.dir + "/solution"+id;
            } else {
                fileName = output;
            }
            try {

            executionTimer.resume();
            QueryExecution result = QueryExecutionFactory.create(query, m);

            if (query.isSelectType()) {
                evaluateSelectQuery(result, fileName, id, tempValue, testing);
            } else if (query.isConstructType()) {
                evaluateConstructQuery(result, fileName);
            } else if (query.isDescribeType()) {
                evaluateDescribeQuery(result, fileName);
            } else if (query.isAskType()) {
                evaluateAskQuery(result, fileName);
            }

            executionTimer.stop();
            m.leaveCriticalSection();
            timer.stop();

            if (testing) {
            String includedViewsStr = "";
            synchronized(includedViewsSet) {
                includedViewsStr = includedViewsSet.toString();
            }
            message(id + "\t" + tempValue + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime()) 
                                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                                            + "\t" +  TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                                            + "\t" + graphUnion.size() + "\t" + includedViewsStr);
            }
            timer.resume();
            this.lastValue = tempValue;
            } catch (java.io.IOException ioe) {
                System.err.println("problems writing to "+fileName);
            } catch (java.lang.OutOfMemoryError oome) {
                executionMCDSATThreaded.deleteDir(new File(fileName));
                System.out.println("out of memory while querying");
            }
        }
    }

    private void evaluateConstructQuery(QueryExecution result, String fileName) throws java.io.IOException {
        Model m = result.execConstruct();
        executionTimer.stop();
        timer.stop();
        OutputStream out = new FileOutputStream(fileName);
        m.write(out, "N-TRIPLE");
        out.close();
        executionTimer.stop();
        timer.stop();
    }

    private void evaluateDescribeQuery(QueryExecution result, String fileName) throws java.io.IOException {
        Model m = result.execDescribe();
        executionTimer.stop();
        timer.stop();
        OutputStream out = new FileOutputStream(fileName);
        m.write(out, "N-TRIPLE");
        out.close();
        executionTimer.resume();
        timer.resume();
    }

    private void evaluateAskQuery(QueryExecution result, String fileName) throws java.io.IOException {
        boolean b = result.execAsk();
        executionTimer.stop();
        timer.stop();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(fileName), "UTF-8"));
        output.write(Boolean.toString(b));
        output.flush();
        output.close();
        executionTimer.resume();
        timer.resume();
    }

    private void evaluateSelectQuery(QueryExecution result, String fileName, int id, int tempValue, boolean testing) throws java.io.IOException {

            executionTimer.stop();
            timer.stop();
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                               new FileOutputStream(fileName), "UTF-8"));
            executionTimer.resume();
            timer.resume();
            for (ResultSet rs = result.execSelect(); rs.hasNext();) {
                QuerySolution binding = rs.nextSolution();
                ArrayList<String> s = new ArrayList<String>();
                for (String var : query.getResultVars()) { 
                    RDFNode n = binding.get(var);
                    String val = null;
                    if (n != null) {
                        val = n.toString();
                    }
                    s.add(val);
                }
                executionTimer.stop();
                timer.stop();
                if (!queried && testing) {
                    message(id + "\t" + tempValue + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                            + "\t" + graphUnion.size()
                            + "\t1");
                    time = 10;
                    queried = true;
                }
                output.write(s.toString());
                output.newLine();
                executionTimer.resume();
                timer.resume();
                if (TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime()) >= timeout) {
                    break;
                }
            }
            executionTimer.stop();
            timer.stop();
            output.flush();
            output.close();
            executionTimer.resume();
            timer.resume();
    }

    private void message(String s) {
        synchronized(timer) {
            timer.stop();
            try {
                info.write(s);
                info.newLine();
                info.flush();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
            timer.resume();
        }
    }

    public static long getModelSize(String tmpFile, Model m) {

        try {
            OutputStream out = new FileOutputStream("/tmp/"+tmpFile);
            m.enterCriticalSection(Lock.READ);
            m.write(out, "N-TRIPLE");
            m.leaveCriticalSection();
            File f = new File("/tmp/"+tmpFile);
            long size = f.length();
            f.delete();
            out.close();
            return size;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
        return 0;
    }

    public void run () {

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    evaluateQuery();
                    if (testing) {
                    long size = getModelSize("unionGraph.n3", graphUnion);
                    message("# Graph Size in bytes: "+size);
                    try {
                        info.flush();
                        info.close();
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                        System.exit(1);
                    }
                    }
                }
            });
        try {
            while (true) {
                Thread.sleep(time);
                if (testing) {
                    evaluateQuery();
                }
                if (TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime()) >= timeout || this.isInterrupted()) {
                    break;
                }
            }
        } catch (InterruptedException ie) {
            System.out.println("Query evaluation ended");
        } finally {
        }
    }
}
