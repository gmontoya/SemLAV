package semLAV;

import java.util.*;
import java.io.*;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.query.*;

import java.util.concurrent.TimeUnit;

// This version includes views considering the arguments
public class IncludingStreamV2 extends Thread {

    private InputStream[] iss;
    private Model graphUnion;
    private Counter includedViews;
    private Catalog catalog;
    HashMap<String, String> constants;
    Timer wrapperTimer;
    Timer graphCreationTimer;
    Timer executionTimer;
    Timer totalTimer;
    BufferedWriter info2;
    Counter ids;
    HashSet<Predicate> includedViewsSet;
    boolean testing;

    public IncludingStreamV2 (InputStream[] iss, Model gu, Counter iv, Catalog c, 
                              HashMap<String, String> cs, Timer wrapperTimer, 
                              Timer graphCreationTimer, Timer executionTimer, 
                              Timer totalTimer, BufferedWriter info2, Counter ids, HashSet<Predicate> includedViewsSet,
                              boolean testing) {

        this.iss = iss;
        this.graphUnion = gu;
        this.includedViews = iv;
        this.catalog = c;
        this.constants = cs;
        this.wrapperTimer = wrapperTimer;
        this.graphCreationTimer = graphCreationTimer;
        this.executionTimer = executionTimer;
        this.totalTimer = totalTimer;
        this.info2 = info2;
        this.ids = ids;
        this.includedViewsSet = includedViewsSet;
        this.testing = testing;
    }

    public void reset() {

        if (testing) {
        message(this.ids.getValue() + "\t" + this.includedViews.getValue() + "\t" 
                                              + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime()) 
                                              + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                                              + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                                              + "\t" +  TimeUnit.MILLISECONDS.toMillis(totalTimer.getTotalTime())
                                              + "\t" + graphUnion.size());        
        wrapperTimer.start();
        graphCreationTimer.start();
        executionTimer.start();
        includedViews.reset();
        }
    }

    private void message(String s) {
        synchronized(totalTimer) {
            totalTimer.stop();
            try {
                info2.write(s);
                info2.newLine();
                info2.flush();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
            totalTimer.resume();
        }
    }

    public void run () {

    try {
            InputStreamReader[] isrs = new InputStreamReader[iss.length];
            BufferedReader[] brs = new BufferedReader[isrs.length];
            boolean[] finished = new boolean[brs.length];
            for (int i = 0; i < brs.length; i++) {
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
                    brs[i].mark(100);
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
                        if (evaluateQueryThreaded.include(includedViewsSet, view, constants)) {
                            graphUnion.enterCriticalSection(Lock.WRITE);
                            try {
                                wrapperTimer.resume();
                                Model tmp =  catalog.getModel(view, constants);
                                wrapperTimer.stop();
                                graphCreationTimer.resume();
                                graphUnion.add(tmp);
                                graphCreationTimer.stop();
                                includedViews.increase();
                            } catch (java.lang.OutOfMemoryError oome) {
                                brs[i].reset();
                                reset();
                                graphUnion.removeAll();
                                finish = false;
                                includedViewsSet = new HashSet<Predicate>();
                                totalTimer.stop();
                                System.out.println("out of memory error");
                                totalTimer.resume();
                                // Should also reset all the ones that are empty
                                for (int j = 0; j != i && j < brs.length; j++) {
                                    if (finished[j]) {
                                        brs[j].reset();
                                        brs[j].reset();
                                        finished[j] = false;
                                        totalTimer.stop();
                                        System.out.println("reset subgoal "+j);
                                        totalTimer.resume();
                                    }
                                }
                                break;
                            } catch (com.hp.hpl.jena.n3.turtle.TurtleParseException tpe) {
                                brs[i].reset();
                                reset();
                                graphUnion.removeAll();
                                finish = false;
                                includedViewsSet = new HashSet<Predicate>();
                                totalTimer.stop();
                                System.out.println("jena exception");
                                totalTimer.resume();
                                for (int j = 0; j != i && j < brs.length; j++) {
                                    if (finished[j]) {
                                        brs[j].reset();
                                        brs[j].reset();
                                        finished[j] = false;
                                        totalTimer.stop();
                                        System.out.println("reset subgoal "+j);
                                        totalTimer.resume();
                                    }
                                }
                                break;
                            } finally {
                                graphUnion.leaveCriticalSection();
                            }
                        }
                    } else {
                        finished[i] = true;
                    }
                }
                if (this.isInterrupted()) {
                    break;
                }
                Thread.sleep(1);
            }
            System.out.println("finish view inclusion!");
        } catch (InterruptedException ie) {
            System.out.println("View inclusion ended");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
    }
}
