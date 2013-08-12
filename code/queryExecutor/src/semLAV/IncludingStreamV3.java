package semLAV;

import java.util.*;
import java.io.*;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.graph.Triple;

import java.util.concurrent.TimeUnit;

// This version includes views considering the arguments
public class IncludingStreamV3 extends Thread {

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
    HashMap<Triple,ArrayList<Predicate>> buckets;
    int[] current;
    Triple[] keys;
    boolean testing;

    public IncludingStreamV3 (HashMap<Triple,ArrayList<Predicate>> buckets, Model gu, Counter iv, Catalog c, 
                              HashMap<String, String> cs, Timer wrapperTimer, 
                              Timer graphCreationTimer, Timer executionTimer, 
                              Timer totalTimer, BufferedWriter info2, Counter ids, HashSet<Predicate> includedViewsSet,
                              boolean testing) {

        this.buckets = buckets;
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
        Set<Triple> ks = this.buckets.keySet();
        int n = ks.size();
        this.current = new int[n];
        this.keys = new Triple[n];
        int i = 0;
        for (Triple k : ks) {
            this.keys[i] = k;
            this.current[i] = 0;
            i++;
        }
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
            boolean[] finished = new boolean[keys.length];
            for (int i = 0; i < keys.length; i++) {
                finished[i] = false;
            }
            boolean finish = false;
            while (!finish) {
                finish = true;
                for (int i = 0; i < keys.length; i++) {
                    String v = null;
                    if (finished[i]) {
                        continue;
                    }
                    Triple k = this.keys[i];
                    ArrayList<Predicate> rvs = this.buckets.get(k);
                    if (this.current[i] < rvs.size()) {
                        Predicate view = rvs.get(this.current[i]);
                        this.current[i] = this.current[i] + 1;
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
                                this.current[i] = this.current[i] - 1;
                                reset();
                                graphUnion.removeAll();
                                finish = false;
                                includedViewsSet = new HashSet<Predicate>();
                                totalTimer.stop();
                                System.out.println("out of memory error");
                                totalTimer.resume();
                                // Should also reset all the ones that are empty
                                for (int j = 0; j != i && j < keys.length; j++) {
                                    if (finished[j]) {
                                        this.current[j] = 0; 
                                        finished[j] = false;
                                        totalTimer.stop();
                                        System.out.println("reset subgoal "+j);
                                        totalTimer.resume();
                                    }
                                }
                                break;
                            } catch (com.hp.hpl.jena.n3.turtle.TurtleParseException tpe) {
                                this.current[i] = this.current[i] - 1;
                                reset();
                                graphUnion.removeAll();
                                finish = false;
                                includedViewsSet = new HashSet<Predicate>();
                                totalTimer.stop();
                                System.out.println("jena exception");
                                totalTimer.resume();
                                for (int j = 0; j != i && j < keys.length; j++) {
                                    if (finished[j]) {
                                        this.current[j] = 0; 
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
        } catch (InterruptedException ie) {
            System.out.println("View inclusion ended");
        }
    }
}
