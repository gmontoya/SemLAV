package semLAV;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.core.Var;

public class ExecutingRWs extends Thread {

    private InputStream is;
    private Catalog catalog;
    private Set loadedViews;
    private Counter executedRewritings;
    private int numViews;
    private HashMap<String, String> constants;
    private Timer wrapperTimer;
    private Timer graphCreationTimer;
    private Timer timer;
    private Timer executionTimer;
    private Reasoner reasoner;
    private BufferedWriter info;
    private String dir;
    private Counter ids;
    private int id;
    private long graphSize;
    private int timeout;

    public ExecutingRWs(InputStream is, Catalog catalog, Counter executedRewritings, 
                        HashMap<String, String> cs, Timer wrapperTimer, 
                        Timer graphCreationTimer, Timer executionTimer, 
                        Timer t, Reasoner r, 
                        BufferedWriter info, String dir, Counter ids, int timeout) {
        this.is = is;
        this.catalog = catalog;
        this.executedRewritings = executedRewritings;
        this.constants = cs;
        this.wrapperTimer = wrapperTimer;
        this.graphCreationTimer = graphCreationTimer;
        this.executionTimer = executionTimer;
        this.timer = t;
        this.reasoner = r;
        this.info = info;
        this.dir = dir;
        this.ids = ids;
        this.timeout = timeout;
    }

    public static Query getJenaQuery(Rewrite rew, Catalog catalog, 
                                     HashMap<String, String> constants) {

        ArrayList<Predicate> goals = rew.getGoals();
        PrefixMapping p = PrefixMapping.Factory.create();
        Query result = new Query();
        ElementGroup eg = new ElementGroup();
        for (int i = 0; i < goals.size(); i++) {
            Predicate sg = goals.get(i);
            Query sq = catalog.getQuery(sg, constants);
            p.setNsPrefixes(sq.getPrefixMapping());

            Element e = sq.getQueryPattern();
            Node n = Node.createURI("http://"+sg.getName()+i);
            ElementNamedGraph eng = new ElementNamedGraph(n, e);
            eg.addElement(eng);
        }
        result.setQueryPattern(eg);
        result.setQuerySelectType();
        for (String v : rew.getHead().getArguments()) {
            result.addResultVar("?" + v);
        }
        return result;
    }

    public static Dataset getDataset(ArrayList<Predicate> goals, Catalog catalog, 
                                     BufferedWriter info, Timer graphCreationTimer, 
                                     Timer wrapperTimer, Reasoner reasoner, 
                                     HashMap<String, String> constants) 
                                                                     throws Exception{

        Dataset d = DatasetFactory.createMem();
        for (int i = 0; i < goals.size(); i++) {
            Predicate p = goals.get(i);
            String n = p.getName();
            graphCreationTimer.stop();
            wrapperTimer.resume();
            Model m = catalog.getModel(p, constants);
            wrapperTimer.stop();
            graphCreationTimer.resume();
            if (reasoner != null) {
                m = ModelFactory.createInfModel (reasoner, m);
            }
            d.addNamedModel("http://"+n+i, m);
        }
        return d;
    }

    public void run () {

        try {
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    message(id + "\t" + executedRewritings.getValue()
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                            + "\t" +  TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                            + "\t" + graphSize);
                    try {
                        info.flush();
                        info.close();
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                        System.exit(1);
                    }
                }
            });
            
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            boolean queried = false;
            while ((line=br.readLine())!= null) {
                QueryExecution result = null;
                if (line.startsWith("[")) {
                    continue;
                }

                Rewrite rew = new Rewrite(line);
                this.executedRewritings.increase();
                ArrayList<Predicate> goals = rew.getGoals();
                graphCreationTimer.resume();
                Query rewSparql = getJenaQuery(rew, catalog, constants);

                graphSize = 0;
                try {
                    Dataset d = getDataset(goals, catalog, info, graphCreationTimer, wrapperTimer, reasoner, constants);
                    graphCreationTimer.stop();
                    timer.stop();
                    Iterator<String> i = d.listNames();
                    while (i.hasNext()) {
                        graphSize = graphSize + d.getNamedModel(i.next()).size();
                    }



                    timer.resume();
                    executionTimer.resume();
                    result = QueryExecutionFactory.create(rewSparql.toString(), d);

                } catch (java.lang.OutOfMemoryError oome) {
                    System.err.println("Error during execution: out of memory.");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                id = this.ids.getValue();
                this.ids.increase();
                String fileName = this.dir + "/solution"+id;

                executionTimer.stop();
                timer.stop();
                BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                        new FileOutputStream(fileName), "UTF-8"));
                executionTimer.resume();
                timer.resume();
                for (ResultSet rs = result.execSelect(); rs.hasNext();) {
                    QuerySolution binding = rs.next();
                    ArrayList<String> r = new ArrayList<String>();
                    for (String var : rew.head.getArguments()) {
                        String val = binding.get(var).toString();
                        r.add(val);
                    }
                    executionTimer.stop();
                    timer.stop();
                    if (!queried) {
                        message(id + "\t" + this.executedRewritings.getValue() 
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                            + "\t" + graphSize
                            + "\t1");
                        queried = true;
                    }
                    output.write(r.toString());
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
                message(id + "\t" + this.executedRewritings.getValue() 
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime()) 
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                            + "\t" +  TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                            + "\t" + graphSize);
                timer.resume();  
                graphCreationTimer.resume();           
                if (TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime()) >= timeout) {
                    break;
                }
            }
            graphCreationTimer.stop();
            timer.stop();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
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
}
