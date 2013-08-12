package semLAV;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PipedOutputStream;
import java.io.PipedInputStream;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.List;
import java.io.File;
import java.io.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.shared.PrefixMapping;

public class evaluateQueryThreaded {
	
    public static void main(String[] args) throws Exception {
		
        String configFile = args[0];
        Properties config = executionMCDSATThreaded.loadConfiguration(configFile);
        String path = config.getProperty("path");
        String sparqlQuery = config.getProperty("querypath");
        String output = config.getProperty("output");
        String sparqlDir = config.getProperty("mappingssparql");
        int timeout = Integer.parseInt(config.getProperty("timeout"));

        ConjunctiveQuery q = new ConjunctiveQuery(sparqlQuery);
        ArrayList<ConjunctiveQuery> ms = new ArrayList<ConjunctiveQuery>();
        File dirSparqlViews = new File (path + sparqlDir);
        File[] views = dirSparqlViews.listFiles();
        if (views != null) {
            for (File v : views) {
                if (v.isFile() && !v.isHidden() && v.getName().endsWith(".sparql")) {
                    ms.add(new ConjunctiveQuery(v));
                }
            }
        }

        boolean testing = Boolean.parseBoolean(config.getProperty("testing"));
        String queryResults = config.getProperty("queryResults");		
        String queryResultsPath = queryResults + "/";
        String file = path + queryResultsPath;
        if (testing) {
            executionMCDSATThreaded.makeNewDir(file);		
        }
        String groundTruthFile = config.getProperty("groundTruth");
        String groundTruthPath = path+groundTruthFile;		
        String n3Dir = config.getProperty("mappingsn3");

        boolean contactSources = Boolean.parseBoolean(config.getProperty("contactsources"));
        boolean sorted = Boolean.parseBoolean(config.getProperty("sorted"));
        HashMap<String, String> constants
                               = loadConstants(config.getProperty("constants"), q.getPrefixMapping());
        Catalog catalog = executionMCDSATThreaded.loadCatalog(config, path, n3Dir, sparqlDir, contactSources);
        execute(sparqlQuery, path, queryResultsPath, n3Dir, 
                groundTruthPath, q, ms, constants, catalog, timeout, sorted, testing, output);
    }

    public static HashMap<String, String> loadConstants(String file, PrefixMapping p) throws Exception {

        HashMap<String, String> hm = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l!=null) {
            java.util.StringTokenizer st = new java.util.StringTokenizer(l);
            String key = st.nextToken();
            String value = p.expandPrefix(st.nextToken());
            hm.put(key, value);
            l = br.readLine();
        }
        br.close();
        return hm;
    }

    // UNSORTED BUCKETS OF RELEVANT VIEWS
    public static int numberRV(ConjunctiveQuery cq,
                               ArrayList<ConjunctiveQuery> ms, HashMap<String, String>
                               constants) throws Exception {

        int n = cq.getNumberSubgoals();
        Counter counter = new Counter();

        final PipedOutputStream[] outArray = new PipedOutputStream[n];
        final PipedInputStream[] inArray = new PipedInputStream[n];
        for (int i = 0; i < n; i++) {

            outArray[i] = new PipedOutputStream();
            inArray[i] = new PipedInputStream(outArray[i]);
        }
        Thread tRelViews = new RelevantViewsSelector2(outArray, cq, ms, constants);
        tRelViews.start();

        Thread tcounter = new CounterStream(inArray, counter);
        tcounter.start();
        tRelViews.join();
        tcounter.join();
        for (int i = 0; i < n; i++) {
            inArray[i].close();
        }
        return counter.getValue();
    }

    // SORTED BUCKETS OF RELEVANT VIEWS
    public static int numberRV2(ConjunctiveQuery cq,
                               ArrayList<ConjunctiveQuery> ms, HashMap<String, String>
                               constants, Catalog c) throws Exception {

        HashMap<Triple,ArrayList<Predicate>> buckets = viewSelection(cq, ms, constants, c);
        HashSet<Predicate> relViews = new HashSet<Predicate>();
        for (Triple t : buckets.keySet()) {
             relViews.addAll(buckets.get(t));
        }
        return relViews.size();
    }

    public static HashMap<Triple,ArrayList<Predicate>> obtainBuckets(ConjunctiveQuery cq,
                               ArrayList<ConjunctiveQuery> ms, HashMap<String, String>
                               constants, Catalog c) throws Exception {

        HashMap<Triple,ArrayList<Predicate>> buckets = viewSelection(cq, ms, constants, c);
        return buckets;
    }

    public static void saveRV2(String fileName,
                               ConjunctiveQuery cq,
                                ArrayList<ConjunctiveQuery> ms, HashMap<String, String>
                                constants, Catalog c) throws Exception {

        HashMap<Triple,ArrayList<Predicate>> buckets = viewSelection2(cq, ms, constants, c);
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(fileName, true),
                                                         "UTF-8"));
        Set<Triple> ks = buckets.keySet();
        HashSet<Predicate> includedViewsSet =  new HashSet<Predicate>();
        int n = ks.size();
        Triple[] keys  = new Triple[n];
        int[] current = new int[n];
        int i = 0;
        for (Triple k : ks) {
            current[i] = 0;
            keys[i] = k;
            i++;
        }
            boolean[] finished = new boolean[n];
            for (i = 0; i < keys.length; i++) {
                finished[i] = false;
            }
            boolean finish = false;
            while (!finish) {
                finish = true;
                for (i = 0; i < n; i++) {
                    String v = null;
                    if (finished[i]) {
                        continue;
                    }
                    Triple k = keys[i];
                    ArrayList<Predicate> rvs = buckets.get(k);
                    if (current[i] < rvs.size()) {
                        Predicate view = rvs.get(current[i]);
                        current[i] = current[i] + 1;
                        finish = false;
                        if (include(includedViewsSet, view, constants)) {
                            output.write(view.toString()+"\n");
                            output.flush();
                            //includedViews.increase();
                        }
                    } else {
                        finished[i] = true;
                    }
                }
            }
    }

    public static void saveRV(String fileName,
                               ConjunctiveQuery cq,
                                ArrayList<ConjunctiveQuery> ms, HashMap<String, String>
                                constants) throws Exception {

        int n = cq.getNumberSubgoals();

        final PipedOutputStream[] outArray = new PipedOutputStream[n];
        final PipedInputStream[] inArray = new PipedInputStream[n];
        for (int i = 0; i < n; i++) {

            outArray[i] = new PipedOutputStream();
            inArray[i] = new PipedInputStream(outArray[i]);
        }
        Thread tRelViews = new RelevantViewsSelector2(outArray, cq, ms, constants);
        tRelViews.start();

        Thread saving = new SavingStream(inArray, fileName);
        saving.start();
        tRelViews.join();
        saving.join();
        for (int i = 0; i < n; i++) {
            inArray[i].close();
        }
    }

    private static void execute(String sparqlQuery,
                                String PATH, String QUERY_RESULTS_PATH, String n3Dir, 
                                String GT_PATH, ConjunctiveQuery cq, 
                                ArrayList<ConjunctiveQuery> ms, HashMap<String, String> 
                                constants, Catalog catalog, int timeout, boolean sorted, boolean testing,
                                String output) throws Exception {
    
        Model graphUnion = ModelFactory.createDefaultModel();
        String dir = PATH + QUERY_RESULTS_PATH +"NOTHING";
        if (testing) {
            executionMCDSATThreaded.makeNewDir(dir);
        }
        Query q = QueryFactory.read(sparqlQuery);
        int n = cq.getNumberSubgoals();
        BufferedWriter info = null;
        BufferedWriter info2 = null;
        if (testing) {
            info = new BufferedWriter(new FileWriter(dir + "/throughput", true));
            info.write("# File Id\tNumber of views considered\tWrapper Time (milliseconds)\tGraph Creation Time (milliseconds)\tExecution Time (milliseconds)\tTotal Time (milliseconds)\tGraph Size (statements)");
            info.newLine();
            info.flush();
            info2 = new BufferedWriter(new FileWriter(dir + "/newRVi", true));
            info2.write("# File Id\tNumber of views considered\tWrapper Time (milliseconds)\tGraph Creation Time (milliseconds)\tExecution Time (milliseconds)\tTotal Time (milliseconds)\tGraph Size (statements)");
            info2.newLine();
            info2.flush();
        }
        Timer numberTimer = new Timer();
        Timer wrapperTimer = new Timer();
        Timer graphCreationTimer = new Timer();
        Timer executionTimer = new Timer();
        Counter ids = new Counter();
        Thread tRelViews = null;
        Thread tinput = null;
        PipedInputStream[] inArray = null;
        Counter includedViews = new Counter();
        HashSet<Predicate> includedViewsSet = new HashSet<Predicate>();
        if (sorted) {
            numberTimer.start();
            HashMap<Triple,ArrayList<Predicate>> buckets = viewSelection2(cq, ms, constants, catalog);
            tinput = new IncludingStreamV3(buckets, graphUnion, includedViews, catalog, constants, wrapperTimer, graphCreationTimer, executionTimer, numberTimer, info2, ids, includedViewsSet, testing);
            tinput.start();
        } else {
            final PipedOutputStream[] outArray = new PipedOutputStream[n];
            inArray = new PipedInputStream[n];
            for (int i = 0; i < n; i++) {
                outArray[i] = new PipedOutputStream();
                inArray[i] = new PipedInputStream(outArray[i]);
            }
            numberTimer.start();
            tRelViews = new RelevantViewsSelector2(outArray, cq, ms, constants);
            tRelViews.start();
            tinput = new IncludingStreamV2(inArray, graphUnion, includedViews, catalog, constants, wrapperTimer, graphCreationTimer, executionTimer, numberTimer, info2, ids, includedViewsSet, testing);
            tinput.start();
        }
        Thread tquery = new QueryingStream(graphUnion, null, q, 
                            executionTimer, numberTimer, includedViews, info, dir, wrapperTimer, graphCreationTimer, ids, includedViewsSet, timeout, testing, output);
        tquery.start();

        while (tinput.isAlive() || tquery.isAlive()) {
            if (!tinput.isAlive()) {
                tquery.interrupt();
                if (!sorted) {
                    tRelViews.interrupt();
                }
            }
            if (!tquery.isAlive()) {
                tinput.interrupt();
                if (!sorted) {
                    tRelViews.interrupt();
                }
            }
        }

        if (!sorted) {
            for (int i = 0; i < n; i++) {
                inArray[i].close();
            }
        }
        if (testing) {
            info2.flush();
            info2.close();
        }
        numberTimer.stop();
    }

    public static void replace (List<Node> list, Node prevArg, Node newArg) {
    
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(prevArg)) {
                
                list.set(i, newArg);
            }
        }
    }
   
    public static ArrayList<Predicate> getInstantiations (ConjunctiveQuery v, Triple g, HashMap<String, String> cs, PrefixMapping ps) {

        ArrayList<Predicate> is = new ArrayList<Predicate>();
        List<Triple> body = v.getBody();
        for (Triple t : body) {

            //if (t.getPredicate().equals(g.getPredicate())) {
                boolean okay = true;
                Node predV = t.getPredicate();
                Node predQ = g.getPredicate();
                if (!v.getHead().getArguments().contains(predV) && predV.isVariable()) {
                    okay = false;
                    continue;
                } else if (!predQ.isVariable()) {
                    if (!predV.isVariable() && !predV.equals(predQ)) {
                        okay = false;
                        continue;
                    }
                }
                ArrayList<Node> mapping = (ArrayList<Node>) v.getHead().getArguments();
                Node subV = t.getSubject();
                Node subQ = g.getSubject();
                if (!v.getHead().getArguments().contains(subV) && subV.isVariable()) {
                    okay = false;
                } else if (!subQ.isVariable()) {
                    if (!subV.isVariable() && !subV.equals(subQ)) {
                        okay = false;
                    }
                    replace(mapping, subV, subQ);
                }
                Node objV = t.getObject();
                Node objQ = g.getObject();

                if (!v.getHead().getArguments().contains(objV) && objV.isVariable()) {
                    okay = false;
                } else if (!objQ.isVariable()) {

                    if (!objV.isVariable() && !objV.equals(objQ)) {
                        okay = false;
                    }
                    replace(mapping, objV, objQ);
                }
                if (okay) {
                    Predicate vi = v.getHead().replace(mapping).toPredicate(cs, ps);
                    vi.setViewSize(v.getNumberSubgoals());
                    is.add(vi);
                }
            //}
        }
        return is;
    }
 
    public static ArrayList<Node> getMapping (ConjunctiveQuery v, Triple g) { 

        List<Triple> body = v.getBody();
        for (Triple t : body) { 

            //if (t.getPredicate().equals(g.getPredicate())) {
                boolean okay = true;
                Node predV = t.getPredicate();
                Node predQ = g.getPredicate();
                if (!v.getHead().getArguments().contains(predV) && predV.isVariable()) {
                    okay = false;
                    continue;
                } else if (!predQ.isVariable()) {
                    if (!predV.isVariable() && !predV.equals(predQ)) {
                        okay = false;
                        continue;
                    }
                }
                ArrayList<Node> mapping = (ArrayList<Node>) v.getHead().getArguments();
                Node subV = t.getSubject();
                Node subQ = g.getSubject();
                if (!v.getHead().getArguments().contains(subV) && subV.isVariable()) {
                    okay = false;
                } else if (!subQ.isVariable()) {
                    if (!subV.isVariable() && !subV.equals(subQ)) {
                        okay = false;
                    }
                    replace(mapping, subV, subQ);
                }
                Node objV = t.getObject();
                Node objQ = g.getObject();

                if (!v.getHead().getArguments().contains(objV) && objV.isVariable()) {
                    okay = false;
                } else if (!objQ.isVariable()) {

                    if (!objV.isVariable() && !objV.equals(objQ)) {
                        okay = false;
                    }
                    replace(mapping, objV, objQ);
                }
                if (okay) {
                    return mapping;
                }
            //}
        }
        return null;
    }

    private static HashMap<Triple,ArrayList<Predicate>> viewSelection2(ConjunctiveQuery q,
                                     ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs, Catalog c) {

        HashMap<Triple,ArrayList<Predicate>> buckets = buildBuckets(q, ms, cs);
        List<Triple> body = q.getBody();
        for (int i = 0; i < buckets.size(); i++) {
            Triple p = body.get(i);
            sortBucket(buckets, p, cs, q.getPrefixMapping(), ms, c);
        }
        return buckets;
    }

    private static HashMap<Triple,ArrayList<Predicate>> viewSelection3(ConjunctiveQuery q,
                                     ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs) {

        HashMap<Triple,ArrayList<Predicate>> buckets = buildBuckets(q, ms, cs);
        List<Triple> body = q.getBody();
        for (int i = 0; i < buckets.size(); i++) {
            Triple p = body.get(i);
            shuffleBucket(buckets.get(p));
        }
        return buckets;
    }

    private static void shuffleBucket(ArrayList<Predicate> b) {

        int n = 2 * b.size();

        for (int i = 0; i < n; i++) {
            int j = (int) (Math.random() * b.size());
            int k = (int) (Math.random() * b.size());
            Predicate pj = b.get(j);
            Predicate pk = b.get(k);
            b.set(j, pk);
            b.set(k, pj);
        }
    } 

    private static HashMap<Triple,ArrayList<Predicate>> buildBuckets(ConjunctiveQuery q,
                                     ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs) {
        HashMap<Triple,ArrayList<Predicate>> buckets = new HashMap<Triple, ArrayList<Predicate>>();

            int n = q.getNumberSubgoals();

            for (Triple p : q.getBody()) {
                ArrayList<Predicate> b = new ArrayList<Predicate>();
                buckets.put(p, b);
            }
            int selectedViews = 0;
            List<Triple> body = q.getBody();
            Thread[] bConstructor = new Thread[body.size()];
            for (int i = 0; i < body.size(); i++) {
                Triple p = body.get(i);
                bConstructor[i] = new bucketConstructor(p, buckets.get(p), ms, cs, q.getPrefixMapping());
                bConstructor[i].start();
            }
            for (int i = 0; i < body.size(); i++) {
                try {
                     bConstructor[i].join();
                } catch (InterruptedException ie) {
                     System.out.println("This thread received an interrupted exception");
                     ie.printStackTrace();
                }
            }
        return buckets;
    }

    private static class bucketConstructor extends Thread {

        Triple p;
        ArrayList<Predicate> b;
        ArrayList<ConjunctiveQuery> ms;
        PrefixMapping ps;
        HashMap<String, String> cs;

        public bucketConstructor(Triple p, ArrayList<Predicate> b, ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs, PrefixMapping ps) {
            this.p = p;
            this.b = b;
            this.ms = ms;
            this.cs = cs;
            this.ps = ps;
        }

        public void run() {

            for (int j = 0; j < ms.size(); j++) {
                ConjunctiveQuery v = ms.get(j);
                ArrayList<Predicate> is = getInstantiations(v, p, cs, ps); //getMapping(v, p);
                //if (mapping != null) { // is it possible to cover this subgoal using this view?
                    //Predicate vi = v.getHead().replace(mapping).toPredicate(cs, ps);
                    //vi.setViewSize(v.getNumberSubgoals());
                    //if (vi.getName().equals("view443")) {
                    //System.out.println("considering view: "+vi+"\nb: "+b);
                    //}
                for (Predicate vi : is) {
                    Predicate toInclude = include(b, vi, cs);
                }
                //}   
            }
        }
    }

    private static HashMap<Triple,ArrayList<Predicate>> viewSelection(ConjunctiveQuery q, 
                                     ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs, Catalog c) {
        HashMap<Triple,ArrayList<Predicate>> buckets = new HashMap<Triple, ArrayList<Predicate>>();
    
            int n = q.getNumberSubgoals();     
            for (Triple p : q.getBody()) {
                ArrayList<Predicate> b = new ArrayList<Predicate>();
                buckets.put(p, b);
            }
            int selectedViews = 0;
            List<Triple> body = q.getBody();
            for (int i = 0; i < body.size(); i++) { 
                    Triple p = body.get(i);
                    ArrayList<Predicate> b = buckets.get(p);
                    for (int j = 0; j < ms.size(); j++) {
                        ConjunctiveQuery v = ms.get(j);
                        ArrayList<Node> mapping = getMapping(v, p);
                        if (mapping != null) { // is it possible to cover this subgoal using this view?
                            Predicate vi = v.getHead().replace(mapping).toPredicate(cs, q.getPrefixMapping());
                            Predicate toInclude = include(b, vi, cs);
                        }
                    }
            }
            for (int i = 0; i < n; i++) {
                Triple p = body.get(i);
                sortBucket(buckets, p, cs, q.getPrefixMapping(), ms, c);
            }
        return buckets;
    }

    private static void mergeSort(int[] keys, ArrayList<Predicate> values, int l, int r) {

        if (l + 1 < r) {
            int m = (l + r) / 2;
            mergeSort(keys, values, l, m);
            mergeSort(keys, values, m, r);
            merge(keys, values, l, m, r);
        }
    }

    private static void merge(int[] keys, ArrayList<Predicate> values, int l, int m, int r) {

        ArrayList<Predicate> tempValues = new ArrayList<Predicate>();
        int[] tempKeys = new int[r-l];
        int i1 = l;
        int i2 = m;
        int i3 = 0;
        while (i1 < m && i2 < r) {
            if ((keys[i1] > keys[i2]) || ((keys[i1] == keys[i2]) && (values.get(i1).getViewSize() >= values.get(i2).getViewSize()))) {
                tempKeys[i3] = keys[i1];
                tempValues.add(values.get(i1));
                i1++;
                i3++;
            } else {
                tempKeys[i3] = keys[i2];
                tempValues.add(values.get(i2));
                i2++;
                i3++;
            }
        }
        while (i1 < m) {
            tempKeys[i3] = keys[i1];
            tempValues.add(values.get(i1));
            i1++;
            i3++;
        }
        while (i2 < r) {
            tempKeys[i3] = keys[i2];
            tempValues.add(values.get(i2));
            i2++;
            i3++;
        }
        for (int i = 0; i < tempKeys.length; i++) {
            keys[l+i] = tempKeys[i];
            values.set(l+i, tempValues.get(i));
        }
    }

    public static ConjunctiveQuery obtainView (Predicate p, ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs, PrefixMapping pf, Catalog catalog) {

        ConjunctiveQuery v = null;
        for (ConjunctiveQuery m : ms) {
            Predicate q = m.getHead().toPredicate(cs, pf);
            if (p.getName().equals(q.getName())) {
                // maybe we should more precise, and return the view with the corresponding instantiation
                //v = m;
                Query view = catalog.getQuery(p, cs);
                v = new ConjunctiveQuery(view, p.getName());
                break;
            }
        }
        return v;
    }

    public static int  obtainOcurrences(ConjunctiveQuery v, Triple t, HashMap<String, String> cs, PrefixMapping ps) {

        ArrayList<Predicate> is = getInstantiations(v, t, cs, ps);
        return is.size();
    }

    private static void sortBucket(HashMap<Triple,ArrayList<Predicate>> buckets,
                                   Triple p, HashMap<String, String> constants, 
                                   PrefixMapping pf, ArrayList<ConjunctiveQuery> ms,
                                   Catalog catalog) {

        ArrayList<Predicate> elements = buckets.get(p);
        int[] occurrences = new int[elements.size()];
        for (int i = 0; i < occurrences.length; i++) {
            int n = 0;
            Predicate cv = elements.get(i);
            Set<Triple> keys = buckets.keySet();
            for (Triple sg : keys) {
                ArrayList<Predicate> relViews = buckets.get(sg);
                if (relViews.contains(cv)) {
                    //System.out.println(cv);
                    int j = obtainOcurrences(obtainView(cv, ms, constants, pf, catalog), p, constants, pf);
                    n = n + j;
                }
            }
            occurrences[i] = n;
        }
        mergeSort(occurrences, elements, 0, occurrences.length);
    }

    private static boolean ready(int[] array, int n) {

        boolean r = true;
        for (int i = 0; i < array.length && r; i++) {
            r = array[i] >= n;
        }
        return r;
    }

    // This version of RelevantViewsSelector considers arguments in
    // the covering of views
    private static class RelevantViewsSelector2 extends Thread {

        OutputStream[] oss;
        ConjunctiveQuery q;
        ArrayList<ConjunctiveQuery> ms;
        HashMap<String, String> cs;

        public RelevantViewsSelector2(OutputStream[] oss, ConjunctiveQuery q, 
                                     ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs) {
            this.oss = oss;
            this.q = q;
            this.ms = ms;
            this.cs = cs;
        }

        public void run () {
          try {     
            int n = oss.length;     
            OutputStreamWriter[] osws = new OutputStreamWriter[n];
            BufferedWriter[] bws = new BufferedWriter[n];
            for (int i = 0; i < n; i++) {
                osws[i] = new OutputStreamWriter(oss[i]);
                bws[i] = new BufferedWriter(osws[i]);
            }
            int[] currentMapping = new int[n];
            for (int i = 0; i < n; i++) {
                currentMapping[i] = 0;
            }
            HashMap<Triple,ArrayList<Head>> buckets = new HashMap<Triple, ArrayList<Head>>();
            for (Triple t : q.getBody()) {
                ArrayList<Head> b = new ArrayList<Head>();
                buckets.put(t, b);
            }
            int selectedViews = 0;

            while (!ready(currentMapping, ms.size())) {
                List<Triple> body = q.getBody();
                for (int i = 0; i < body.size(); i++) { 
                    Triple t = body.get(i);
                    ArrayList<Head> b = buckets.get(t);
                    int ncm = currentMapping[i];
                    for (int j = currentMapping[i]; j < ms.size(); j++) {
                        ConjunctiveQuery v = ms.get(j);
                        ncm = j + 1;
                        ArrayList<Node> mapping = getMapping(v, t);
                        if (mapping != null) { // is it possible to cover this subgoal using this view?
                            Head vi = v.getHead().replace(mapping);
                            //Head toInclude = include(b, vi, cs);
                            //if ( toInclude != null ) {
                                Predicate p = vi.toPredicate(cs, q.getPrefixMapping());
                                selectedViews++;
                                if (this.isInterrupted()) {
                                    for (int l = 0; l < n; l++) { 
                                        bws[l].close();
                                        osws[l].close();
                                        oss[l].close();
                                    }
                                    return;
                                }
                                bws[i].write(p+"\n");
                                //break;
                            //}
                        }
                    }
                    currentMapping[i] = ncm;
                }
            }
            for (int i = 0; i < n; i++) {
                Predicate vi = new Predicate("end()");
                bws[i].write(vi+"\n");
                bws[i].flush();
                bws[i].close();
                osws[i].close();
                oss[i].close();
            }

          } catch (IOException ioe) {
            ioe.printStackTrace();
          }
        }
    }

    private static boolean weaker(Node argA, Node argB, HashMap<String, String> cs) {
        return !argA.isVariable() && (argB.isVariable() || !argA.equals(argB));
    }

    private static boolean weaker(String argA, String argB, HashMap<String, String> cs) {
        return cs.containsKey(argA) && (!cs.containsKey(argB) || !argA.equals(argB));
    }

    protected static boolean include(HashSet<Predicate> res, Predicate v, HashMap<String, String> cs) {
    
    	int vsize = v.getArguments().size();
    	ArrayList<String> argsV = v.getArguments();
        HashSet<Predicate> toRemove = new HashSet<Predicate>();
        for (Predicate iv : res) {
        	ArrayList<String> argsIV = iv.getArguments();
    		if (iv.getName().equals(v.getName()) && (argsIV.size() == vsize)) {
    		    boolean coversIVV = true; // the included view covers view?
    		    boolean coversVIV = true; // the view covers included view?
    		    for (int j = 0; j < vsize; j++) {
    		    	
    		    	if (weaker(argsV.get(j), argsIV.get(j), cs)) {
    		    		coversVIV = false;
    		    	}
    		    	if (weaker(argsIV.get(j), argsV.get(j), cs)) {
    		    		coversIVV = false;
    		    	}
    		    }
    		    if (coversIVV) {
    		    	return false;
    		    } else if (coversVIV) {
                    toRemove.add(iv);
    		    }
    	    }
    	}
        synchronized(res) {
            for (Predicate p : toRemove) {
                res.remove(p);
            }
            res.add(v);
        }
        return true;
    }

    private static Predicate include(ArrayList<Predicate> res, Predicate v, HashMap<String, String> cs) {

        int vsize = v.getArguments().size();
        ArrayList<String> argsV = v.getArguments();
        boolean included = false;
        for (int i = 0; i < res.size(); i++) {
                Predicate iv = res.get(i);
                ArrayList<String> argsIV = iv.getArguments();
                if (iv.getName().equals(v.getName()) && (iv.getArguments().size() == vsize)) {
                    boolean coversIVV = true; // the included view covers view?
                    boolean coversVIV = true; // the view covers included view?
                    for (int j = 0; j < vsize; j++) {

                        if (weaker(argsV.get(j), argsIV.get(j), cs)) {
                                coversVIV = false;
                        }
                        if (weaker(argsIV.get(j), argsV.get(j), cs)) {
                                coversIVV = false;
                        }
                    }
                    if (coversIVV) {
                        return null;
                    } else if (coversVIV && !included) {
                        res.set(i, v);
                        included = true;
                    } else if (coversVIV && included) {
                        res.remove(i);
                        i--;
                    }
            }
        }
        if (!included) {
                res.add(v);
        }
        return v;
    }
    
    private static Head include(ArrayList<Head> res, Head v, HashMap<String, String> cs) {
    
    	int vsize = v.getArguments().size();
    	List<Node> argsV = v.getArguments();
    	boolean included = false;
        for (int i = 0; i < res.size(); i++) {
    		Head iv = res.get(i);
        	List<Node> argsIV = iv.getArguments();
    		if (iv.getName().equals(v.getName()) && (iv.getArguments().size() == vsize)) {
    		    boolean coversIVV = true; // the included view covers view?
    		    boolean coversVIV = true; // the view covers included view?
    		    for (int j = 0; j < vsize; j++) {
    		    	
    		    	if (weaker(argsV.get(j), argsIV.get(j), cs)) {
    		    		coversVIV = false;
    		    	}
    		    	if (weaker(argsIV.get(j), argsV.get(j), cs)) {
    		    		coversIVV = false;
    		    	}
    		    }
    		    if (coversIVV) {
    		    	return null;
    		    } else if (coversVIV && !included) {
    		    	res.set(i, v);
    		    	included = true;
    		    } else if (coversVIV && included) {
    		    	res.remove(i);
    		    	i--;
    		    }
    	    }
    	}
        if (!included) {
        	res.add(v);
        }
        return v;
    }
}
