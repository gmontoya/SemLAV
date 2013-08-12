package semLAV;

import com.hp.hpl.jena.query.*;
import org.hamcrest.Matcher;
import java.util.*;
import com.hp.hpl.jena.rdf.model.*;
import java.io.*;
import com.hp.hpl.jena.ontology.*;
import com.hp.hpl.jena.reasoner.*;
import static ch.lambdaj.Lambda.*;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.sparql.mgt.Explain.InfoLevel;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class executionMCDSATThreaded {

    public static void main (String args[]) {

        try {
            Properties config = loadConfiguration(System.getProperty("user.dir")
                                                  +"/configC.properties");
            final String PATH = config.getProperty("path");
            final String N3 = config.getProperty("mappingsn3");
            final String SPARQL = config.getProperty("mappingssparql");
            final String ONTO = config.getProperty("ontology");
            final String JOIN_TYPE = config.getProperty("jointype");
            final String RW = config.getProperty("rewriter");
            final String GT = config.getProperty("groundTruth");
            final String GT_PATH = PATH + GT;
            final String QUERY_RESULTS = config.getProperty("queryresults");
            final String QUERY_RESULTS_PATH = QUERY_RESULTS + JOIN_TYPE + "_" + RW + "/";
            String file = PATH + QUERY_RESULTS_PATH;
            makeNewDir(file);
            final String QUERY_PATH = config.getProperty("querypath");
            Query query = QueryFactory.read(QUERY_PATH);
            int numViews = Integer.parseInt(config.getProperty("numberviews"));
            boolean contactSources = Boolean.parseBoolean(config.getProperty("contactsources"));
            final String conjunctiveQuery = config.getProperty("conjunctiveQuery");
            final String mappings = config.getProperty("mappings");
            final HashMap<String, String> constants 
                               = loadConstants(config.getProperty("constants"));
            
            Catalog catalog = loadCatalog(config, PATH, N3, SPARQL, contactSources);
            Reasoner reasoner = null;
            if (ONTO != null) {
                reasoner = makeReasoner(PATH + ONTO);
            }
            int t = Integer.parseInt(config.getProperty("timeout"));
            if (JOIN_TYPE.equals("GUN")) {
                executeGUNWholeAnswer(PATH, ONTO, QUERY_RESULTS_PATH, query, catalog, 
                        reasoner, GT_PATH, constants, 
                        conjunctiveQuery, mappings, t, numViews, file, RW);
            } else if (JOIN_TYPE.equals("JENA")) {
                executeJENAWholeAnswer(PATH, ONTO, QUERY_RESULTS_PATH, query, catalog, 
                        reasoner, GT_PATH, constants, 
                        conjunctiveQuery, mappings, t, numViews, file, RW);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Catalog loadCatalog (Properties config, final String PATH, 
                                       final String N3, final String SPARQL, 
                                       boolean contactSources) 
                                                    throws java.io.IOException {
        Properties Catalog = new Properties();
        String catalogPath = config.getProperty("catalogpath");
        if (catalogPath != null) {
            try {
                FileInputStream fileinput = new FileInputStream(catalogPath);
                Catalog.load(fileinput);
                fileinput.close();
            } catch (java.io.FileNotFoundException fnfe) {

            }
        }
        Catalog catalog = new Catalog(Catalog, PATH + N3, PATH + SPARQL, contactSources);
        return catalog;
    }

    public static Reasoner makeReasoner (final String fileName) 
                                                    throws java.io.IOException {
        FileInputStream fileinput = new FileInputStream(fileName);
        OntModel ontology = ModelFactory.createOntologyModel(
                              OntModelSpec.OWL_MEM_MICRO_RULE_INF, null);
        ontology.read(fileinput, null, "N3");
        fileinput.close();
        Reasoner reasoner = ReasonerRegistry.getOWLMicroReasoner();
        reasoner = reasoner.bindSchema(ontology);
        return reasoner;
    }

    public static Properties loadConfiguration (String file) throws java.io.IOException {
        Properties ps = new Properties();
        FileInputStream fileinput = new FileInputStream(file);
        ps.load(fileinput);
        fileinput.close();
        return ps;
    }

    public static HashMap<String, String> loadConstants(String file) throws Exception {

        HashMap<String, String> hm = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l!=null) {
            int i = l.indexOf("\t");
            String k = l.substring(0, i);
            String v = l.substring(i+1);
            hm.put(k, v);
            l = br.readLine();
        }
        br.close();
        return hm;
    }

    public static void makeNewDir(String file) {
        
        File f = new File(file);
        deleteDir(f);                
        f.mkdir();
    }

    public static void deleteDir(File f) {
        
        File[] content = f.listFiles();
        if (content != null) {
            for (File g : content) {
                deleteDir(g);
            }
        }
        f.delete();
    }

    public static void executeJENAWholeAnswer(final String PATH, final String ONTO,  
                                             final String QUERY_RESULTS_PATH, Query query, 
                                             Catalog catalog, Reasoner reasoner, 
                                             final String GT_PATH, 
                                             HashMap<String, String> constants, 
                                             String conjunctiveQuery, String mappings, int t, 
                                             int numViews, String dir, String RW) throws Exception {
        String line;
        String codePath = System.getProperty("user.dir");
        codePath = codePath.substring(0, codePath.lastIndexOf("/queryExecutor/src"));
        int number = 0;
        BufferedWriter info = new BufferedWriter(new FileWriter(dir+"/throughput", true));
        info.write("# File Id\tNumber of rewritings considered\tWrapper Time (milliseconds)\tGraph Creation Time (milliseconds)\tExecution Time (milliseconds)\tTotal Time (milliseconds)\tGraph Size (statements)");
        info.newLine();
        info.flush();
        Timer numberTimer = new Timer();
        Timer executionTimer = new Timer();
        Timer wrapperTimer = new Timer();
        Timer graphCreationTimer = new Timer();
        Counter ids = new Counter();
        numberTimer.start();
        Process p = startRewriter(PATH + mappings, conjunctiveQuery, t, codePath, RW);
        InputStream is = p.getInputStream();
        InputStream es = p.getErrorStream();
        Counter executedRewritings = new Counter();

        Thread tinput = new ExecutingRWs(is, catalog, executedRewritings, constants, 
                                         wrapperTimer, graphCreationTimer, 
                                         executionTimer, numberTimer, reasoner, info, dir, ids, t);
        Thread terror = new IgnoringStream(es);
        terror.setPriority(Thread.MIN_PRIORITY);
        tinput.setPriority(Thread.MAX_PRIORITY);
        tinput.start();
        terror.start();

        tinput.join();
        is.close();
        es.close();
        p.destroy();
    }

    public static Process startRewriter(String mappingsFile, String queryFile, 
                                        int t, String codePath, String RW) throws Exception {
        List<String> l = new ArrayList<String>();
        l.add("timeout");
        int s = (int) (t / 1000);
        l.add(s+"s");
        if (RW.equals("gqr")) {
            l.add(codePath + "/GQR/GQR/gqr.sh");
        } else {
            l.add(codePath + "/mcdsat/"+RW+"/"+RW);
            //l.add("-t");
            l.add("RW");
        }
        //l.add("-v");
        l.add(mappingsFile);
        //l.add("-q");
        l.add(queryFile);
        ProcessBuilder pb = new ProcessBuilder(l);
        Process p = pb.start();
        return p;
    }

    public static void executeGUNWholeAnswer(final String PATH, final String ONTO,  
                                             final String QUERY_RESULTS_PATH, Query query, 
                                             Catalog catalog, Reasoner reasoner, 
                                             final String GT_PATH, 
                                             HashMap<String, String> constants, 
                                             String conjunctiveQuery, String mappings, int t, 
                                             int numViews, String dir, String RW) throws Exception {
        String line;
        String codePath = System.getProperty("user.dir");
        codePath = codePath.substring(0, codePath.lastIndexOf("/queryExecutor/src"));
        int number = 0;
        BufferedWriter info = new BufferedWriter(new FileWriter(dir+"/throughput", true));
        info.write("# File Id\tNumber of rewritings considered\tWrapper Time (milliseconds)\tGraph Creation Time (milliseconds)\tExecution Time (milliseconds)\tTotal Time (milliseconds)\tGraph Size (statements)");
        info.newLine();
        info.flush();
        HashSet<String> loadedViews = new HashSet<String>();
        Model graphUnion = ModelFactory.createDefaultModel();
        Timer numberTimer = new Timer();
        Timer executionTimer = new Timer();
        Timer wrapperTimer = new Timer();
        Timer graphCreationTimer = new Timer();
        Counter ids = new Counter();
        numberTimer.start();
        Process p = startRewriter(PATH + mappings, conjunctiveQuery, t, codePath, RW);
        InputStream is = p.getInputStream();
        InputStream es = p.getErrorStream();
        Counter executedRewritings = new Counter();
        Thread tinput = new IncludingStreamRW(is, catalog, graphUnion, loadedViews, 
                                            executedRewritings, numViews, constants, 
                                            wrapperTimer, graphCreationTimer);
        Thread terror = new IgnoringStream(es);
        terror.setPriority(Thread.MIN_PRIORITY);
        tinput.setPriority(Thread.MIN_PRIORITY);
        HashSet<Predicate> includedViewsSet = new HashSet<Predicate>();
        tinput.start();
        terror.start();
        Thread tquery = new QueryingStream(graphUnion, reasoner, query, 
                            executionTimer, numberTimer, 
                            executedRewritings, info, dir, wrapperTimer, 
                            graphCreationTimer, ids, includedViewsSet, t, true, "");
        tquery.setPriority(Thread.MAX_PRIORITY);
        tquery.start();
        int exitValue = p.waitFor();
        tinput.join();
        tquery.interrupt();
        tquery.join();
        is.close();
        es.close();
        if (   (exitValue == 0)
            || (loadedViews.size() == numViews)) {
            info.write("# The answer is complete.");
        } else {
            info.write("# The answer may be incomplete.");
        }
        info.newLine();
        if (exitValue != 0) {
            info.write("# Error generating the rewritings. Code error: "+exitValue);
            info.newLine();
        }
        info.flush();
        info.close();
    }
}
