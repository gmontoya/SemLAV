import java.io.*;
import java.io.File;
import java.io.BufferedWriter;
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

import java.math.BigInteger;

import semLAV.*;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

public class obtainNumberRewritings {

    public static void main(String[] args) throws Exception {

        String configFile = args[0];
        String throughputFile = args[1];
        Properties config = executionMCDSATThreaded.loadConfiguration(configFile);

        String path = config.getProperty("path");
        String sparqlQuery = config.getProperty("querypath");
        String sparqlDir = config.getProperty("mappingssparql");

        ConjunctiveQuery q = new ConjunctiveQuery(sparqlQuery);
        PrefixMapping pf = q.getPrefixMapping();
        ArrayList<ConjunctiveQuery> ms = new ArrayList<ConjunctiveQuery>();
        File dirSparqlViews = new File (path + sparqlDir);
        File[] views = dirSparqlViews.listFiles();
        if (views != null) {
            for (File v : views) {
                if (v.isFile() && !v.isHidden() && v.getName().endsWith(".sparql")) {
                    ConjunctiveQuery vcq = new ConjunctiveQuery(v);
                    ms.add(vcq);
                    pf = pf.withDefaultMappings(vcq.getPrefixMapping());
                }
            }
        }
        HashMap<String, String> constants
                               = executionMCDSATThreaded.loadConstants(config.getProperty("constants"));
        Catalog c = executionMCDSATThreaded.loadCatalog(config, path, null, sparqlDir, false);
        HashMap<Triple,ArrayList<Predicate>> buckets = evaluateQueryThreaded.obtainBuckets(q, ms, constants, c);
        ArrayList<Predicate> usedViews = obtainUsedMappings2.getRVs(throughputFile, 510);
        BigInteger totalNRWs = new BigInteger("1");
        BigInteger coveredNRWs = new BigInteger("1");
        for (Triple t : buckets.keySet()) {
             ArrayList<Predicate> b = buckets.get(t);
             //totalNRWs = totalNRWs.multiply(new BigInteger(b.size()+""));
             long bucketSize = 0;
             long coveredBucket = 0;
             for (Predicate p : b) {
                 int i = obtainOcurrences(obtainView(p, ms, constants, pf), t, constants, pf);
                 //totalNRWs = totalNRWs.multiply(new BigInteger(b.size()+""));
                 bucketSize = bucketSize + i;
                 //System.out.println("considering subgoal "+t+" y view);
                 if (usedViews.contains(p)) {
                     //int i = obtainOcurrences(obtainView(p, ms, constants, pf), t, constants, pf);
                     coveredBucket = coveredBucket + i;
                 }
             }
             coveredNRWs = coveredNRWs.multiply(new BigInteger(coveredBucket+""));
             totalNRWs = totalNRWs.multiply(new BigInteger(bucketSize+""));
        }
        System.out.println("for query: "+sparqlQuery+"\ntotalNRWs: "+totalNRWs+"\ncoveredBucket: "+coveredNRWs);
    }

    public static ConjunctiveQuery obtainView (Predicate p, ArrayList<ConjunctiveQuery> ms, HashMap<String, String> cs, PrefixMapping pf) {

        ConjunctiveQuery v = null;
        for (ConjunctiveQuery m : ms) {
            Predicate q = m.getHead().toPredicate(cs, pf);
            if (p.equals(q)) {
                v = m;
                break;
            }
        }
        return v;
    }

    public static int  obtainOcurrences(ConjunctiveQuery v, Triple t, HashMap<String, String> cs, PrefixMapping ps) {

        ArrayList<Predicate> is = evaluateQueryThreaded.getInstantiations(v, t, cs, ps);
        return is.size();
    }
}

