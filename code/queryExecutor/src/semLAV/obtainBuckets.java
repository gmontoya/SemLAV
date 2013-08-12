package semLAV;

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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.graph.Triple;

public class obtainBuckets {

    public static void main(String[] args) throws Exception {

        String configFile = args[0];
        String outputFile = args[1];
        Properties config = executionMCDSATThreaded.loadConfiguration(configFile);

        String path = config.getProperty("path");
        String sparqlQuery = config.getProperty("querypath");
        String sparqlDir = config.getProperty("mappingssparql");

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
        HashMap<String, String> constants
                               = executionMCDSATThreaded.loadConstants(config.getProperty("constants"));
        Catalog c = executionMCDSATThreaded.loadCatalog(config, path, null, sparqlDir, false);
        HashMap<Triple,ArrayList<Predicate>> buckets = evaluateQueryThreaded.obtainBuckets(q, ms, constants, c);
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outputFile, false), "UTF-8"));
        for (Triple t : buckets.keySet()) {
             output.write(t+"\t"+(buckets.get(t))+"\n");
        }
        output.flush();
        output.close();
    }
}

