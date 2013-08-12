package semLAV;

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

public class calculateNumberRVs {

    public static void main(String[] args) throws Exception {

        String configFile = args[0];
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
        Timer t = new Timer();
        t.start();
        int n = evaluateQueryThreaded.numberRV2(q, ms, constants, c);
        t.stop();
        System.out.println(sparqlQuery+" has "+n+" relevant views. It took "+TimeUnit.MILLISECONDS.toMillis(t.getTotalTime())+" msecs to calcule relevant views.");
    }
}

