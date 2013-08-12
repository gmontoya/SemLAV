package semLAV;

import java.util.*;
import java.io.*;

class obtainRelevantViews {

    public static void main(String[] args) throws Exception {

        String configFile = args[0];
        String fileName = args[1];
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
                               = evaluateQueryThreaded.loadConstants(config.getProperty("constants"), q.getPrefixMapping());
        Catalog c = executionMCDSATThreaded.loadCatalog(config, path, null, sparqlDir, false);
        evaluateQueryThreaded.saveRV2(fileName, q, ms, constants, c);
    }
}
