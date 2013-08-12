package semLAV;

import java.io.*;
import java.util.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;

class FileWrapper implements Wrapper {

    private String filePath;
    private Query query;
    private String resultFile;
    private HashMap<String, String> map;
    private String format;
    private String[] jenaFormats = {"TURTLE", "NTRIPLES", "RDFXML", "N3", "RDFJSON"};

    public FileWrapper(String filePath, Query q, String resultFile, 
                       String mappingsFile, String format) throws Exception {

        this.filePath = filePath;
        this.query = q;
        this.resultFile = resultFile;
        this.map = EndpointWrapper.readMap(mappingsFile);
        this.format = format;
    }

    public static boolean belongs(String f, String[] fs) {

        for (int i = 0; i < fs.length; i++) {
            if (fs[i].equals(f)) {
                return true;
            }
        }
        return false;
    }

    public void obtainData() throws Exception {

        if (belongs(format, jenaFormats)) {
            Model m = FileManager.get().loadModel(this.filePath);
            Query construct = EndpointWrapper.obtainQuery(this.query, this.map);
            QueryExecution qe = QueryExecutionFactory.create(construct, m);
            Model r = qe.execConstruct();
            OutputStream out = new FileOutputStream(resultFile);

            r.write(out, "N-TRIPLE");
            out.close();
            qe.close();
        } else {
            throw new RuntimeException("Wrong use of File Wrapper..\n"
                    +"Currently the files should be in a format that Jena can handle");
        }
    }

    public void dataReaded() {
        File f = new File(resultFile);
        f.delete();
    }

    /*
        java -cp ".:../lib2/*" semLAV/FileWrapper /home/gabriela/gun2012/code/expfiles/berlinData/datasets/datasetFiveThousand.nt /home/gabriela/SemLAV/testWrappers/view1_0.sparql /home/gabriela/SemLAV/testWrappers/resultsView1 /home/gabriela/SemLAV/testWrappers/mappingsBerlin1 NTRIPLES
     */
    public static void main (String args[]) throws Exception {

        String filePath = args[0];
        Query query = QueryFactory.read(args[1]);
        String results = args[2];
        String mappingsFile = args[3];
        String format = args[4];
        FileWrapper fw = new FileWrapper(filePath, query, results, mappingsFile, format);
        System.out.println("query:\n"+fw.query);
        fw.obtainData();
    }
}
