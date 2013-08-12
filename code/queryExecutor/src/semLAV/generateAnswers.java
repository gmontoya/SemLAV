package semLAV;

import com.hp.hpl.jena.rdf.model.InfModel;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.reasoner.Reasoner;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.io.File;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import java.io.OutputStreamWriter;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import java.util.HashSet;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.FileReader;

class generateAnswers {

    public static void main(String args[]) throws Exception {

        Properties config = executionMCDSATThreaded.loadConfiguration("configData.properties");
        String queriesFolder = config.getProperty("queriesFolder");
        String sparqlViewsFolder = config.getProperty("sparqlViewsFolder");
        String n3ViewsFolder = config.getProperty("n3ViewsFolderIn");
        String answersFolder = config.getProperty("answersFolder");

        if (answersFolder != null) {
            executionMCDSATThreaded.makeNewDir(answersFolder);
            Model m = ModelFactory.createDefaultModel();
            File f = new File(n3ViewsFolder);
            File[] content = f.listFiles();
            if (content != null) {
                for (File g : content) {
                    if (g.isFile()) {
                        m = loadViewData(g, m);
                    }
                }
            }
            System.out.println("data loaded"+" "+m.size()+" tuples loaded");
            f = new File(queriesFolder);
            content = f.listFiles();
            if (content != null) {
                for (File g : content) {
                    if (g.isFile()) {
                        saveQueryAnswer(m, g, answersFolder);
                    }
                }
            }
        }
    }

    public static Model loadViewData(File g, Model m) {
        String fileName = g.getAbsolutePath();
        m = FileManager.get().readModel(m, fileName);
        return m;
    }

    public static void saveQueryAnswer(Model res, File g, String queryAnswers) {

        try {
            String fileName = g.getAbsolutePath();
            String name = g.getName();
            int i = name.lastIndexOf(".");
            name = name.substring(0, i);

            Query q = QueryFactory.read(fileName);

            HashSet<QuerySolution> solutions = new HashSet<QuerySolution>();

            QueryExecution queryExec = QueryExecutionFactory.create(q, res);

            if (q.isSelectType()) {
              BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(queryAnswers+"/"+name),
                                                           "UTF-8"));
              for (ResultSet rs = queryExec.execSelect() ; rs.hasNext() ; ) {
                QuerySolution binding = rs.nextSolution();
                ArrayList<String> s = new ArrayList<String>();
                for (String var : q.getResultVars()) {
                    RDFNode n = binding.get(var);
                    String val = null;
                    if (n != null) {
                        val = n.toString();
                    }
                    s.add(val);
                }
                output.write(s.toString());
                output.newLine();                
              }
              output.flush();
              output.close();        
            } else if (q.isConstructType()) {
              Model m = queryExec.execConstruct();
              OutputStream out = new FileOutputStream(queryAnswers+"/"+name);
              m.write(out, "N-TRIPLE");
              out.close();
            } else if (q.isDescribeType()) {
              Model m = queryExec.execDescribe();
              OutputStream out = new FileOutputStream(queryAnswers+"/"+name);
              m.write(out, "N-TRIPLE");
              out.close();
            } else if (q.isAskType()) {
              boolean b = queryExec.execAsk();
              BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(queryAnswers+"/"+name), "UTF-8"));
              output.write(Boolean.toString(b));
              output.flush();
              output.close();
            }
        }  catch (Exception e) {
			e.printStackTrace(System.out);
        }
    }
}
