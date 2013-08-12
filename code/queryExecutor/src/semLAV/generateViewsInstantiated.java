package semLAV;

import java.util.*;
import java.io.*;

import com.hp.hpl.jena.util.*;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.Template;

import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.sparql.core.BasicPattern;

public class generateViewsInstantiated {

    public static void main (String[] args) {

        try {
            Properties config = executionMCDSATThreaded.loadConfiguration(System.getProperty("user.dir")
                                                  +"/configData.properties");
            String sparqlViewsFolder = config.getProperty("sparqlViewsFolder");
            String n3ViewsFolder = config.getProperty("n3ViewsFolder");
            final HashMap<String, String> constants 
                               = executionMCDSATThreaded.loadConstants(config.getProperty("constantsFile"));
            String viewsFile = args[0];
            BufferedReader br = new BufferedReader(new FileReader(viewsFile));
            String l = br.readLine();
            Catalog catalog = new Catalog(null, n3ViewsFolder+"/", sparqlViewsFolder+"/", false);
            HashSet<String> views = new HashSet<String>();
            while (l != null) {
                views.add(l);
                l = br.readLine();
            }
            int i = 1;
            for (String v : views) {
                System.out.println("Processing view "+(i++)+" / "+views.size());
                processLine(v, sparqlViewsFolder, n3ViewsFolder, constants, catalog);
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public static void processLine(String l, String sparqlViewsFolder, 
                                   String n3ViewsFolder, HashMap<String, String> constants, Catalog catalog) throws Exception {
        //System.out.println("l: "+l);
        Predicate p = new Predicate(l);
        //System.out.println("p: "+p);
        Query q = catalog.getQuery(p, constants);
        String vn = p.getName();
        Model res = FileManager.get().loadModel(n3ViewsFolder+"/"+vn+".n3");
        String fileName = sparqlViewsFolder + "/" + vn + ".sparql";
        FileInputStream fis = new FileInputStream(fileName);
        String outputName = getOutputName(l);
        HashSet<QuerySolution> solutions = new HashSet<QuerySolution>();
        //System.out.println(q.toString());
        QueryExecution queryExec = QueryExecutionFactory.create(q.toString(),
                                                                    res);
        Model result = ModelFactory.createDefaultModel();
        Query construct = getConstruct(q);

        QueryExecution qem = QueryExecutionFactory.create(construct.toString(), res);
        qem.execConstruct(result);

        OutputStream out = new FileOutputStream(n3ViewsFolder+"/"+outputName+".n3");
        result.write(out, "N-TRIPLE");
        out.close();
        //create(sparqlViewsFolder, outputName, q);
    }

    public static Query getConstruct(Query q) {

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        BasicPattern bgp = BasicPattern.wrap(l);
        Template pat = new Template(bgp);

        Element eg = q.getQueryPattern();
        Query nq = new Query();
        nq.setQueryConstructType();
        nq.setQueryPattern(eg);
        PrefixMapping p = PrefixMapping.Factory.create();
        p.setNsPrefixes(q.getPrefixMapping());
        nq.setPrefixMapping(p);
        nq.setResultVars();
        nq.setConstructTemplate(pat);
        return nq;
    }

    public static void create(String sparqlViewsFolder, String outputName, Query q) throws Exception {
        String newName = sparqlViewsFolder+"/"+outputName+".sparql";
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(newName),
                                                         "UTF-8"));
        output.write(q.toString());
        output.flush();
        output.close();
    }

    public static String getOutputName(String v) {

        Predicate p = new Predicate(v);
        ArrayList<String> args = p.getArguments();
        String name = p.getName();
        for (int i = 0; i < args.size(); i++) {
            String a = args.get(i);
            char c = a.charAt(0);
            if ((Character.isLetter(c) && Character.isLowerCase(c))|| Character.isDigit(c)) {
                name = name + "_" + a;
            } else {
                name = name + "_V";
            }
        }
        
        return name;
    }
}
