
import java.io.*;
import java.util.*;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;

import semLAV.myVisitor123;
// java -cp ".:../lib2/*" obtainConstants $SemLAVPATH/expfiles/berlinData/originalSparqlQueriesInstantiated $SemLAVPATH/expfiles/berlinData/FiveThousand/300views/viewsSparql $SemLAVPATH/expfiles/berlinData/FiveThousand/300views/constantsFile
class obtainConstants {

    static int id = 0;

    public static void main(String args[]) throws Exception {

        String queriesFolder = args[0];
        String viewsFolder = args[1];
        String constantsFile = args[2];

        File f = new File(viewsFolder);
        File[] content = f.listFiles();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(constantsFile), 
                                                         "UTF-8"));
        HashMap<String, String> vars = new HashMap<String, String>();
        if (content != null) {
            for (File g : content) {
                if (g.isFile() && !g.isHidden()) {
                    saveConstants(g, vars);
                }
            }
        }

        f = new File(queriesFolder);
        content = f.listFiles();
        if (content != null) {
            for (File g : content) {
                if (g.isFile() && !g.isHidden()) {
                    saveConstants(g, vars);
                }
            }
        }
        saveVars(vars, constantsFile);
    }

    public static void saveConstants(File f, HashMap<String, String> vars) {

        String fileName = f.getAbsolutePath();
        System.out.println("considering "+fileName);
        Query q = QueryFactory.read(fileName);
        PrefixMapping p = q.getPrefixMapping();
        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        for (Triple t : l) {
            Node n = t.getSubject();
            saveNode(n, vars, p);
            n = t.getPredicate();
            saveNode(n, vars, p);
            n = t.getObject();
            saveNode(n, vars, p);
        }
    }

    public static void saveNode(Node n, HashMap<String, String> vars, PrefixMapping p) {
        if (!n.isVariable()) {
            String a = null;
            if (n.isURI()) {
                a = p.expandPrefix(n.toString());
            } else {
                a = n.toString();
            }
            if (!vars.containsValue(a)) {
                vars.put("constant"+id, a);
                id++;
            }
        }
    }

    public static void saveVars(HashMap<String, String> vars, String file) {

        try {
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                    new FileOutputStream(file), "UTF-8"));
            for (String k : vars.keySet()) {
                output.write(k+"\t"+vars.get(k));
                output.newLine();
            }
            output.flush();
            output.close();
        }  catch (Exception e) {
			e.printStackTrace(System.out);
        }
    }

}
