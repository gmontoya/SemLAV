package semLAV;
import java.io.*;
import java.util.*;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.query.QueryFactory;

class generateMappings {

    public static void main(String args[]) throws Exception {

        Properties config = executionMCDSATThreaded.loadConfiguration("configData.properties");
        String queriesFolder = config.getProperty("queriesFolder");
        String sparqlViewsFolder = config.getProperty("sparqlViewsFolder");
        String mappingsFile = config.getProperty("mappingsFile");
        String conjunctiveQueriesFolder = config.getProperty("conjunctiveQueriesFolder");
        String constantsFile = config.getProperty("constantsFile");
        int factor = Integer.parseInt(config.getProperty("factor"));
        int n = Integer.parseInt(config.getProperty("n"));
        executionMCDSATThreaded.makeNewDir(conjunctiveQueriesFolder);
        File f = new File(sparqlViewsFolder);
        File[] content = f.listFiles();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(mappingsFile), 
                                                         "UTF-8"));
        HashMap<String, String> vars = new HashMap<String, String>();
        for (int i = 1; i <= n; i++) {
            for (int j = 0; j < factor; j++) {
                int k = (i-1)*factor+(j+1);
                String viewName = sparqlViewsFolder+"/view"+k+/*i+"_"+j+*/".sparql";
                //System.out.println("view name: "+viewName);
                File g = new File(viewName);
                if (g.exists()) {
                    String m = getMapping(g, vars);
                    output.write(m);
                    output.newLine();
                }
            }
        }
        output.flush();
        output.close(); 
        f = new File(queriesFolder);
        content = f.listFiles();
        if (content != null) {
            for (File g : content) {
                if (g.isFile()) {
                    saveConjunctiveQuery(g, conjunctiveQueriesFolder, vars);
                }
            }
        }
        saveVars(vars, constantsFile);
    }

    public static String getMapping(File g, HashMap<String, String> vars) {

        try {
            String fileName = g.getAbsolutePath();
            //System.out.println(fileName);
            String name = g.getName();
            int i = name.lastIndexOf(".");
            name = name.substring(0, i);

            Query q = QueryFactory.read(fileName);

            vars.putAll(getConstants(q));
            //System.out.println("name: "+name);
            //System.out.println("q: "+q);
            return toMapping(q, name);
        }  catch (Exception e) {
			e.printStackTrace(System.out);
            return null;
        }
    }

    public static void saveConjunctiveQuery(File g, String folder, 
                                            HashMap<String, String> vars) {

        try {
            String fileName = g.getAbsolutePath();
            String name = g.getName();
            int i = name.lastIndexOf(".");
            name = name.substring(0, i);

            Query q = QueryFactory.read(fileName);

            vars.putAll(getConstants(q));

            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                    new FileOutputStream(folder+"/"+name), 
                                                         "UTF-8"));
            output.write(toMapping(q, name));
            output.flush();
            output.close();
        }  catch (Exception e) {
			e.printStackTrace(System.out);
        }
    }

    public static String transform(Node n) {

        if (n.isVariable()) {
            return n.getName();
        } else if (n.isURI()) {
            String s = n.getURI();
            int i = s.lastIndexOf("/");
            int j = s.lastIndexOf("#");
            j = Math.max(Math.max(i, j), 0);
            return s.substring(j+1, s.length()).toLowerCase().trim();
        //} else if (s.lastIndexOf(":")>=0) {
        //    return s.substring(s.lastIndexOf(":")+1, s.length()).toLowerCase().trim();
        } else if (n.isLiteral()) {
            return n.getLiteralLexicalForm();
        } else {
            return n.toString().toLowerCase().trim();
        }
    }

    public static String toMapping(Query q, String name) {

        String s = name+"(";

        List<Var> vars = q.getProjectVars();
        for (Var v : vars) {
            s = s + v.getName() + ", ";
        }
        if (vars.size() > 0) {
            s = s.substring(0, s.length()-2);
        }
        s = s + ") :- ";
        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        for (Triple t : l) {
            s = s + toPredicate(t) + ", ";
        }
        if (l.size() > 0) {
            s = s.substring(0, s.length()-2);
        }
        return s;
    }

    public static String toPredicate(Triple t) {

        String s = transform(t.getSubject());
        String p = transform(t.getPredicate());
        String o = transform(t.getObject());
        return p+"("+s+", "+o+")";
    }

    public static HashMap<String, String> getConstants(Query q) {

        HashMap<String, String> hm = new HashMap<String, String>();
        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        for (Triple t : l) {
            Node n = t.getSubject();
            if (n.isLiteral() || n.isURI()) {
                //System.out.println("node: "+n);
                hm.put(transform(n), n.toString());
            }
            n = t.getPredicate();
            if (n.isLiteral() || n.isURI()) {
                //System.out.println("node: "+n);
                hm.put(transform(n), n.toString());
            }
            n = t.getObject();
            if (n.isLiteral() || n.isURI()) {
                //System.out.println("node: "+n);
                hm.put(transform(n), n.toString());
            }
        }
        return hm;
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
