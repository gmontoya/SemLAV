package semLAV;
import com.hp.hpl.jena.rdf.model.InfModel;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
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
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.datatypes.RDFDatatype;

class generateViews {

    public static void main(String args[]) throws Exception {

        Properties config = executionMCDSATThreaded.loadConfiguration("configData.properties");
        String dataSetFile = config.getProperty("dataSetFile");
        String indexesFolder = config.getProperty("indexesFolder");
        String queriesFolder = config.getProperty("queriesFolder");
        int factor = Integer.parseInt(config.getProperty("factor"));
        String sparqlViewsFolder = config.getProperty("sparqlViewsFolder");
        String n3ViewsFolder = config.getProperty("n3ViewsFolder");
        String n3OntologyFile = config.getProperty("n3OntologyFile");
        String ontologyFile = config.getProperty("ontologyFile");

        if (sparqlViewsFolder != null && n3ViewsFolder != null) {
            executionMCDSATThreaded.makeNewDir(sparqlViewsFolder);
            executionMCDSATThreaded.makeNewDir(n3ViewsFolder);
            //System.out.println("ready to load model");
            Model res = FileManager.get().loadModel(dataSetFile);
            //System.out.println("model loaded");
            Reasoner reasoner = null;
            if (ontologyFile != null && n3OntologyFile != null) {
                createOntologyFile(res, n3OntologyFile, ontologyFile);
                reasoner = executionMCDSATThreaded.makeReasoner(n3OntologyFile);
            }
            File f = new File(indexesFolder);
            File[] content = f.listFiles();
            if (content != null) {
                for (File g : content) {
                    if (g.isFile()) {
                        extractIndexData(res, g, n3ViewsFolder, reasoner,
                                         sparqlViewsFolder, factor);
                    }
                }
            }
        }
    }

    public static Node convert (RDFNode n) {

        String s = n.toString();
        if (n.isLiteral()) {
            Literal l = n.asLiteral();
            s = l.getLexicalForm();
            String lg = l.getLanguage();
            RDFDatatype dt = l.getDatatype();
            return Node.createLiteral(s, lg, dt);
        } else if (n.isURIResource()) {
            return Node.createURI(s);
        } else if (n.isAnon()) {
            return Node.createAnon(new AnonId(s));
        } 
        return null;
    }

    public static Expr getConstantValue(RDFNode n) {

        Node node = convert(n);
        if (node == null) {
            return null;
        }
        return new NodeValueNode(node);
    }    

    public static void createOntologyFile(Model m, String n3FileName, 
                                          String fileName) throws Exception {

        Property p = m.createProperty("http://www.w3.org/2000/01/rdf-schema#","subClassOf");
        Resource s = null; 
        RDFNode o = null; 
        Selector ss = new SimpleSelector(s, p, o);       
        Model sc = m.query(ss);
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(fileName), "UTF-8"));
        StmtIterator it = sc.listStatements();
        while (it.hasNext()) {
            Statement st = it.nextStatement();
            Node r = convert(st.getSubject());
            Node n = convert(st.getObject());
            String c = "type(X, "+generateMappings.transform(r)+") <= type(X, "
                        +generateMappings.transform(n)+")";
            output.write(c);
            output.newLine();
        }
        output.flush();
        output.close();
        p = m.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#","type");
        ss = new SimpleSelector(s, p, o);       
        Model t = m.query(ss);
        Model r = t.union(sc);
        OutputStream out = new FileOutputStream(n3FileName);
        r.write(out, "N-TRIPLE");
    }

    public static void extractIndexData(Model res, File g, String viewsDataset, 
                                        Reasoner reasoner, String viewsFolder, int f) {

        try {
            String fileName = g.getAbsolutePath();
            String name = g.getName();
            int i = name.lastIndexOf(".");
            name = name.substring(0, i);

            Query q = QueryFactory.read(fileName);

            HashSet<QuerySolution> solutions = new HashSet<QuerySolution>();

            if (reasoner != null) {
                res = ModelFactory.createInfModel (reasoner, res);
            }
            //System.out.println("the inference model is ready");
            QueryExecution queryExec = QueryExecutionFactory.create(q.toString(), 
                                                                    res);
            //System.out.println("the query execution is ready");
            Model result[] = new Model[f];

            for (i = 0; i < f; i++) {
                result[i] = ModelFactory.createDefaultModel();
            }
            //System.out.println("the empty models are ready");
            ResultSet rs = queryExec.execSelect();
            //System.out.println("the select is ready");
            i = 0;

            while (rs.hasNext()) {
                QuerySolution solution = rs.nextSolution();
                Query construct = getConstruct(q, solution);
                //System.out.println("construct: "+construct);
                QueryExecution qem = QueryExecutionFactory.create(construct.toString(), res);
                qem.execConstruct(result[i]);
                i = (i+1)%f;
            }
            int j = Integer.parseInt(name.substring(4));
            for (i = 0; i < f; i++) {
                int k = (j-1)*f+(i+1);
                OutputStream out = new FileOutputStream(viewsDataset+"/view"+k+".n3");
                result[i].write(out, "N-TRIPLE");
                out.close();
                copy(fileName, viewsFolder, k);
            }
        }  catch (Exception e) {
			e.printStackTrace(System.out);
        }
    }

    public static Query getConstruct(Query q, QuerySolution solution) {

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        BasicPattern bgp = BasicPattern.wrap(l);
        Template pat = new Template(bgp);

        //Element eg = q.getQueryPattern();
        Query nq = new Query();
        nq.setQueryConstructType();

        ElementGroup eg = (ElementGroup) (q.getQueryPattern());
        List<Element> els = eg.getElements();
        ElementGroup eg2 = new ElementGroup();
        for (Element e : els) {
            eg2.addElement(e);
        }

        Iterator<String> vars = solution.varNames();

        while (vars.hasNext()) {
            String a = vars.next();
            RDFNode n = solution.get(a);
            Expr e = new E_Equals(new NodeValueNode(Node.createVariable(a)), 
                                  getConstantValue(n));
            eg2.addElement(new ElementFilter(e));
        }

        nq.setQueryPattern(eg2);
        PrefixMapping p = PrefixMapping.Factory.create();
        p.setNsPrefixes(q.getPrefixMapping());
        nq.setPrefixMapping(p);
        nq.setResultVars();
        nq.setConstructTemplate(pat);
        return nq;
    }

    public static void copy(String name, String viewsFolder, int k) throws Exception {
        int i = name.lastIndexOf(".");
        int j = name.lastIndexOf("/");
        String oldName = name.substring(j+1, i);
        //int p = oldName.indexOf("w");
        //String end = "_"+oldName.substring(p+1)+"_"+k;
        //String newName = viewsFolder+"/"+oldName+"_"+k+name.substring(i);
        String newName = viewsFolder+"/view"+k+name.substring(i);
        BufferedReader br = new BufferedReader(new FileReader(name));
        String l = br.readLine();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(newName), 
                                                         "UTF-8"));
        while (l != null) {
            //String m = processLine(l, end);
            output.write(l);
            output.newLine();
            l = br.readLine();
        }
        output.flush();
        output.close(); 
        br.close();
    }

    public static String processLine(String l, String end) {

        StringTokenizer st = new StringTokenizer(l, " \t\n\r\f:{}.", true);
        String ns = "";

        while (st.hasMoreTokens()) {
            String t = st.nextToken();
            char c = t.charAt(0);
            if ((c == '?') || (c == '$')) {
                t = t + end;
            }
            ns = ns + t;
        }
        return ns;
    }
}
