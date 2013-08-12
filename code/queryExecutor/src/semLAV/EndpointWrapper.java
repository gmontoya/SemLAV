package semLAV;

import java.io.*;
import java.util.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.datatypes.BaseDatatype;

class EndpointWrapper implements Wrapper {

    private String endpointAddress;
    private HashMap<String, String> map;
    private Query query;
    private String resultFile;

    /*
     *  Arguments: endpointAddress: address of the endpoint to contact.
     *             q: SPARQL query that defines the view in the global schema.
     *             result: path of the file to store the output
     *             mappingFile: file that indicates for each uri/literal in the
     *                          global schema how to obtain it using the local 
     *                          schema.
     */
    public EndpointWrapper(String endpointAddress, Query q, String result, String mappingFile) throws Exception {
        this.endpointAddress = endpointAddress;
        this.map = readMap(mappingFile);
        this.query = obtainQuery(q, this.map);
        this.resultFile = result;
    }

    protected static HashMap<String, String> readMap(String file) throws Exception {

        HashMap<String, String> hm = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l!=null) {
            int i = l.indexOf("\t");
            String k = l.substring(0, i);
            String v = l.substring(i+1);
            hm.put(k, v);
            l = br.readLine();
        }
        br.close();
        return hm;
    }

    public static Node replace(Node n, HashMap<String, String> map) {

        //System.out.println("Considering.. "+n.toString());
        if (n.isURI()) {
            String go = n.getURI();
            //System.out.println("it corresponds to uri (getURI): "+go);
            //System.out.println("it corresponds to uri (toString): "+go);
            String lo = map.get(go);
            if (lo != null) {                
                return NodeFactory.createURI(lo);
            }
        } else if (n.isLiteral()) {
            String go = n.toString();
            //System.out.println("it corresponds to literal: "+go);
            String aux = map.get(go);
            Node naux = buildNode(aux);
            if (naux != null) {
                return naux;
            }
        }
        return n;
    }

    public static Node buildNode(String s) {

        String lit = getLit(s);
        if (lit != null) {       
            String datatype = getDatatype(s);
            String lang = getLanguage(s);         
            if (datatype == null && lang == null) {
                return NodeFactory.createLiteral(lit);
            } else if (datatype == null) {
                return NodeFactory.createLiteral(lit, lang, false); // or true ??
            } else if (lang == null) {
                BaseDatatype dt = new BaseDatatype(datatype);
                return NodeFactory.createLiteral(lit, dt);
            } else {  // both different of null
                BaseDatatype dt = new BaseDatatype(datatype);
                return NodeFactory.createLiteral(lit, lang, dt);
            }
        }
        return null;
    }

    public static String getLit(String s) {

        if (s == null || s.length() < 2) {
            return null;
        }
        int begin = s.indexOf("\"");
        int end = s.lastIndexOf("\"");
        return s.substring(begin+1, end);
    }

    public static String getDatatype(String s) {

        String res = null;
        int tok0 = s.lastIndexOf("\"");
        int tok = s.lastIndexOf("^^");
        int tok2 = s.lastIndexOf("@");
        if (tok2 < tok) {
            tok2 = s.length();
        }
        if (tok >= 0 && tok > tok0) {
            res = s.substring(tok+2, tok2);
        }
        return res;
    }

    public static String getLanguage(String s) {

        String res = null;
        int tok0 = s.lastIndexOf("\"");
        int tok = s.lastIndexOf("@");
        if (tok >= 0 && tok > tok0) {
            res = s.substring(tok+1);
        }
        return res;
    }

    public String getQuery() {

        return query.toString();
    }

    public static Query obtainQuery(Query q, HashMap<String, String> map) {

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        List<Triple> l2 = new ArrayList<Triple>();
        for (Triple t : l) {
            Node subject = replace(t.getSubject(), map);
            Node predicate = replace(t.getPredicate(), map);
            Node object = replace(t.getObject(), map);
            l2.add(new Triple(subject, predicate, object));
        }

        BasicPattern bgp = BasicPattern.wrap(l2);
        ElementPathBlock epb = new ElementPathBlock(bgp);
        ElementGroup eg = new ElementGroup();
        eg.addElement(epb);

        BasicPattern bgp0 = BasicPattern.wrap(l);

        Query nq = new Query();
        nq.setQueryConstructType();
        //nq.setQuerySelectType();
        //List<Var> vars = q.getProjectVars();
        //for (Var v : vars) {
        //    nq.addResultVar(v);
        //}
        nq.setQueryPattern(eg);

        PrefixMapping p = PrefixMapping.Factory.create();
        p.setNsPrefixes(q.getPrefixMapping());
        nq.setPrefixMapping(p);

        nq.setResultVars();
        nq.setConstructTemplate(new Template(bgp0));

        //PrefixMapping p = PrefixMapping.Factory.create();
        //p.setNsPrefixes(q.getPrefixMapping());
        //nq.setPrefixMapping(p);
        return nq;
    }

    public void obtainData() throws Exception {

        String s = query.toString();
        //System.out.println("query:\n"+s);
        QueryExecution qexec = QueryExecutionFactory.sparqlService(endpointAddress, s);
        Model result = qexec.execConstruct();
        OutputStream out = new FileOutputStream(resultFile);

        result.write(out, "N-TRIPLE");
        out.close();
        qexec.close();
    }

    public void dataReaded() {
        File f = new File(resultFile);
        f.delete();
    }
    
    /*
        java -cp ".:../lib2/*" semLAV/EndpointWrapper http://live.dbpedia.org/sparql ~/SemLAV/testWrappers/queryDBPediaSimple.sparql ~/SemLAV/testWrappers/resultsDBPedia.n3 ~/SemLAV/testWrappers/mappingsDBPedia1.properties

        http://dblp.l3s.de/d2r/sparql
     */
    public static void main (String[] args) throws Exception {

        String endpointAddress = args[0];
        Query query = QueryFactory.read(args[1]);
        String results = args[2];
        String mappings = args[3];
        EndpointWrapper ew = new EndpointWrapper(endpointAddress, query, results, mappings);
        System.out.println(ew.getQuery());
        ew.obtainData();
    }
}
