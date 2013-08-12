package semLAV;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileManager;
import java.io.*;
import java.util.*;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class Catalog {

	Properties catalog;
	String execDir;
    String sparqlDir;
    boolean contactSource=false;

	public Catalog(){

		this.catalog = new Properties();
		this.execDir = "";
        this.sparqlDir = "";
	}

	public Catalog(Properties cat){

		this.catalog = cat;
	}

    public Catalog(Properties cat, String p, String q){

		this.catalog = cat;
		this.execDir = p;
        this.sparqlDir = q;
	}
	
	public Catalog(Properties cat, String p, String q, boolean c){

		this.catalog = cat;
		this.execDir = p;
        this.sparqlDir = q;
        this.contactSource = c;
	}

	// Be aware that this method doesn't take into account the view arguments.
	// If you want that, use the next one
	public Model getModel(String str) {

        Model res = FileManager.get().loadModel(this.execDir+str+".n3");
        return res;
    }

    public Wrapper createWrapper(Predicate p, HashMap<String, String> constants) throws Exception {

        String str = p.getName();
        String tmpFile = this.execDir+str+".n3";

        String props = this.catalog.getProperty(str);
        String[] args = props.split(",");
        if (args[0].equals("endpoint")) {
            Query q = getQuery(p, constants);
            return new EndpointWrapper(args[1], q, tmpFile, args[2]);
        } else if (args[0].equals("api")) {
            Query q = QueryFactory.read(this.sparqlDir+p.getName()+".sparql");
            return new APIWrapper(args[1], p, q, constants, tmpFile, args[2], args[3], args[4]);
        } else if (args[0].equals("file")) {
            Query q = getQuery(p, constants);
            return new FileWrapper(args[1], q, tmpFile, args[2], args[3]);
        } else {
            throw new RuntimeException("Unknown Wrapper Type");
        }
    }

    public Model getModelWrapper(Predicate p, HashMap<String, String> constants) throws Exception {

        String str = p.getName();
        String tmpFile = this.execDir+str+".n3";
        Wrapper w = createWrapper(p, constants);
        w.obtainData();
        Model res = FileManager.get().loadModel(tmpFile);
        w.dataReaded();
        return res;
    }
	
    // DOES NOT COVER THE CASE WHEN this.contactSource is true
	public Model getModel(Predicate p, HashMap<String, String> constants) {
		
        if (this.contactSource) {
            try {
                return getModelWrapper(p, constants);
            } catch (Exception e) {
                e.printStackTrace(System.out);
                System.out.println("Problems using wrapper for "+p.toString());
                return ModelFactory.createDefaultModel();
            }
        }
		String end = "";
		boolean none = true;
        for (String a : p.getArguments()) {
            if (constants.containsKey(a)) {
                end = end + "_" + a;
                none = false;
                //System.out.println("a: "+a);
            } else {
            	end = end + "_V";
            }
        }
        String name = p.getName();
        //System.out.println("name: "+name);
        //System.out.println("none: "+none);
        if (!none) {
            name = name+end;
        }
        return getModel(name);
	}

    static int n = 0;

    public Query replace(Query q, ArrayList<String> args) {

        for (String a : args) {
            q = replace(q, a, "NewVariable"+n);
            n++;
        }
        return q;
    }

    public static Node replace (Node n, String vold, String vnew) {

        if (n.isVariable() &&  n.getName().equals(vold)) {
            return new Node_Variable(vnew);
        }
        return n;
    }

    public Query replace (Query q, String vOld, String vNew) {

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        List<Triple> l2 = new ArrayList<Triple>();
        for (Triple t : l) {
            l2.add(new Triple(replace(t.getSubject(), vOld, vNew), 
                              replace(t.getPredicate(), vOld, vNew), 
                              replace(t.getObject(), vOld, vNew)));
        }
        BasicPattern bgp = BasicPattern.wrap(l2);
        ElementPathBlock epb = new ElementPathBlock(bgp);
        ElementGroup eg = new ElementGroup();
        eg.addElement(epb);
        Query nq = new Query();
        nq.setQuerySelectType();
        nq.setQueryPattern(eg);
        List<Var> vars = q.getProjectVars();
        for (Var v : vars) {
            if (v.getName().equals(vOld)) {
                nq.addResultVar(vNew);
            } else {
                nq.addResultVar(v);
            }
        }
        PrefixMapping p = PrefixMapping.Factory.create();
        p.setNsPrefixes(q.getPrefixMapping());
        nq.setPrefixMapping(p);
        return nq;
    }

    // Only handles strings and URIs
    public Expr getConstantValue(String c, PrefixMapping p) {

        if (c.startsWith("\"")) {
            return new NodeValueString(c.substring(1, c.length()-1));
        } else {
            String ec = p.expandPrefix(c);
            //if (ec.equals(c)) {
            //    ec = ec.substring(1, ec.length()-1);
            //}
            return new NodeValueNode(Node.createURI(ec));
        }
    }

    public Query constraint(Query query, ArrayList<String> args, 
                            HashMap<String, String> constants) {
        ElementGroup eg = (ElementGroup) (query.getQueryPattern());
        for (String a : args) {
            if (constants.containsKey(a)) {
                Expr e = new E_Equals(new NodeValueNode(Node.createVariable(a)), 
                                      getConstantValue(constants.get(a), query.getPrefixMapping()));
                eg.addElement(new ElementFilter(e));
            }
        }
        query.setQueryPattern(eg);
        return query;
    }

    public Query getQuery(Predicate p, HashMap<String, String> constants) {

		try {
            Query query = QueryFactory.read(this.sparqlDir+p.getName()+".sparql");
            // Replace all variables in query that coincide with
            // variables in arguments of p
            ArrayList<String> arguments = new ArrayList<String>();
            for (String a : p.getArguments()) {
                arguments.add(a);
            }
            query = replace(query, arguments);

            // Replace query arguments
            List<Var> projectedVars = query.getProjectVars();
            for (int k = 0; k < projectedVars.size(); k++) {
                Var v = projectedVars.get(k);
                query = replace(query, v.getName(), arguments.get(k));
            }

            // Include filters that ensures constant arguments
            query = constraint(query, p.getArguments(), constants);
            return query;
        }  catch (Exception e) {
			e.printStackTrace(System.out);
			return null;
        }
    }

    public static HashMap<String, String> loadConstants(String file) throws Exception {

        HashMap<String, String> hm = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l!=null) {
            java.util.StringTokenizer st = new java.util.StringTokenizer(l);
            hm.put(st.nextToken(), st.nextToken());
            l = br.readLine();
        }
        br.close();
        return hm;
    }

    public static void main(String[] args) throws Exception {

        String CODE = args[0];
        Properties Catalog = new Properties();
        FileInputStream fileinput = new FileInputStream(CODE+"/expfiles/catalog");
        Catalog.load(fileinput);
        fileinput.close();
        String PATH = CODE+"/expfiles/";
        String N3 = "berlinData/FiveThousand/300views/viewsN3/";
        String SPARQL = "berlinData/FiveThousand/300views/viewsSparql/";
        boolean contactSources = false;
        Catalog catalog = new Catalog(Catalog, PATH + N3, PATH + SPARQL, contactSources);
        HashMap<String, String> constants = loadConstants(CODE+"/expfiles/berlinData/constantsFile");
        Predicate p = new Predicate("view123(A, deliverydays, de, yes, _E, F)");
        Query q = catalog.getQuery(p, constants);
        System.out.println(q);
    }
}
