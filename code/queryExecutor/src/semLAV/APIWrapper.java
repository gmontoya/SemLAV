package semLAV;

import java.io.*;
import java.util.*;
import java.net.*;
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
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import javax.xml.parsers.*;

import org.w3c.dom.NodeList;

class APIWrapper implements Wrapper {

    private String apiURL;
    private HashSet<String> call;
    private String prefix;
    private String field;
    private HashMap<String, HashSet<HashSet<String>>> map;
    private HashMap<String, String> translation;
    private String resultFile;

    /*
     * 'p' corresponds to the instantiation of view defined by 'q' used.
     * 'constants' establishes the relation between constants in 'p' and
     * real URIs and literal used in the global schema.
     * 'result' corresponds to the file where the results are to be stored.
     * 'mappingsFile' contains the way we can obtain the global schema URIs
     * and literals using the api fields and values.
     * 'prefix' corresponds to the prefix to be given to the subjects of the
     * view. 'field' indicates which field is the center of the star-shaped 
     * query that defines this view.
     */
    public APIWrapper(String apiURL, Predicate p, Query q, 
                      HashMap<String, String> constants, String result, 
                      String mappingsFile, String prefix, String field) throws Exception {

        this.apiURL = apiURL;
        this.resultFile = result;
        this.prefix = prefix;
        this.field = field;
        readMappingFile(mappingsFile);
        this.call = buildCall(p, constants, this.map, q);
    }

    private void readMappingFile(String file) throws Exception {

        this.map = new HashMap<String, HashSet<HashSet<String>>>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l != null) {
            java.util.StringTokenizer st = new java.util.StringTokenizer(l, "\t");
            String globalSchemaElement = st.nextToken();
            String localSchemaElement = st.nextToken();
            HashSet<HashSet<String>> traduction = getTraduction(localSchemaElement);
            this.map.put(globalSchemaElement, traduction);
            l = br.readLine();
        }
        br.close();
    }

    private HashSet<HashSet<String>> getTraduction(String s) {

        HashSet<HashSet<String>> trad = new HashSet<HashSet<String>>();
        java.util.StringTokenizer st = new java.util.StringTokenizer(s, ";");
        while (st.hasMoreTokens()) {
            HashSet<String> t = new HashSet<String>();
            String token = st.nextToken();
            java.util.StringTokenizer st2 = new java.util.StringTokenizer(token, ",");
            while (st2.hasMoreTokens()) {
                String token2 = st2.nextToken();
                t.add(token2);
            }
            trad.add(t);
        }
        return trad;
    }

    private HashSet<String> buildCall(Predicate p, HashMap<String, String> constants,
                                    HashMap<String, HashSet<HashSet<String>>> map, Query q) {

        // TODO: build a set of calls
        HashSet<String> calls = new HashSet<String>();
        HashMap<String, String> restrictions = new HashMap<String, String>();
        translation = new HashMap<String, String>();

        Node n = getCenter(q);
        String value = getInstantiation(n, q, p, constants);
        if (value != null) {
            restrictions.put(field, value);
        }
        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();

        for (Triple t : l) {
            Node predicate = t.getPredicate();            
            String traduction = trad(predicate.toString());
            // TODO: consider case predicate variable
            // TODO: consider mappings with AND or OR
            Node object = t.getObject();            
            value = getInstantiation(object, q, p, constants);
            if (value != null) {
                restrictions.put(traduction, value);
            }
            translation.put(traduction, predicate.toString());
        }

        String call = getCall(restrictions);
        calls.add(call);
        return calls;
    }

    private String trad(String n) {

        String t = n;
        HashSet<HashSet<String>> ts = map.get(n);
        if (ts != null) {
            Iterator<HashSet<String>> it1 = ts.iterator();
            if (it1.hasNext()) {
                Iterator<String> it2 = it1.next().iterator();
                if (it2.hasNext()) {
                    t = it2.next();
                }
            }
        }
        return t;
    }

    public static Node getCenter(Query q) {

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        Node center = null;
        for (Triple t : l) {
            Node subject = t.getSubject();
            if (center == null) {
                center = subject;
            } else if (!center.equals(subject)) {
                throw new RuntimeException("Wrong use of API Wrapper..\n"
                    +"Currently the view should be defined as a star shaped query");
            }
        }
        return center;
    }

    public String getTranslation(Node n) {

        String go = n.toString();
        return getTranslation(go);
    }

    public String getTranslation(String go) {

        String aux = trad(go);
        return aux;
    }

    public String getInstantiation(Node n, Query q, Predicate p,
                                          HashMap<String, String> constants) {
        String inst = null;
        if (n.isURI() || n.isLiteral()) {
            inst = getTranslation(n);
        } else {
            List<? extends Node> projectedVars = q.getProjectVars();
            for (int i = 0; i < projectedVars.size(); i++) {
                Node a = projectedVars.get(i);
                if (n.equals(a) && constants.containsKey(p.getArgument(i))) {
                    String value = constants.get(p.getArgument(i));
                    inst = getTranslation(value);
                }
            }
        }
        return inst;
    }

    public static String getCall(HashMap<String, String> restrictions) {

        boolean singleton = true;
        boolean atLeastOne = false;
        String call = "";

        for (String r : restrictions.keySet()) {
            String value = restrictions.get(r);
            call += "{\"" + r + "\":{\"$eq\":" + value.replaceAll(" ","+") + "}},";
            if (!atLeastOne) {
                atLeastOne = true;
            } else {
                singleton = false;
            }
        }
        if (call.length() > 0 && singleton) {
            call = call.substring(1, call.length()-2);
        } else if (call.length() > 0) {
            call = call.substring(0, call.length()-1);
        }
        if (!singleton) {
            call = "\"$and\":[" + call + "]";
        }
        if (atLeastOne) {
            call = "?filter={" + call + "}&";
        } else {
            call = "?";
        }
        call = call+"format=xml";

        return call;
    }

    public void obtainData() throws Exception {

        Model model = ModelFactory.createDefaultModel();
        for (String c : this.call) {
            InputStream stream = null;
            URL url = new URL(this.apiURL+c);
            URLConnection connection = url.openConnection();
            stream = connection.getInputStream();
            //show(stream);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            org.w3c.dom.Document d = db.parse(stream);
            processDocument(d, model);
        }
	    FileOutputStream out = new FileOutputStream(resultFile);
        model.write(out, "N-TRIPLE");
        out.close();
    }
    
    private static void show (InputStream is) throws Exception {

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String l = "";
        while ((l = br.readLine())!=null) {
            System.out.println("(show) "+l);
        }
    }

    public void processDocument(org.w3c.dom.Document d, Model model) {

        org.w3c.dom.Element de = d.getDocumentElement();
        NodeList nl = de.getElementsByTagName("data");
        for (int i = 0; i < nl.getLength(); i++) {
            NodeList nl2 = nl.item(i).getChildNodes();

            for (int j = 0; j < nl2.getLength(); j++) {
                org.w3c.dom.Node current = nl2.item(j);
                if (current.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
                    continue;
                }
                String subject = getValue((org.w3c.dom.Element) current, field);
                if (subject == null || subject.equals("null")) {
                    return;
                }
                Resource res = model.createResource(prefix+subject);
                for (String t : translation.keySet()) {
                    String value = getValue((org.w3c.dom.Element) current, t);
                    if (value == null || value.equals("null")) {
                        continue;
                    }
                    Property p = model.createProperty(translation.get(t));
                    res.addProperty(p, value);
                }
            }
        }
    }

    public static String getValue (org.w3c.dom.Element current, String field) {

        String[] elements = field.split("\\.");

        for (int i = 0; i < elements.length && current != null; i++) {
            org.w3c.dom.NodeList nodes = current.getElementsByTagName(elements[i]);
            org.w3c.dom.Node node = (org.w3c.dom.Node) nodes.item(0);
            current = (org.w3c.dom.Element) node;
        }
        if (current != null) {
            return current.getChildNodes().item(0).getNodeValue();
        }
        return null;
    }

    public void dataReaded() {

        File f = new File(resultFile);
        f.delete();
    }

    /*
        java -cp ".:../lib2/*" semLAV/APIWrapper http://data.nantes.fr/api/publication/22440002800011_CG44_TOU_04815/hotels_STBL/content ~/SemLAV/testWrappers/view1.sparql ~/SemLAV/testWrappers/resultsHotel.n3 ~/SemLAV/testWrappers/mappingsHotel1 http://example.org/hotel/ ID ~/SemLAV/testWrappers/constantsFile
     */
    public static void main(String[] args) throws Exception {

        String apiURL = args[0];
        Predicate p = new Predicate("view1(H,N,C,constant2,W,M)");
        Query query = QueryFactory.read(args[1]);
        String results = args[2];
        String mappings = args[3];
        String prefix = args[4];
        String field = args[5];
        HashMap<String, String> constants = executionMCDSATThreaded.loadConstants(args[6]);
        APIWrapper aw = new APIWrapper(apiURL, p, query, constants, results, mappings, prefix, field);
        System.out.println("mappings:\n"+aw.map);
        System.out.println("call:\n"+aw.call);
        System.out.println("translations:\n"+aw.translation);
        aw.obtainData();
    }
}
