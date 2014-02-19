import java.io.*;
import java.util.*;

class testXMLParser {

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

    private static HashSet<HashSet<String>> getTraduction(String s) {

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

    public static void main (String args[]) throws Exception {

        String file = args[0];
        BufferedReader br = new BufferedReader(new FileReader(file));
        String l = br.readLine();
        while (l != null) {
            java.util.StringTokenizer st = new java.util.StringTokenizer(l, "\t");
            String globalSchemaElement = st.nextToken();
            String localSchemaElement = st.nextToken();
            HashSet<HashSet<String>> traduction = getTraduction(localSchemaElement);
            System.out.println("the traduction of "+localSchemaElement+" is: "+traduction);
            l = br.readLine();
        }
    }
/*
    public static void main (String args[]) throws Exception {

        String apiURL = args[0];

        java.io.InputStream stream = null;
        java.net.URL url = new java.net.URL(apiURL+"?format=xml");
        java.net.URLConnection connection = url.openConnection();
        stream = connection.getInputStream();
        javax.xml.parsers.DocumentBuilderFactory dbf = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        javax.xml.parsers.DocumentBuilder db = dbf.newDocumentBuilder();
        org.w3c.dom.Document d = db.parse(stream);
        System.out.println("documentElement:\n"+d.getDocumentElement());
        System.out.println("documentType:\n"+d.getDoctype());

        org.w3c.dom.NodeList nl = d.getElementsByTagName("data") ;

        System.out.println("number of data nodes:\n"+nl.getLength());
        org.w3c.dom.Node n = nl.item(0);
        org.w3c.dom.NodeList nl2 = n.getChildNodes();
        System.out.println("number of elements in the first data:\n"+nl2.getLength());
        for (int i = 0; i < nl2.getLength(); i++) {
        org.w3c.dom.Node n2 = nl2.item(i);
        if (n2.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE) {
            continue;
        }
        //System.out.println("getBaseURI:\n"+n2.getBaseURI());
        //System.out.println("getLocalName:\n"+n2.getLocalName());
        //System.out.println("getNamespaceURI:\n"+n2.getNamespaceURI());
        System.out.println("getNodeName:\n"+n2.getNodeName()); 
        //System.out.println("getNodeValue:\n"+n2.getNodeValue()); 
        //System.out.println("getTextContent:\n"+n2.getTextContent());
        org.w3c.dom.Element e = (org.w3c.dom.Element) n2;
        String value = getValue(e, "MEL_1");
        System.out.println("value of MEL_1:\n"+value);        
        value = getValue(e, "geo.name");
        System.out.println("value of geo.name:\n"+value);
        }
    }
*/
}

