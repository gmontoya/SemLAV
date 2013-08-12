import java.util.*;
import java.io.*;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

// java -cp ".:../lib2/*" generateQueries $SemLAVPATH/expfiles/berlinData/datasets/datasetFiveThousand.nt $SemLAVPATH/expfiles/berlinData/originalSparqlQueries $SemLAVPATH/expfiles/berlinData/originalSparqlQueriesInstantiated $SemLAVPATH/expfiles/berlinData/query.sparql $SemLAVPATH/bsbmtools-0.2/titlewords.txt
class generateQueries {

    public static void main(String args[]) {

        String datasetFile = args[0];
        String baseQueriesFolder = args[1];
        String instantiatedQueriesFolder = args[2];
        String baseQuery = args[3];
        String wordsFile = args[4]; 

        QuerySolution solution = getRandomSolution(datasetFile, baseQuery);
        Random r = new Random();
        int x = r.nextInt(500) + 1; 
        int y = r.nextInt(500) + 1;
        String word = readOneWord(wordsFile);
        
        File f = new File(baseQueriesFolder);
        File[] content = f.listFiles();
        if (content != null) {
            for (File g : content) {
                if (g.isFile() && !g.isHidden()) {
                    makeInstance(g, solution, x, y, word, instantiatedQueriesFolder);
                }
            }
        }
    }

    public static void makeInstance(File g, QuerySolution solution, int x, int y, String word, String outputFolder) {

        String fileContent = readFile(g);
        fileContent = replace(fileContent, "%x%", x+"");
        fileContent = replace(fileContent, "%y%", y+"");
        fileContent = replace(fileContent, "%word1%", word);
        Iterator<String> it = solution.varNames();

        while(it.hasNext()) {
            String v = it.next();
            RDFNode n = solution.get(v);
            String value = n.toString();
            if (n.isURIResource()) {
                value = "<"+value+">";
            } else if (n.isLiteral()) {
                Literal l = n.asLiteral();
                value = "\""+l.getString()+"\"";
                if (l.getLanguage()!= null && l.getLanguage()!= "") {
                    value = value + "@"+l.getLanguage();
                }
                if (l.getDatatype()!= null) {
                    value = value + "^^<"+l.getDatatypeURI()+">";
                }
            }
            fileContent = replace(fileContent, "%"+v+"%", value);
        }

        saveFile(fileContent, g.getName(), outputFolder);
    }

    public static String readFile(File g) {

        String content = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(g));
            String l = br.readLine();
            while (l!=null) {
                content = content + l + "\n";
                l = br.readLine();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Error reading file "+g.getAbsolutePath());
        }
        return content;        
    }

    public static void saveFile(String content, String fileName, String folder) {

        try {
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                      new FileOutputStream(folder+"/"+fileName), "UTF-8"));
            output.write(content);
            output.flush();
            output.close();         
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Error writing file "+folder+"/"+fileName);
        }
    }

    public static String replace (String content, String oldWord, String newWord) {

        return content.replaceAll(oldWord, newWord);
    }

    public static String readOneWord(String fileName) {

        String word = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String l = null;
            l = br.readLine();
            int y = 80;
            Random r = new Random();
            while (l != null) {
                word = l;
                int x = r.nextInt(100);
                if (x > y) {
                    break;
                }
                l = br.readLine();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Error reading file "+fileName);
        }
        return word;
    }

    public static QuerySolution getRandomSolution(String datasetFile, String queryFile) {

        Query q = QueryFactory.read(queryFile);
        Model res = FileManager.get().loadModel(datasetFile);
        QueryExecution queryExec = QueryExecutionFactory.create(q, res);
        QuerySolution binding = null;
        int l = 80;
        Random r = new Random();

        for (ResultSet rs = queryExec.execSelect() ; rs.hasNext() ; ) {
            binding = rs.nextSolution();
            int x = r.nextInt(100);
            if (x > l) {
                break;
            }
        }
        return binding;
    }
}
