import java.io.*;
import java.util.*;
import semLAV.Predicate;
import semLAV.IgnoringStream;

public class computeCoverage {

    public static void main (String[] args) throws Exception {

        String throughput = args[0];
        String query = args[1];
        String berlinMappings = args[2];
        String berlinMappingsUsed = args[3];
        String rewriterCommand = args[4];
        String outFile = args[5];
        //int k = Integer.parseInt(args[6]);

        ArrayList<Predicate> usedViews = obtainUsedMappings2.getRVs(throughput, 510);
        //System.out.println("used views: "+usedViews.toString());
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outFile), "UTF-8"));
        int k = usedViews.size();
        //int l = (int) (usedViews.size()/10);
        //for (int k = l; k <= usedViews.size(); k=k+l) {
            ArrayList<Predicate> uVs = new ArrayList<Predicate>(usedViews.subList(0, k));
            //System.out.println("for k="+k+" selected views: "+uVs);
            obtainUsedMappings2.selectUsedViews(uVs, berlinMappings, berlinMappingsUsed);

            Process p = startRewriter(berlinMappingsUsed, query, rewriterCommand);
            InputStream is = p.getInputStream();
            InputStream es = p.getErrorStream();
            Thread terror = new IgnoringStream(es);
            terror.setPriority(Thread.MIN_PRIORITY);
            terror.start();

            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            Double d = 0.0;
            while ((line=br.readLine())!= null) {
                int i = line.indexOf("models=");
                if (i>=0) {
                    d = Double.parseDouble(line.substring(i+7, line.lastIndexOf(":")).trim());
                    break;
                }
            }
            is.close();
            es.close();
            p.destroy();
            output.write(k+"\t"+d+"\n");
            output.flush();
        //}
        output.flush();
        output.close();
        File f = new File(berlinMappingsUsed);
        f.delete();
    }

    public static Process startRewriter(String mappingsFile, String queryFile, String rewriterCommand) throws Exception {
        List<String> l = new ArrayList<String>();
        l.add(rewriterCommand);
        l.add("RW");
        l.add(mappingsFile);
        l.add(queryFile);

        ProcessBuilder pb = new ProcessBuilder(l);
        Process p = pb.start();
        return p;
    }
}
