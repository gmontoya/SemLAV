import java.io.*;
import java.util.*;
import semLAV.*;

public class verifyRelevantViewsOrder {

    public static void main(String[] args) throws Exception {

        String throughputFile = args[0];
        String relevantViewsFile = args[1];

        ArrayList<String> relevantViews = readRVs(relevantViewsFile);

        BufferedReader br = new BufferedReader(new FileReader(throughputFile));
        String l = br.readLine();
        int i = 1;
        while (l != null) {
            if (l.startsWith("#")) {
                l = br.readLine();
                i++;
                continue;
            }
            HashSet<String> hs = readUsedViews(l);
         
            if (hs != null && !goodUse(hs, relevantViews)) {
                System.out.println("there is a problem in line: "+i);
            }
            l = br.readLine();
            i++;
        } 
    }

    public static HashSet<String> readUsedViews(String l) {

      try {
        HashSet<String> hs = new HashSet<String>();
        String aux = l.substring(l.indexOf("[")+1, l.lastIndexOf("]"));
        StringTokenizer st = new StringTokenizer(aux, ")");
        while (st.hasMoreTokens()) {
            String tmp = st.nextToken();
            if (tmp.startsWith(",")) {
                tmp = tmp.substring(1);
            }
            tmp = tmp + ")";
            hs.add(tmp.trim());
        }
        return hs;
      } catch (IndexOutOfBoundsException e) {
        return null;
      }
    }

    public static ArrayList<String> readRVs(String fileName) throws Exception{

        ArrayList<String> rvs = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String rvl = "";
        String l = br.readLine();
        while (l != null) {
            rvs.add(l.trim());
            l = br.readLine();
        }
        return rvs;
    }

    public static boolean goodUse(HashSet<String> hs, ArrayList<String> relevantViews) {

        int n = hs.size();
        HashSet<String> hsRV = new HashSet<String>();
        int i = 0;
        while (i < relevantViews.size() && i < n) {
            if (hsRV.add(relevantViews.get(i))) {
                i++;
            }
        }
        //System.out.println("hs: "+hs);
        //System.out.println("hsRV: "+hsRV);
        return (hs.equals(hsRV));
    }
}
