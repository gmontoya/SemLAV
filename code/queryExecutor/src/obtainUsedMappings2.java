import java.io.*;
import java.util.*;
import semLAV.Predicate;

class obtainUsedMappings2 {

    public static void main (String[] args) throws Exception {

        String throughput = args[0];
        String berlinMappings = args[1];
        String berlinMappingsUsed = args[2];
        int k = Integer.parseInt(args[3]);

        ArrayList<Predicate> usedViews = getRVs(throughput, k);
        System.out.println(usedViews);
        System.out.println(usedViews.size());
        selectUsedViews(usedViews, berlinMappings, berlinMappingsUsed);
    }

    public static void selectUsedViews(ArrayList<Predicate> usedViews, String inFile, String outFile) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(inFile));
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outFile, false), "UTF-8"));
        String l = br.readLine();

        while (l != null) {
            String viewName = l.substring(0, l.indexOf(":")).trim();
            Predicate p = new Predicate(viewName);
            if (usedViews.contains(p)) {
                output.write(l);
                output.newLine();
            }
            l = br.readLine();
        }
        output.flush();
        output.close();
        br.close();
    }

    public static ArrayList<Predicate> getRVs(String fileName, int k) throws Exception {

        ArrayList<Predicate> rvs = new ArrayList<Predicate>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String rvl = "";
        String l = br.readLine();
        while (l != null) {
            if (!l.startsWith("#")) {
                rvl = l;
            }
            l = br.readLine();
        }
        l = rvl.substring(rvl.indexOf("[")+1, rvl.lastIndexOf("]"));
        StringTokenizer st = new StringTokenizer(l, ")");
        int j = 0;
        while (st.hasMoreTokens() && j < k) {
            String tmp = st.nextToken();
            if (tmp.startsWith(",")) {
                tmp = tmp.substring(1);
            }
            tmp = tmp + ")";
            Predicate p = new Predicate(tmp);
            rvs.add(p);
            j++;
        }
        br.close();
        return rvs;
    }
}
