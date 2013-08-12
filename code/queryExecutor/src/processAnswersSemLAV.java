import java.util.*;
import java.io.*;

class processAnswersSemLAV {

    public static HashSet<Integer> readChanges(String fileName) {

        HashSet<Integer> change = new HashSet<Integer>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String l = null;
            l = br.readLine();
            while (l != null) {
                if (l.startsWith("#")) {
                    l = br.readLine();
                    continue;
                }
                int id = takeID(l);
                change.add(id);
                l = br.readLine();
            }
            br.close();
        } catch (IOException ioe) {
            //ioe.printStackTrace();
            //System.err.println("Error reading file "+fileName);
        }
        return change;
    }

    public static HashSet<ArrayList<String>> loadSolution(String file) {

        HashSet<ArrayList<String>> solution = new HashSet<ArrayList<String>>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String l = null;
            l = br.readLine();
            while (l != null) {
                ArrayList<String> m = getMapping(l);
                solution.add(m);
                l = br.readLine();
            }
            br.close();
        } catch (IOException ioe) {
            //ioe.printStackTrace();
            //System.err.println("Error reading file "+file);
        }
        return solution;
    }

    public static ArrayList<String> getMapping (String l) {

        ArrayList<String> m = new ArrayList<String>();
        l = l.substring(1, l.length()-1);
        StringTokenizer st = new StringTokenizer(l, ",");
        while (st.hasMoreTokens()) {
            String t = st.nextToken().trim();
            m.add(t);
        }
        return m;
    }

    public static int takeID(String l) {

        String id = null;

        int pos = l.indexOf("\t");
        if (pos > 0) {
            id = l.substring(0, pos);
        }
        return Integer.parseInt(id);
    }

    public static void main (String[] args) {

        String folder = args[0];
        //boolean isSemLAV = Boolean.parseBoolean(args[1]);
        String file = folder + "/throughput";
        String out = folder + "/answersInfo";
        String rvi = folder + "/newRVi";
        boolean hasAnswers = false;
        int prevId = -1;

        HashSet<ArrayList<String>> previous = new HashSet<ArrayList<String>>();
        HashSet<ArrayList<String>> previousRVi = new HashSet<ArrayList<String>>();
        HashSet<Integer> change = readChanges(rvi);
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String l = null;
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(out), "UTF-8"));
            output.write("# Id\tNumber of answers in this execution\tFrom those how many are new in this RVi\tFrom those how many are new\tHow many answers have been found in this RVi\tHow many answers have been found in total\n");
            l = br.readLine();
            while (l != null) {
                if (l.startsWith("#")) {
                    l = br.readLine();
                    continue;
                }
                int id = takeID(l);
                if (id == prevId) {
                    hasAnswers = true;
                } else {
                    prevId = id;
                }
                if (change.contains(id)) {
                    previousRVi.clear();
                }
                //if (hasAnswers) {
                    HashSet<ArrayList<String>> current = loadSolution(folder+"/solution"+id);
                    int a = current.size();
                    HashSet<ArrayList<String>> tempSet = new HashSet<ArrayList<String>>();
                    tempSet.addAll(current);
                    tempSet.removeAll(previousRVi);
                    int b = tempSet.size();
                    tempSet = new HashSet<ArrayList<String>>();
                    tempSet.addAll(current);
                    tempSet.removeAll(previous);
                    int c = tempSet.size();
                    previous.addAll(current);
                    previousRVi.addAll(current);
                    int d = previousRVi.size();
                    int e = previous.size();
                    output.write(id+"\t"+a+"\t"+b+"\t"+c+"\t"+d+"\t"+e+"\n");
                    output.flush();
                    File f = new File(folder+"/solution"+id);
                    f.delete();
               //}
               l = br.readLine();
           }
           output.close();
           br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.err.println("Error reading file "+file);
        }
    }
}
