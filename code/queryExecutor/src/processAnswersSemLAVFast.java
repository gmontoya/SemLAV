import java.util.*;
import java.io.*;

class processAnswersSemLAVFast {

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
        String answer = args[1];
        boolean withPR = Boolean.parseBoolean(args[2]);
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
            output.write("# Precision\tRecall\tTotal number of answers\n");
            l = br.readLine();
            int best = -1;
            long bestSize = -1;
            while (l != null) {
                if (l.startsWith("#")) {
                    l = br.readLine();
                    continue;
                }
                int id = takeID(l);
                File f = new File(folder+"/solution"+id);
                long tempSize = f.length();
                if (tempSize > bestSize) {
                    bestSize = tempSize;
                    best = id;
                }
                l = br.readLine();
            }
            br.close();
            HashSet<ArrayList<String>> current = loadSolution(folder+"/solution"+best);
            long currentSize = current.size();

            if (withPR) {
                //HashSet<ArrayList<String>> current = loadSolution(folder+"/solution"+best);
                HashSet<ArrayList<String>> groundTruth = loadSolution(answer);
                current.retainAll(groundTruth);
                long intersectionSize = current.size();
                double precision = intersectionSize / currentSize;
                double recall = intersectionSize / groundTruth.size();
                output.write(precision+"\t"+recall+"\t"+currentSize);
            } else {
                output.write("?\t?\t"+currentSize);
            }
            br = new BufferedReader(new FileReader(file));
            l = br.readLine();
            while (l != null) {
                if (l.startsWith("#")) {
                    l = br.readLine();
                    continue;
                }
                int id = takeID(l);
                File f = new File(folder+"/solution"+id);
                f.delete();
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
