import java.util.*;
import java.io.*;

// java processResultsSemLAV /home/gabriela/data/510views 510views output /home/gabriela/data/510views/answersSize /home/gabriela/Documents/results/iswc2013
class processResultsSemLAV {

    public static void main(String[] args) throws Exception {

        String dir = args[0]; // "/home/gabriela/data/510views";
        String setup = args[1]; // "510views";
        String dir2 = args[2]; // "output";
        String answersFile = args[3]; // "/home/gabriela/gun2012/code/expfiles/berlinData/FiveMillions/answersSize";
        String outDir = args[4]; // where the output must go..

        File f = new File(dir);
        File[] content = f.listFiles();
        HashMap<String, String> answerSize = readAnswerSize(answersFile);

        if (content != null) {

            for (File g : content) {
                if (g.isDirectory() && g.getName().startsWith(dir2)) { //"outputDataset10query")) {
                    System.out.println(g.getName());
                    String a = getApproach(g.getName());
                    processFolder(g, outDir, ("data"+setup+a), answerSize, a);
                }
            }
        }
    }

    public static String getApproach(String s) {

        if (s.indexOf("JENA")>= 0) {
            return "JENA";
        } else if (s.indexOf("SemLAV")>=0) {
            return "SemLAV";
        } else {
            return "ERROR";
        }
    }

    public static void processFolder(File g, String outDir, String name, 
                                     HashMap<String, String> answerSize, String approach) throws Exception {

        String dirName = g.getAbsolutePath();
        String tFile = dirName+(approach.equals("SemLAV")?"/NOTHING":"")+"/throughput";
        long[] as = {-1, -1};
        long[][] data = readData(tFile, as);
        File[] content = g.listFiles();
        String y = dirName.substring(dirName.indexOf("q"));
        System.out.println("y: "+y);
	    y = y.substring(5, y.indexOf("_"));
        if (approach.equals("JENA")) {
            processFolder2(g, data, as, outDir, y, approach, name, answerSize);
        }
        if (content != null) {
            for (File h : content) {
                if (h.isDirectory() && !h.isHidden()) {
                    try {
                        processFolder2(h, data, as, outDir, y, approach, name, answerSize);    
                    } catch (Exception e) {
                        System.err.println("Problems with "+g.getAbsolutePath());
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static long[][] readData(String fileName, long[] additional) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String l = br.readLine();
        int k = 0;
        while (l != null) {
            if (!l.startsWith("#")) {
                k++;
            }
            l = br.readLine();
        }
        br.close();
        long[][] data = new long[k][7];
        br = new BufferedReader(new FileReader(fileName));
        k = 0;
        l = br.readLine();
        while (l != null) {
            if (!l.startsWith("#")) {
                //System.out.println(l);
                //System.out.println(l.lastIndexOf(" "));
                //System.out.println(l.lastIndexOf("\t"));
                StringTokenizer st = new StringTokenizer(l);
                //st.nextToken();
                //st.nextToken();
                int j = 0;
                while (st.hasMoreTokens() && j < 7) {
                    data[k][j] = Long.parseLong(st.nextToken());
                    j++;
                }
                if (st.hasMoreTokens()) {
                    String t = st.nextToken().trim();
                    if (t.equals("1")) {
                        additional[0] = data[k][5];
                    }
                }
                //times[k] = l.substring(l.lastIndexOf(" ")+1);
                k++;
            } else {
                if (l.startsWith("# Graph Size")) {
                    long s = Long.parseLong(l.substring(l.indexOf(":")+1).trim());
                    additional[1] = (long) (s/1048576);
                }
            }
            l = br.readLine();            
        }
        br.close();
        return data;
    }

    public static HashMap<String, String> readAnswerSize(String answersFile) throws Exception {

        HashMap<String, String> hm = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(answersFile));
        String l = br.readLine();
        while (l != null) {
            int p = l.lastIndexOf(" ");
            hm.put(l.substring(0, p), l.substring(p+1));
            l = br.readLine();
        }
        br.close();

        return hm;
    }

    public static int[] getAnswerPercentage(String fileName, int answerSize) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String l = br.readLine();
        int k = 0;
        while (l != null) {
            if (!l.startsWith("#")) {
                k++;
            }
            l = br.readLine();
        }
        br.close();
        br = new BufferedReader(new FileReader(fileName));
        l = br.readLine();
        int[] pas = new int[k];
        int i = 0;
        while (l != null) {
            //System.out.println("l: "+l);
            //System.out.println("last tab in pos: "+l.lastIndexOf("\t"));
            //System.out.println("substring: "+l.substring(l.lastIndexOf("\t")+1));
            if (!l.startsWith("#")) {
                long size = Long.parseLong(l.substring(l.lastIndexOf("\t")+1));
                //System.out.println("i: "+i);
                //System.out.println("obtained answers: "+size);
                //System.out.println("total size: "+answerSize);
                long n = size*100;
                //System.out.println("n1: "+n);
                if (n == 0 && answerSize == 0)
                    n = 100;
                else 
                    n = (long) (n/answerSize);
                //System.out.println("n2: "+n);
                pas[i++] = (int) n; //(int) ((size*100)/answerSize);
            }
            l = br.readLine();
        }
        br.close();

        return pas;
    }

    public static int[] getAnswerNumber(String fileName) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String l = br.readLine();
        int k = 0;
        while (l != null) {
            if (!l.startsWith("#")) {
                k++;
            }
            l = br.readLine();
        }
        br.close();
        br = new BufferedReader(new FileReader(fileName));
        l = br.readLine();
        int[] an = new int[k];
        int i = 0;
        while (l != null) {
            //System.out.println("l: "+l);
            //System.out.println("last tab in pos: "+l.lastIndexOf("\t"));
            //System.out.println("substring: "+l.substring(l.lastIndexOf("\t")+1));
            if (!l.startsWith("#")) {
                int size = Integer.parseInt(l.substring(l.lastIndexOf("\t")+1));
                an[i++] = size;
            }
            l = br.readLine();
        }
        br.close();

        return an;
    }

    public static long getMaximalModelSize(long[][] data) {

        if (data.length == 0) {
            return 0;
        }
        long m = data[0][6];
        for (int i = 1; i < data.length; i++) {
            if (data[i][6] > m) {
                m = data[i][6];
            }
        }
        return m;
    }

    public static void processFolder2(File g, long[][] data, long[] as, String outDir, 
                                      String y, String approach, String name, 
                                      HashMap<String, String> answersSize) throws Exception {

        String dirName = g.getName();
        //System.out.println(dirName);
        String dirPath = g.getAbsolutePath();
        String numQuery = y;
        //System.out.println(answersSize);
        //System.out.println(numQuery);
        //String recall[] = getRecall(dirPath+"/Recall", times.length);
        //String recallM[] = getRecall(dirPath+"/RecallMains", times.length);
        //String recallD[] = getRecall(dirPath+"/RecallDesserts", times.length);
        String outputName = outDir+"/"+name+".dat";

        File f = new File(outputName);
        boolean exists = f.exists();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(outputName, true), "UTF-8"));
        if (!exists) {
            output.write("# "+outputName);
            output.newLine();
            output.write("# Query\t\"Answer Size\"\t\"Wrapper Time (secs)\"\t\"Graph Creation Time (secs)\"\t\"Execution Time (secs)\"\t\"Total Execution Time (secs)\"\t\"Time First Answer\"\t\"Answer Percentage\"\t\"Views or Rewritings considered\"\t\"Maximal Memory Used (triples)\"\t\"Throughput (# answers/millisec)\"\t\"Graph Union Size (in MB)\"\t\"How many times was Q executed\"");
            output.newLine();
        }
        String s = numQuery+"\t";
        System.out.println(numQuery);
        int size = Integer.parseInt(answersSize.get(numQuery));
        if (data.length == 0) {
            s = s + "0\t0\t0\t0\t0\t"+((int)(0/size))+"\t0\t0";
        } else {
            int pa[] = getAnswerPercentage(dirPath+"/answersInfo", size);
            int na[] = getAnswerNumber(dirPath+"/answersInfo");
            int p = na.length - 1;
            double t = 0;
            if (p >= 0) {
                s = s + na[p] + "\t";
                t = ((double) na[p]) / (data[data.length-1][5]);
            } else {
                s = s + "0\t";
            }
            p = data.length - 1;
            if (p >= 0) {
                s = s + data[p][2] + "\t" + data[p][3] + "\t" + data[p][4] + "\t" + data[p][5] + "\t";
            } else {
                s = s + "0\t0\t0\t0\t";
            }
            s = s + as[0] + "\t";
            p = pa.length - 1;
            if (p >= 0) {
                s = s + pa[p] + "\t";
            } else {
                s = s + "0\t";
            }
            p = data.length - 1;
            if (p >= 0) {
                s = s + data[p][1] + "\t";
            } else {
                s = s + "0\t";
            }
            long maxModelSize = getMaximalModelSize(data);
            s = s + maxModelSize; 
            s = s + "\t" + t + "\t" + as[1] + "\t" + data.length;
        }
        output.write(s);
        output.newLine();

        output.flush();
        output.close();
    }
}
