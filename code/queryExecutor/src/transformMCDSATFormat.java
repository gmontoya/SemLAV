import semLAV.Counter;

import java.util.*;
import java.io.*;

public class transformMCDSATFormat {

    public static void main(String args[]) {

        try {
            String nameIn = args[0];
            String nameOut = args[1];
            Counter i = new Counter();
            i.increase();
            BufferedReader br = new BufferedReader(new FileReader(nameIn));
            String l = br.readLine();
            String queryString = "";
            while (l != null) {
                queryString = queryString + l + "\n";
                l = br.readLine();
            }

            while (breakFormat(queryString)) {
                queryString = fixOneVariable(queryString, i);
            }

            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                    new FileOutputStream(nameOut), 
                                                         "UTF-8"));
            output.write(queryString);
            output.flush();
            output.close();
        }  catch (Exception e) {
			e.printStackTrace(System.out);
        }        
    }

    public static String fixOneVariable (String queryString, Counter i) {

        StringTokenizer st = new StringTokenizer(queryString, " \t\n\r\f");

        while (st.hasMoreTokens()) {

            String t = st.nextToken();
            if (t.startsWith("?") && ((t.length() < 3) || (t.charAt(1) != 'X'))) {
                boolean badFormat = false;
                for (int j = 2; j < t.length(); j++) {
                    if (!Character.isDigit(t.charAt(j))) {
                        badFormat = true;
                    }
                }
		if ((t.length() < 3) || badFormat) {
                    String newVar = "\\?X"+i.getValue();
                    queryString = queryString.replaceAll("\\?"+t.substring(1)+" ", newVar+" ");
                    i.increase();
                    return queryString;
		}
            }
        }
        return queryString;
    }

    public static boolean breakFormat(String queryString) {

        StringTokenizer st = new StringTokenizer(queryString, " \t\n\r\f");

        while (st.hasMoreTokens()) {

            String t = st.nextToken();
            if (t.startsWith("?")) {
                if ((t.length() < 3) || (t.charAt(1) != 'X')) {
                    return true;
                } else {
                    for (int i = 2; i < t.length(); i++) {
                        if (!Character.isDigit(t.charAt(i))) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
