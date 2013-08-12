import java.util.*;

class answerSizeSemLAV {

    public static void main (String [] args ) {

        String solutionFile = args[0];
        HashSet<ArrayList<String>> solution = processAnswersSemLAV.loadSolution(solutionFile);
        System.out.println(solution.size());
    }
}
