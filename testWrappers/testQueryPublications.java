class testQueryPublications {

    public static void main (String[] args) {

        String fileName = "/home/gabriela/SemLAV/testWrappers/dataMEV.rdf";
        String queryFile = "/home/gabriela/SemLAV/testWrappers/queryPublications1.sparql";

        Model m = FileManager.get().loadModel(fileName);
        Query query = QueryFactory.read(queryFile);
        QueryExecution result = QueryExecutionFactory.create(query, m);

            executionTimer.stop();
            timer.stop();
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                                               new FileOutputStream(fileName), "UTF-8"));
            executionTimer.resume();
            timer.resume();
            for (ResultSet rs = result.execSelect(); rs.hasNext();) {
                QuerySolution binding = rs.nextSolution();
                ArrayList<String> s = new ArrayList<String>();
                for (String var : query.getResultVars()) { 
                    RDFNode n = binding.get(var);
                    String val = null;
                    if (n != null) {
                        val = n.toString();
                    }
                    s.add(val);
                }
                executionTimer.stop();
                timer.stop();
                if (!queried && testing) {
                    message(id + "\t" + tempValue + "\t" + TimeUnit.MILLISECONDS.toMillis(wrapperTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(graphCreationTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(executionTimer.getTotalTime())
                            + "\t" + TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime())
                            + "\t" + graphUnion.size()
                            + "\t1");
                    time = 10;
                    queried = true;
                }
                output.write(s.toString());
                output.newLine();
                executionTimer.resume();
                timer.resume();
                if (TimeUnit.MILLISECONDS.toMillis(timer.getTotalTime()) >= timeout) {
                    break;
                }
            }
            executionTimer.stop();
            timer.stop();
            output.flush();
            output.close();
            executionTimer.resume();
            timer.resume();        

        QueryExecution qe = QueryExecutionFactory.create(queryFile, m);
        Model r = qe.execConstruct();
            OutputStream out = new FileOutputStream(resultFile);

            r.write(out, "N-TRIPLE");
            out.close();
            qe.close();
    }
}
