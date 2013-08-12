## To use this script it is necessary to change the file $SemLAVPATH/mcdsat/mcdsat/mcdsat
## comment line 35: $MODS --write-models $NNF | sed -n 's/{\([0-9 ]*\)}/\1/p' | python $MCDSATDIR/Main.py G $EXP $2 $3 $VIS.pyo $LOG2 &&
## that generate the rewritings, and include instead the line:
## $MODS $NNF
## that will just generate the theory and allows to obtain the number of rewritings without 
## enumerating them.

QUERIES=`seq 1 18`
SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUP=510views
DATASET=TenMillions
MEMSIZE=20480m
j=1

for i in $QUERIES; do
    java computeCoverage $SemLAVPATH/expfiles/berlinOutput/$DATASET/$SETUP/outputSemLAVquery${i}_${MEMSIZE}_exec${j}/NOTHING/throughput $SemLAVPATH/expfiles/berlinData/conjunctiveQueries/query${i} $SemLAVPATH/expfiles/berlinData/$DATASET/$SETUP/mappingsBerlin /tmp/mappingsBerlin $SemLAVPATH/mcdsat/mcdsat/mcdsat /tmp/coverage_query${i} 
done

