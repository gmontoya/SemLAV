#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUPS="510views"
QUERIES=`seq 1 18` 
DATASET=TenMillions
MEMSIZE=20480m
MEMSIZE2=40960m
TIMEOUT=600000
sorted=true
testing=true
n=1

cp configD.properties.base configD.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configD.properties
sed -i".bkp" "s|DATASET|$DATASET|" configD.properties
sed -i".bkp" "s|MEMSIZE|$MEMSIZE|" configD.properties
sed -i".bkp" "s|TIMEOUT|$TIMEOUT|" configD.properties
sed -i".bkp" "s/sorted=[a-z]*/sorted=$sorted/" configD.properties
sed -i".bkp" "s/testing=[a-z]*/testing=$testing/" configD.properties
for setup in $SETUPS ;do
    sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configD.properties
    for i in $QUERIES ;do
        for j in `seq 1 $n` ;do
            sed -i".bkp" "s|exec[0-9]|exec${j}|" configD.properties
            sed -i".bkp" "s/query[0-9][0-9]*/query$i/" configD.properties
            #sed -i".bkp" "s|QUERY|$SemLAVPATH/expfiles/berlinData/sparqlQueries/query$i|" configD.properties
            java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/evaluateQueryThreaded configD.properties
            java -XX:MaxHeapSize=${MEMSIZE2} processAnswersSemLAVFast $SemLAVPATH/expfiles/berlinOutput/$DATASET/$setup/outputSemLAVquery${i}_${MEMSIZE}_exec${j}/NOTHING $SemLAVPATH/expfiles/berlinData/$DATASET/answers/query${i} false
        done
    done
done
rm configD.properties.bkp
rm configD.properties
