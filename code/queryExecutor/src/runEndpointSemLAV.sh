#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUPS="300views"
#QUERIES=`seq 1 18`
QUERY=$1
DATASET=FiveThousand
MEMSIZE=2048m
MEMSIZE2=2048m
TIMEOUT=600000
OUTPUT=$2
sorted=true
testing=false
n=1

cp configD.properties.base configD.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configD.properties
sed -i".bkp" "s|DATASET|$DATASET|" configD.properties
sed -i".bkp" "s|MEMSIZE|$MEMSIZE|" configD.properties
sed -i".bkp" "s|TIMEOUT|$TIMEOUT|" configD.properties
sed -i".bkp" "s/sorted=[a-z]*/sorted=$sorted/" configD.properties
sed -i".bkp" "s/testing=[a-z]*/testing=$testing/" configD.properties
sed -i".bkp" "s|OUTPUT|$OUTPUT|" configD.properties
for setup in $SETUPS ;do
    sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configD.properties
    sed -i".bkp" "s|exec[0-9]|exec${n}|" configD.properties
    sed -i".bkp" "s|QUERY|$QUERY|" configD.properties
    java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/evaluateQueryThreaded configD.properties
            #java -XX:MaxHeapSize=${MEMSIZE2} processAnswersSemLAV $SemLAVPATH/expfiles/berlinOutput/$DATASET/$setup/outputSemLAVquery${i}_${MEMSIZE}_exec${j}/NOTHING
done
rm configD.properties.bkp
rm configD.properties
