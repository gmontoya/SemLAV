#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUPS="510views"
QUERIES="4 10 12 13 1" 
DATASET=TenMillions
MEMSIZE=20480m
j=1

cp configD.properties.base configE.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configE.properties
sed -i".bkp" "s|DATASET|$DATASET|" configE.properties
for setup in $SETUPS ;do
    sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configE.properties
    for i in $QUERIES ;do
        sed -i".bkp" "s/query[0-9][0-9]*/query$i/" configE.properties
        java -cp ".:../lib2/*" obtainNumberRewritings configE.properties $SemLAVPATH/expfiles/berlinOutput/$DATASET/$setup/outputSemLAVquery${i}_${MEMSIZE}_exec${j}/NOTHING/throughput
    done
done
rm configE.properties.bkp
rm configE.properties
