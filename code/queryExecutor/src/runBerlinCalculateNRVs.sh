#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUPS="510views"
QUERIES=`seq 13 13` 
DATASET=TenMillions

cp configD.properties.base configE.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configE.properties
sed -i".bkp" "s|DATASET|$DATASET|" configE.properties
for setup in $SETUPS ;do
    sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configE.properties
    for i in $QUERIES ;do
        sed -i".bkp" "s/query[0-9][0-9]*/query$i/" configE.properties
        java -cp ".:../lib2/*" semLAV/calculateNumberRVs configE.properties
    done
done
#rm configE.properties.bkp
#rm configE.properties
