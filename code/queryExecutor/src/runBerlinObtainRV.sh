#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
SETUPS="510views"
QUERIES=`seq 1 12` 
DATASET=TenMillions
MEMSIZE=20480m
folder=$SemLAVPATH/expfiles/berlinData

cp configD.properties.base configD.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configD.properties
sed -i".bkp" "s|DATASET|$DATASET|" configD.properties
sed -i".bkp" "s|MEMSIZE|$MEMSIZE|" configD.properties
#sed -i".bkp" "s|TIMEOUT|$TIMEOUT|" configD.properties
for setup in $SETUPS ;do
    sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configD.properties
    for i in $QUERIES ;do
        sed -i".bkp" "s/query[0-9][0-9]*/query$i/" configD.properties
        java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/obtainRelevantViews configD.properties ${folder}/$DATASET/${setup}/relevantViews
    done
done
rm configD.properties.bkp
rm configD.properties
