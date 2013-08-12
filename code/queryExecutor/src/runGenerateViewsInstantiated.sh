#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
FACTOR="34"
DATASET=TenMillions
DATASETFILE=$SemLAVPATH/expfiles/berlinData/datasets/dataset${DATASET}.nt
MEMSIZE=20480m

for f in $FACTOR ;do
    SETUP=$(($f*15))views
    cp configData.properties.base configData.properties
    sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configData.properties
    sed -i".bkp" "s|DATASET|$DATASET|" configData.properties
    sed -i".bkp" "s/[0-9][0-9]*views/$SETUP/" configData.properties
    sed -i".bkp" "s/factor=[0-9][0-9]*/factor=$f/" configData.properties
    sed -i".bkp" "s|dataSetFile=[^\n]*|dataSetFile=$DATASETFILE|" configData.properties

    java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/generateViewsInstantiated $SemLAVPATH/expfiles/berlinData/$DATASET/$SETUP/usedViewInstantiationsSortedNEW
done

rm configData.properties
rm configData.properties.bkp
