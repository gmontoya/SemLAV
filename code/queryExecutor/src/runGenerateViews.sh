#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
FACTOR="34"
DATASET=TenMillions
DATASETFILE=$SemLAVPATH/expfiles/berlinData/datasets/dataset${DATASET}.nt
MEMSIZE=20480m
SETUP=510views

cp configData.properties.base configData.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configData.properties
sed -i".bkp" "s|DATASET|$DATASET|" configData.properties
sed -i".bkp" "s/[0-9][0-9]*views/$SETUP/" configData.properties
sed -i".bkp" "s|dataSetFile=[^\n]*|dataSetFile=$DATASETFILE|" configData.properties

for f in $FACTOR ;do
    SETUP=$(($f*15))views
    sed -i".bkp" "s/factor=[0-9][0-9]*/factor=$f/" configData.properties

    java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/generateViews
done

rm configData.properties
rm configData.properties.bkp
