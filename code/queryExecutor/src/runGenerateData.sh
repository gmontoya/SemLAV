#!/bin/bash

SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
FACTOR="34"
DATASET=TenMillions
DATASETFILE=$SemLAVPATH/code/expfiles/berlinData/datasets/dataset${DATASET}.nt
SETUP=510views
MEMSIZE=40960m

cp configData.properties.base configData.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configData.properties
sed -i".bkp" "s|DATASET|$DATASET|" configData.properties
sed -i".bkp" "s/[0-9][0-9]*views/$SETUP/" configData.properties
sed -i".bkp" "s/factor=[0-9][0-9]*/factor=$f/" configData.properties
sed -i".bkp" "s|dataSetFile=[^\n]*|dataSetFile=$DATASETFILE|" configData.properties

for f in $FACTOR ;do
    SETUP=$(($f*15))views
    cp configData.properties.base configData.properties
    sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configData.properties
    sed -i".bkp" "s|DATASET|$DATASET|" configData.properties
    sed -i".bkp" "s/[0-9][0-9]*views/$SETUP/" configData.properties
    sed -i".bkp" "s/factor=[0-9][0-9]*/factor=$f/" configData.properties
    sed -i".bkp" "s|dataSetFile=[^\n]*|dataSetFile=$DATASETFILE|" configData.properties

    java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" experimentseswc/generateViews
    java -cp ".:../lib2/*" experimentseswc/generateMappings
done

java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/generateAnswers
$SemLAVPATH/code/expfiles/scripts/sortFolder.sh $SemLAVPATH/code/expfiles/berlinData/$DATASET/answers

rm configData.properties
rm configData.properties.bkp
