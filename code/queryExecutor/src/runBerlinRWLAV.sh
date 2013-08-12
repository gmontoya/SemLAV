#!/bin/bash

STRATS="JENA"
SETUPS="510views"
REWRITERS="gqr minicon"
QUERIES=`seq 1 18`
DATASET=TenMillions
MEMSIZE=20480m
MEMSIZE2=81920m
TIMEOUT=600000
SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`

cp configC.properties.base configC.properties
sed -i".bkp" "s|SemLAVPATH|$SemLAVPATH|" configC.properties
sed -i".bkp" "s|DATASET|$DATASET|" configC.properties
sed -i".bkp" "s|MEMSIZE|$MEMSIZE|" configC.properties
sed -i".bkp" "s|TIMEOUT|$TIMEOUT|" configC.properties
for str in $STRATS ;do
    sed -i".bkp" "s/jointype=[A-Z]*/jointype=$str/" configC.properties
    for setup in $SETUPS ;do
        sed -i".bkp" "s/[0-9][0-9]*views/$setup/" configC.properties
        nv=${setup:0:$((${#setup}-5))}
        sed -i".bkp" "s/numberviews=[0-9][0-9]*/numberviews=$nv/" configC.properties
        for rw in $REWRITERS ;do
            sed -i".bkp" "s/rewriter=[a-z]*/rewriter=$rw/" configC.properties
            for i in $QUERIES ;do
                for j in `seq 1 1` ;do
                    sed -i".bkp" "s|exec[0-9]|exec${j}|" configC.properties
                    sed -i".bkp" "s/query[0-9][0-9]*/query$i/" configC.properties
                    java -XX:MaxHeapSize=${MEMSIZE} -cp ".:../lib2/*" semLAV/executionMCDSATThreaded
                    java -XX:MaxHeapSize=${MEMSIZE2} processAnswersSemLAV $SemLAVPATH/expfiles/berlinOutput/$DATASET/$setup/outputquery${i}_${MEMSIZE}_exec${j}${str}_$rw
                done
            done
        done
    done
done

rm configC.properties.bkp
rm configC.properties
