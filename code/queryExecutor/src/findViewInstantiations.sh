SETUPS="510views"
SemLAVPATH=`pwd | rev | cut -d"/" -f 3- | rev`
folder=$SemLAVPATH/expfiles/berlinData
outputFolder=$SemLAVPATH/expfiles/berlinOutput
DATASET=TenMillions

for setup in $SETUPS ;do
    java -cp ".:../lib2/*" findViewInstantiations ${folder}/$DATASET/${setup}/relevantViews ${folder}/$DATASET/${setup}/usedViewInstantiations
done
