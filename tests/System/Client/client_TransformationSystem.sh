#!/bin/bash

#
echo "lhcb-proxy-init -g lhcb_prmgr"
lhcb-proxy-init -g lhcb_prmgr
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "


#Values to be used
userdir=$( echo "$USER" |cut -c 1)/$USER
stamptime=$(date +%Y%m%d_%H%M%S)
mkdir -p TransformationSystemTest

#selecting a random USER Storage Element
SEs=$(dirac-dms-show-se-status |grep USER |grep -v 'Banned\|Degraded\|-2' | awk '{print $1}')
x=0
for n in $SEs
do
	arrSE[x]=$n
	let x++
done
# random=$[ $RANDOM % $x ]
# randomSE=${arrSE[$random]}

echo ""
echo "Submitting test production"
python dirac-test-production.py -ddd
if [ $? -ne 0 ]
then
   exit $?
fi

transID=`cat TransformationID`

# Create unique files
echo ""
echo "Creating unique test files"
./random_files_creator.sh --Files=5 --Name="Test_Transformation_System_" \
  --Path=$PWD/TransformationSystemTest/



# Add the random files to the transformation
echo ""
echo "Adding files to Storage Element $randomSE"
filesToUpload=$(ls TransformationSystemTest/)
for file in $filesToUpload
do
	random=$[ $RANDOM % $x ]
	randomSE=${arrSE[$random]}
	echo "/lhcb/user/$userdir/TransformationSystemTest/$stamptime/$file \
	     ./TransformationSystemTest/$file $randomSE" \
	     >> TransformationSystemTest/LFNlist.txt
done
dirac-dms-add-file TransformationSystemTest/LFNlist.txt

LFNlist=$(cat TransformationSystemTest/LFNlist.txt | awk -vORS=, '{print $1}')

echo ""
echo "Adding the files to the test production"
dirac-transformation-add-files $transID \
 --LFNs=$LFNlist
if [ $? -ne 0 ]
then
   exit $?
fi

# ___ Use Ramdom SEs___