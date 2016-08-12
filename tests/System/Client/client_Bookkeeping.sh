#! /bin/bash

diracDir=$PWD

# Move to a tmp directory
tmpDir=$(mktemp -d)
cd $tmpDir
echo $tmpDir

# Generate 10 files with random content
# The names will be "random_content_X" and be between 1 and 10 Mb

# array of fileNames
fileNames=()
for n in {1..10}
do
  fileNames+=(random_content_$(date +"20%y%m%d")_$(date +"%H%M%S")_$( printf %03d "$n" ))
done

for n in {1..10}
do
  random10=$(( (RANDOM % 10) +1 )) # a random value between 1 and 10
  dd if=/dev/urandom of=${fileNames[$n-1]}.init bs=1M count=$random10
done

# Copy initXMLReport.xml template in tmpDir
cp $diracDir/tests/System/Client/BKReportsSamples/InitXMLReport.xml .

# For each random_content_X files, create a BK report
for n in {1..10}
do
  # Create the specific BK report
  xmlName=bookkeping_${fileNames[$n-1]}.xml
  cp InitXMLReport.xml $xmlName

  # Getting the info
  fileName=random_content_$( printf %03d "$n" ).init
  size=$(stat --printf="%s" random_content_$( printf %03d "$n" ).init)
  guid=$(python $diracDir/tests/System/Client/dirac-get-guid.py $tmpDir/random_content_$( printf %03d "$n" ).init -o LogLevel=FATAL)
  location=$HOSTNAME
  version=$(dirac-version)
  tdate=$(date +"20%y-%m-%d")
  ttime=$(date +"%R")
  start=$(date -u +"20%y-%m-%d %R")
  end=$(date +"20%y-%m-%d %R")

  # Applying the info
  sed -i s/VAR_Name/00000001_0000000$( printf %03d "$n" )_$( printf %03d "$n" )/g $xmlName
  sed -i s/VAR_Location/$location/g $xmlName
  sed -i s/VAR_ProgramVersion/$version/g $xmlName
  sed -i s/VAR_FileName/$fileName/g $xmlName
  sed -i s/VAR_FileSize/$size/g $xmlName
  sed -i "s/VAR_JobStart/$start/g" $xmlName
  sed -i "s/VAR_JobEnd/$end/g" $xmlName
  sed -i s/VAR_Date/$tdate/g $xmlName
  sed -i s/VAR_Time/$ttime/g $xmlName
  sed -i s/VAR_Guid/$guid/g $xmlName
done

# Making sure the file types can be sent
python ./tests/System/Client/dirac-add-bkk-ft.py INIT "just a desc for a test file type (INIT)" 1
python ./tests/System/Client/dirac-add-bkk-ft.py FOO "just a desc for a test file type (FOO)" 1
python ./tests/System/Client/dirac-add-bkk-ft.py BAR "just a desc for a test file type (BAR)" 1

# Now we send the bookkeping reports
for n in {1..10}
do
  python $diracDir/tests/System/Client/dirac-send-bk-report.py bookkeping_00000001_0000000$( printf %03d "$n" )_$( printf %03d "$n" ).xml -ddd
done
