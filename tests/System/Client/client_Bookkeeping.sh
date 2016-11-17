#! /bin/bash

helpmessage="\n\nScript usage:\n\n
\t\t ./client_Bookkeeping.sh <--option=value> \n\n
e.g.\t  ./client_Bookkeeping.sh --Files=12 --Name='Pippo_files_' \n
e.g.\t  ./client_Bookkeeping.sh --Path=/path/to/directory/i/want/ \n
\n
Options:
\n\n
  -f=\t  --Files=\t\t:Insert number of files needed --Files=20 (default = 10)\n
  -n=\t  --Name=\t\t:Insert fale name that means something to you  --Name='Pippo_files_'\n
  -p=\t  --Path=\t\t:Insert the path that you wish to use --Path=<path_to_dir>\n
  -h\t   --help\t\t\t:Shows this help\n\n
\n\n
\t\t* If no values inserted it will use defaults values:\n
\t\t\t Number of Files => 10\n
\t\t\t File Names      => random_content_<random value>_00x.init\n
\t\t\t Path            => /tmp/<username>/tmp.<random value>/ \n
\n\n\n

"

#Default values
numberOfFiles=10
filesName="random_content_"
stime=$(date +"%H%M%S")
extra=$PWD
bkpath=$extra/BKReportsSamples/
# if [ $DIRAC ]
# then
#   diracDir=$DIRAC
# else
diracDir=$PWD
# fi
echo $diracDir
# Parsing arguments
if [ $# -gt 0 ]
then
  for i in "$@"
    do
      case $i in

        -h|--help|-?)
        echo -e $helpmessage
        exit 0
        ;;

        -f=*|--Files=*)
        numberOfFiles="${i#*=}"
        shift # past argument=value
        ;;

        -n=*|--Name=*)
        filesName="${i#*=}"
        shift # past argument=value
        ;;

        -p=*|--Path=*)
        temporaryPath="${i#*=}"
        if [ ! -d "$temporaryPath" ]
          then
          mkdir -p $temporaryPath
        fi
        shift # past argument=value
        ;;

        *)
        echo -e $helpmessage
        exit 0
            # unknown option
        ;;
      esac
    done
fi

# Default temporary path
if [ -z "$temporaryPath" ]
  then
  temporaryPath=$(mktemp -d)
fi

# Move to a tmp directory
cd $temporaryPath
if [ $? -ne 0 ]
  then
  echo $(tput setaf 1)"ERROR: cannot change to directory: " $temporaryPath$(tput sgr 0)
  exit $?
fi


# if [ $DIRAC ]
# then
#   diracDir=$DIRAC
# else
#   diracDir=$PWD
# fi

# Move to a tmp directory
# tmpDir=$(mktemp -d)
# echo $tmpDir

# cd $tmpDir
# if [ $? -ne 0 ]
# then
#   echo 'ERROR: cannot change to ' $tmpDir
#   return
# fi

# Generate 10 files with random content
# The names will be "random_content_X" and be between 1 and 10 Mb

# array of fileNames
$extra/random_files_creator.sh --Files=$numberOfFiles --Name=$filesName --Path=$temporaryPath

# fileNames=()
# for n in {1..10}
# do
#   fileNames+=($(date +"20%y%m%d")_$(date +"%H%M%S")_$( printf %03d "$n" ))
# done

# for n in {1..10}
# do
#   random10=$(( (RANDOM % 10) +1 )) # a random value between 1 and 10
#   dd if=/dev/urandom of=random_content_${fileNames[$n-1]}.init bs=1M count=$random10
# done

# Making sure the file types can be sent
# python $diracDir/tests/System/Client/dirac-add-bkk-ft.py INIT "just a desc for a test file type (INIT)" 1
# python $diracDir/tests/System/Client/dirac-add-bkk-ft.py FOO "just a desc for a test file type (FOO)" 1
# python $diracDir/tests/System/Client/dirac-add-bkk-ft.py BAR "just a desc for a test file type (BAR)" 1

# python $extra/dirac-add-bkk-ft.py INIT "just a desc for a test file type (INIT)" 1
# python $extra/dirac-add-bkk-ft.py FOO "just a desc for a test file type (FOO)" 1
# python $extra/dirac-add-bkk-ft.py BAR "just a desc for a test file type (BAR)" 1
#touch LFNlist.txt
files=$(ls $temporaryPath)
# Copy initXMLReport.xml template in tmpDir
# cp $diracDir/tests/System/Client/BKReportsSamples/InitXMLReport.xml .
#cp $diracDir/BKReportsSamples/InitXMLReport.xml
# For each random_content_X files, create a BK report, then send it
#for n in $(eval echo "{1..$numberOfFiles}")

version=$(echo $PYTHONPATH | tr ":" "\n" | grep \/DIRAC_v | sed 's/.*DIRAC_//') # dirac-version script broken
tdate=$(date +"20%y-%m-%d")
ttime=$(date +"%R")

for file in $files
do
  # Names of files
  
#  files=$filesName${fileNames[$n-1]}.init
  xmlName=bookkeeping_${file%.*}.xml

  # Create the specific BK report
  cp $extra/BKReportsSamples/InitXMLReport.xml $extra/BKReportsSamples/$xmlName
  chmod 777 $extra/BKReportsSamples/$xmlName
  # Getting the info
  size=$(stat --printf="%s" $file)
  # guid=$(python $diracDir/tests/System/Client/dirac-get-guid.py $tmpDir/$fileName -o LogLevel=FATAL)
  guid=$(python $extra/dirac-get-guid.py $temporaryPath/$file -o LogLevel=FATAL)  
  location=$HOSTNAME
  start=$(date -u +"20%y-%m-%d %R")
  end=$(date +"20%y-%m-%d %R")

  # Applying the info
  sed -i s/VAR_Name/${file%.*}/g $bkpath$xmlName
  sed -i s/VAR_Location/$location/g $bkpath$xmlName
  sed -i s/VAR_ProgramVersion/$version/g $bkpath$xmlName
  sed -i s/VAR_FileName/$file/g $bkpath$xmlName
  sed -i s/VAR_FileSize/$size/g $bkpath$xmlName
  sed -i "s/VAR_JobStart/$start/g" $bkpath$xmlName
  sed -i "s/VAR_JobEnd/$end/g" $bkpath$xmlName
  sed -i s/VAR_Date/$tdate/g $bkpath$xmlName
  sed -i s/VAR_Time/$ttime/g $bkpath$xmlName
  sed -i s/VAR_ShortenTime/$stime/g $bkpath$xmlName
  sed -i s/VAR_Guid/$guid/g $bkpath$xmlName

  echo "/lhcb/Certification/Test/INIT/$version/$tdate/$stime/$file \
  .$temporaryPath$file" >> $extra/LFNlist.txt
#  python $diracDir/tests/System/Client/dirac-send-bk-report.py $xmlName -ddd
  python $extra/dirac-send-bk-report.py $bkpath$xmlName -ddd
done
