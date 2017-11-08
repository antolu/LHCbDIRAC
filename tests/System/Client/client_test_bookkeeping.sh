echo "\n\n \t\t\t ===> Now some simple scripts  "
echo " "
echo "======  dirac-bookkeeping-run-files 81789"
dirac-bookkeeping-run-files 81789
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
echo "======  dirac-bookkeeping-gui"
dirac-bookkeeping-gui
if [ $? -ne 0 ]
then
   exit $?
fi
echo " "
