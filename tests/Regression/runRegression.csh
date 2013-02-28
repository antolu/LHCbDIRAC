SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr
cd /tmp/$user
svn co svn+ssh://$user@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
setenv PYTHONPATH /tmp/$user/:$PYTHONPATH
cd LHCbTestDirac/Regression
python Test_RegressionProductionJobs.py -dd > testRegressionProductionJobs.txt
python Test_RegressionUserJobs.py -dd > testRegressionUserJobs.txt
