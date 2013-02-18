SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr
cd /tmp/$user
svn co svn+ssh://$user@svn.cern.ch/reps/dirac/TestLHCbDIRAC/trunk/TestLHCbDIRAC
setenv PYTHONPATH /tmp/$user/:$PYTHONPATH
cd TestLHCbDIRAC/Regression
python Test_ProductionJobs.py -dd > testProductionJobs.txt
python Test_RegressionProductionJobs.py -dd > testRegressionProductionJobs.txt
python Test_RegressionUserJobs.py -dd > testRegressionUserJobs.txt
