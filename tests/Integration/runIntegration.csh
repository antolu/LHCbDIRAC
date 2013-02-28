SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr
cd /tmp/$user
svn co svn+ssh://$user@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
setenv PYTHONPATH /tmp/$user/:$PYTHONPATH
cd LHCbTestDirac/Integration
python Test_ProductionJobs.py -dd > testProductionJobs.txt
python Test_UserJobs.py -dd > testUserJobs.txt
python Test_SAMJobs.py -dd > testSAMJobs.txt

