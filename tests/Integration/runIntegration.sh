SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr
cd /tmp/$USER
svn co svn+ssh://$USER@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
export PYTHONPATH=/tmp/$USER/:$PYTHONPATH
cd LHCbTestDirac/Integration
python Test_ProductionJobs.py -dd > testProductionJobs.txt
python Test_UserJobs.py -dd > testUserJobs.txt
python Test_SAMJobs.py -dd > testSAMJobs.txt

