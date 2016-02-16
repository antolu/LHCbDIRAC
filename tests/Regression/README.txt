==================
Regression Testing
==================

To be run after integreation tests have finished and checked, and before running the system tests.

After having installed the client version on AFS, login on lxplus and issue:

wget http://svnweb.cern.ch/world/wsvn/dirac/LHCbTestDirac/trunk/LHCbTestDirac/Regression/runRegression.csh

This will do the following:

SetupProject LHCbDirac --dev (by default this is the certification setup)
lhcb-proxy-init -g lhcb_prmgr (will ask for your password)
cd /tmp/yourId
svn co svn+ssh://yourId@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
setenv PYTHONPATH /tmp/yourId/:$PYTHONPATH
cd LHCbTestDirac/Regression

Then, it will run the following 3 test files to run: 
- 'Test_RegressionProductionJobs.py'
- 'Test_RegressionUserJobs.py'
 
and save the result in a txt file for each of them. Running these tests can take some time.
