===================
Integration Testing
===================

To be run after unit tests have finished and checked, and before running the regression tests.

After having installed the client version on AFS, login on lxplus and issue:

wget http://svnweb.cern.ch/world/wsvn/dirac/LHCbTestDirac/trunk/LHCbTestDirac/Regression/runIntegration.csh

This will do the following:

SetupProject LHCbDirac --dev (by default this is the certification setup)
lhcb-proxy-init -g lhcb_prmgr (will ask for your password)
cd /tmp/yourId
svn co svn+ssh://yourId@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
setenv PYTHONPATH /tmp/yourId/:$PYTHONPATH
cd LHCbTestDirac/Integration

Then, it will run the following 3 test files to run: 
- 'Test_ProductionJobs.py'
- 'Test_UserJobs.py'
- 'Test_SAMJobs.py'
 
and save the result in a txt file for each of them. Running these tests can take some time.
