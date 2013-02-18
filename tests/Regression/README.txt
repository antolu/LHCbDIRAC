==================
Regression Testing
==================

Used as acceptance tests, once unit tests have finished, and before running the system tests.

Process (logging to lxplus after having installed the version under test):

SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr

cd /tmp/yourId

svn co svn+ssh://yourId@svn.cern.ch/reps/dirac/TestLHCbDIRAC/trunk/TestLHCbDIRAC

setenv PYTHONPATH /tmp/yourId/:$PYTHONPATH

cd TestLHCbDIRAC/Regression

There are 3 test files to be run: 'Test_ProductionJobs.py', 'Test_RegressionProductionJobs.py' and 'Test_RegressionUserJobs.py'. 
The example that follows is for the 'Test_ProductionJobs.py', and should be repeated also for the others.

python Test_ProductionJobs.py -dd > test.txt &

If some test fails, edit the Test_ProductionJobs.py file commenting the "tearDown" lines, then restart it. 
This way you will not delete the output at the end of the test, so that you can debug the issue.