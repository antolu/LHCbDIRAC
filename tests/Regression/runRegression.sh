#!/bin/bash
SetupProject LHCbDirac --dev
lhcb-proxy-init -g lhcb_prmgr
cd /tmp/$USER
svn co svn+ssh://$USER@svn.cern.ch/reps/dirac/LHCbTestDirac/trunk/LHCbTestDirac
export PYTHONPATH=/tmp/$USER/:$PYTHONPATH
cd LHCbTestDirac/Regression
python Test_RegressionProductionJobs.py -dd > testRegressionProductionJobs.txt
python Test_RegressionUserJobs.py -dd > testRegressionUserJobs.txt
