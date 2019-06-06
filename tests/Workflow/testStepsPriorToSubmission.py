###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" This is a script for testing production steps prior to submission. Full instructions can be found here:

        https://twiki.cern.ch/twiki/bin/view/LHCb/ProdReqPreLaunchTest

    To run this test, follow the 4 steps below:

    1. Login on lxplus (new session, please)
    2. lb-run LHCbDIRAC/prod -norc lhcb-proxy-init
    3. put the following file in ~/.dirac.cfg
        DIRAC
        {
          Setup=LHCb-Production
        }
        LocalSite
        {
          Site = DIRAC.Jenkins.ch
          GridCE = jenkins.cern.ch
          CEQueue = jenkins-queue_not_important
          LocalSE = CERN-DST-EOS
          LocalSE += CERN-HIST-EOS
          LocalSE += CERN-RAW
          LocalSE += CERN-FREEZER-EOS
          LocalSE += CERN-SWTEST
          Architecture = x86_64-slc6
          CPUScalingFactor = 5.7
          CPUNormalizationFactor = 5.7
          SharedArea = /cvmfs/lhcb.cern.ch/lib
          CPUTimeLeft = 123456
        }
    4. edit this py file only in the part where it's written "EDIT HERE"
    5. lb-run LHCbDIRAC/prod bash -norc
    6. RUN the edited script using: python testStepsPriorToSubmission.py -ddd > LHCbDIRACLog.txt

    Once you're done, it'd better if you remove (or better rename) the ~/.dirac.cfg file.
"""


##################################
### EDIT HERE ###

#Just put one file, or 2, or whatever you want to be in input of your test job
lfns = ['/lhcb/data/2016/RAW/TURBO/LHCb/COLLISION16/173905/173905_0000000126.raw'] # a list of strings

#Answer this: do you want to include the ancestors of the files above? If yes, then at which depth?
includeAncestors = False
ancestorsDepth = 1

#Answer also this: do you have already deployed steps (in the step manager web page)? (True/False) Then look at what's next
stepReady = True

#And, one more question: is this a merging step that you are testing?
mergingStep = False

if stepReady: #if this is True, please provide a step number
  stepsList = [129889] # a list of stepIDs
else: #you'll need to answer ALL the following (I'm just giving examples) NOTE: this works for ONLY 1 step, not for a list:
  fileTypesIn = ['RAW'] # a list of strings
  fileTypesOut = ['CHARM1.MDST','CHARM2.MDST','CHARM3PRESCALED.MDST','CHARM4PRESCALED.MDST',
                  'CHARMSTRANGELFV.MDST','DIMUON.MDST','LAMBDAPRESCALED.MDST'] # a list of strings
  applicationName = 'DaVinci'
  applicationVersion = 'v41r1'
  systemConfig = 'x86_64-slc6-gcc48-opt'
  extraPackages = 'AppConfig.v3r287;TurboStreamProd.v4r1'
  optionFiles = '$APPCONFIGOPTS/Turbo/Streams_v4r1.py;$APPCONFIGOPTS/Turbo/Tesla_Data_2016_forStreams_v1.py'
  optionsFormat = 'Tesla'
  dddb = 'dddb-20150724'
  conddb = 'cond-20160420'

##################################




##################################
### DO NOT EDIT FROM HERE

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

pr = ProductionRequest()
diracProduction = DiracProduction()

if stepReady:
  pr.stepsList = stepsList
  pr.outputVisFlag = [{"1": 'Y'}]
  pr.resolveSteps()
  stepsInProd = pr.stepsListDict
  pr.outputSEs = ['Tier1-DST']
  pr.specialOutputSEs = [{}]
  pr._determineOutputSEs()
  outDict = pr.outputSEsPerFileType[0]
else:
  stepsInProd = [{'StepId': 12345, 'StepName': 'Whatever',
                  'ApplicationName': applicationName,
                  'ApplicationVersion': applicationVersion,
                  'ExtraPackages': extraPackages, 'ProcessingPass': 'whoCares',
                  'Visible': 'N', 'Usable': 'Yes',
                  'DDDB': dddb, 'CONDDB': conddb, 'DQTag': '', 'OptionsFormat': optionsFormat,
                  'OptionFiles': optionFiles,
                  'isMulticore': 'N',
                  'SystemConfig': systemConfig, 'mcTCK': '', 'ExtraOptions': '',
                  'fileTypesIn':fileTypesIn,
                  'fileTypesOut':fileTypesOut,
                  'visibilityFlag': [{'Visible': 'Y', 'FileType': fileTypesOut}]
                 }]
  pr.outputSEs = ['Tier1-DST']
  pr.specialOutputSEs = [{}]
  outDict = {t: 'Tier1-DST' for t in fileTypesOut}

if not includeAncestors:
  ancestorsDepth = 0

if mergingStep:
  jobType = 'Merge'
else:
  jobType = 'Turbo' #whatever...
prod = pr._buildProduction( jobType, stepsInProd, outDict, 0, 100,
                             inputDataPolicy = 'protocol', inputDataList = lfns,
                             ancestorDepth = ancestorsDepth )

diracProduction.launchProduction( prod, False, True, 0 )
