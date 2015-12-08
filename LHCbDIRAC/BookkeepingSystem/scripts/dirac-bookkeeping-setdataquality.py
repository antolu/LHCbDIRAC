#! /usr/bin/env python

import DIRAC
from DIRAC           import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base import Script
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  
import re

bkClient = BookkeepingClient()

################################################################################
#                                                                              #
# CheckDQFlag:                                                                 #
#                                                                              #
# Make sure the DQ flag is a known one.                                        #
#                                                                              #
################################################################################
def CheckDQFlag(dqFlag):
  res = bkClient.getAvailableDataQuality()
  if not res['OK']:
    gLogger.error(res['Message'])
    return S_ERROR()

  if dqFlag in res['Value']:
    retVal = S_OK()
  else:
    retVal = S_ERROR()

  return retVal
################################################################################
#                                                                              #
# FlagBadRun:                                                                  #
#                                                                              #
# A run is BAD, flag it and all its files BAD.                                 #
#                                                                              #
################################################################################
def FlagBadRun(runNumber):
  res = GetProcessingPasses(runNumber, '/Real Data')
  if not res['OK']:
    gLogger.error('FlagBadRun: %s' %(res['Message']))
    return 2
        
  allProcPass = res['Value']
    
  res = bkClient.setRunDataQuality(int(runNumber), 'BAD')
  if not res['OK']:
    gLogger.error('FlagBadRun: %s' %(res['Message']))
    return 2

  retVal = 0
  for thisPass in allProcPass:
    res = bkClient.setRunAndProcessingPassDataQuality(runNumber, thisPass, 'BAD')
    if not res['OK']:
      gLogger.error('FlagBadRun: %s' %(res['Message']))
      retVal = 2
      break

  return retVal
################################################################################
#                                                                              #
# FlagFileList:                                                                #
#                                                                              #
# Flag a LFN or a list of LFN contained in a file.                             #
#                                                                              #
################################################################################
def FlagFileList(filename, dqFlqg):
  lfns = []

  #
  # Load the list of LFN
  #
    
  try:
    f = open(filename)
    for lfn in f:
      lfns.append(lfn.strip())
  except Exception:
    lfns = [filename]

  #
  # Now flag the LFN
  #
    
  retVal = 0
  res    = bkClient.setFileDataQuality(lfns, dqFlag)

  if not res['OK']:
    gLogger.error('FlagFileList: %s' %(res['Message']))
    retVal = 2
  else:
    print 'The data quality has been set %s for the following files:' %dqFlag
    for l in lfns:
      print l
            
  return retVal
################################################################################
#                                                                              #
# FlagRun:                                                                     #
#                                                                              #
# Flag a run given its number, the processing pass and the DQ flag.            #
#                                                                              #
################################################################################
def FlagRun(runNumber, procPass, dqFlag):
  res = GetProcessingPasses(runNumber, '/Real Data')
  if not res['OK']:
    gLogger.error('FlagRun: %s' %(res['Message']))
    return 2
        
  allProcPass = res['Value']

  #
  # Make sure the processing pass entered by the operator is known
  #

  if procPass not in  allProcPass:
    gLogger.error('%s is not a valid processing pass.' %procPass)
    return 2

  #
  # Add to the list all other processing pass, like stripping, calo-femto..
  #

  allProcPass = [procPass]
  res = GetProcessingPasses(runNumber, procPass)
  if not res['OK']:
    gLogger.error('FlagRun: %s' %(res['Message']))
    return 2
        
  allProcPass.extend(res['Value'])

  # Flag the processing passes

  retVal = 0
  for thisPass in allProcPass:
    res = bkClient.setRunAndProcessingPassDataQuality(str(runNumber),
                                                      thisPass,
                                                      dqFlag)
    if not res['OK']:
      gLogger.error('FlagRun: processing pass %s\n error: %s' %(thisPass,
                                                                  res['Message']))
      retVal = 2
      break
    else:
      print 'Run %s Processing Pass %s flagged %s' %(str(runNumber),
                                                            thisPass,
                                                            dqFlag)
    
  return retVal
################################################################################
#                                                                              #
# GetProcessingPasses:                                                         #
#                                                                              #
# Find all known processing passes for the selected configurations.            #
#                                                                              #
################################################################################
def GetProcessingPasses(runNumber, headPass):
  passes = []

  res = bkClient.getRunInformations(int(runNumber))
  if not res['OK']:
    return S_ERROR(res['Messgage'])

  cfgName    = res['Value']['Configuration Name']
  cfgVersion = res['Value']['Configuration Version']

  bkDict = {'ConfigName'    : cfgName,
            'ConfigVersion' : cfgVersion}

  res = bkClient.getProcessingPass(bkDict, headPass)
  if not res['OK']:
    return S_ERROR('Cannot load the processing passes for Version %s' % cfgVersion )

  for recordList in res['Value']:
    if recordList['TotalRecords'] == 0:
      continue
    parNames = recordList['ParameterNames']

    found = False
    for thisId in xrange(len(parNames)):
      parName = parNames[thisId]
      if parName == 'Name':
        found = True
        break
    if found:
      for reco in recordList['Records']:
        recoName = headPass + '/' + reco[0]
        passes.append(recoName)
                
  return S_OK(passes)
################################################################################
#                                                                              #
#                                  >>> Main <<<                                #
#                                                                              #
################################################################################

gLogger.setLevel("VERB")

Script.registerSwitch("l:", "lfnfile",        "Flag a LFN or list of LFN")
Script.registerSwitch("r:", "run",            "Flag a run")
Script.registerSwitch("p:", "processingPass", "Processing pass for which a run should be flagged")

Script.setUsageMessage('\n'.join([ 'Usage:',
                                   '  %s -l [LFN|filename] dqflag'        % Script.scriptName,
                                   '  %s -r runNumber BAD'                % Script.scriptName,
                                   '  %s -r runNumber -p procPass dqFlag' % Script.scriptName,
                                   '']))

Script.parseCommandLine()

args     = Script.getPositionalArgs()
switches = Script.getUnprocessedSwitches()

if len(args):
  dqFlag = args[0].upper()
else:
  dqFlag = ''

filename  = ''
runNumber = ''
procPass  = '' 
#
# Read the switchs.
#

for switch in switches:
  opt = switch[0].lower()
  val = switch[1]

  if opt in ('l', 'lfnfile'):
    filename = val
  elif opt in ('r', 'run'):
    runNumber = str(val)
  elif opt in ('p', 'processingpass'):
    procPass = val

#
# Verify the user input and execute if correct.
#

exitCode = 0
if not len(dqFlag):
  Script.showHelp()
  exitCode = 2
else:
  res = CheckDQFlag(dqFlag)
  if res['OK']:
    if len(filename) and len(runNumber):
      Script.showHelp()
      exitCode = 2
    elif not len(filename) and not len(runNumber):
      Script.showHelp()
      exitCode = 2
    elif len(filename):
      FlagFileList(filename, dqFlag)
    elif len(runNumber):
      if dqFlag == 'BAD':
        exitCode = FlagBadRun(runNumber)
        if exitCode == 0:
          gLogger.info('Run % is flagged BAD')
      else:
        if not len(procPass):
          Script.showHelp()
          exitCode = 2
        else:
          m = re.search('/Real Data', procPass)
          if not m:
            gLogger.error('You forgot /Real Data in the processing pass: %s' % procPass);
            exitCode = 2
          else:
            exitCode = FlagRun(runNumber, procPass, dqFlag)
  else:
    exitCode = 2
            
    
DIRAC.exit(exitCode)
