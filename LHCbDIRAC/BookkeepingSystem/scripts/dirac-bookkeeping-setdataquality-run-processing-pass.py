#! /usr/bin/env python
"""
Flags all files in a processing pass and its descendants + flags the RAW files
Parameters:
   <Processing pass> : processing pass to start from (can be /RealData)
   <run> : run number
   <flag> : flag to set
"""

import DIRAC
from DIRAC           import gLogger
from DIRAC.Core.Base import Script

################################################################################
#                                                                              #
# GetProcessingPasses:                                                         #
#                                                                              #
# Find all known processing passes for the selected configurations.            #
#                                                                              #
################################################################################
def GetProcessingPasses( bkDict, headPass ):
  passes = {}
  res = bkClient.getProcessingPass( bkDict, headPass )
  if not res['OK']:
    gLogger.error( 'Cannot load the processing passes for head % in Version %s Data taking condition %s' % ( 
      headPass, bkDict['ConfigVersion'], bkDict['ConditionDescription'] ) )
    gLogger.error( res['Message'] )
    DIRAC.exit( 2 )

  for recordList in res['Value']:
    if recordList['TotalRecords'] == 0:
      continue
    parNames = recordList['ParameterNames']

    found = False
    for thisId in xrange( len( parNames ) ):
      parName = parNames[thisId]
      if parName == 'Name':
          found = True
          break
    if found:
      for reco in recordList['Records']:
        recoName = headPass + '/' + reco[0]
        passes[recoName] = True
        passes.update( GetProcessingPasses( bkDict, recoName ) )

  return passes
################################################################################
#                                                                              #
#                                  >>> Main <<<                                #
#                                                                              #
################################################################################

Script.setUsageMessage( 'Usage: %s <Processing Pass> <run> <status> <flag>' % ( Script.scriptName ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()
if len( args ) < 3:
  Script.showHelp()
  DIRAC.exit( 2 )

exitCode = 0

realData = '/Real Data'
processing = args[0].replace( '/RealData', realData )
if processing == '':
  processing = realData
run = args[1]
flag = args[2]

# gLogger.error('Please wait for the OK from Stefan before actually flagging')
# gLogger.error('Do ELOG the flag in the DQ ELOG fr future rerefence.')
# DIRAC.exit(2)

#
# Processing pass needs to start as "/Real Data" for FULL stream flagging
#

if realData not in processing:
  print 'You forgot /Real Data in the processing pass:  ', processing
  DIRAC.exit( 2 )
#
# Make sure it is a known processing pass
#
irun = int( run )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bkClient = BookkeepingClient()
res = bkClient.getRunInformations( irun )
if not res['OK']:
  gLogger.error( 'Cannot load the information for run %s' % ( run ) )
  gLogger.error( res['Message'] )
  DIRAC.exit( 2 )

dtd = res['Value']['DataTakingDescription']
configName = res['Value']['Configuration Name']
configVersion = res['Value']['Configuration Version']

bkDict = {'ConfigName'           : configName,
          'ConfigVersion'        : configVersion,
          'ConditionDescription' : dtd}

if processing != realData:
  knownPasses = GetProcessingPasses( bkDict, '' )
  if processing not in knownPasses:
    gLogger.error( "%s is not a valid processing pass." % ( processing ) )
    DIRAC.exit( 2 )

recoPasses = GetProcessingPasses( bkDict, processing )
if realData in recoPasses:
  recoPasses.remove( realData )

# Flag the run realData first

res = bkClient.setRunAndProcessingPassDataQuality( run, realData, flag )

if not res['OK']:
  print res['Message']
  DIRAC.exit( 2 )
else:
  print 'Run %s RAW files flagged %s' % ( run, flag )

# Now the reconstruction and stripping processing passes
for thisPass in recoPasses:
  res = bkClient.setRunAndProcessingPassDataQuality( run, thisPass, flag )

  if not res['OK']:
    print res['Message']
    DIRAC.exit( 2 )
  else:
    print 'Run %s Processing Pass %s flagged %s' % ( run, thisPass, flag )

DIRAC.exit( 0 )

