#! /usr/bin/env python

import DIRAC
from DIRAC           import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base import Script
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

import re

################################################################################
#                                                                              #
# GetProcessingPasses:                                                         #
#                                                                              #
# Find all known processing passes for the selected configurations.            #
#                                                                              #
################################################################################
def GetProcessingPasses( bkDict, headPass, passes ):
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
        passes = GetProcessingPasses( bkDict, recoName, passes )

  return passes
################################################################################
#                                                                              #
# Usage:                                                                       #
#                                                                              #
################################################################################
def usage():
  print 'Usage: %s <Processing Pass> <run> <status flag>' % ( Script.scriptName )
################################################################################
#                                                                              #
#                                  >>> Main <<<                                #
#                                                                              #
################################################################################

Script.parseCommandLine()
args = Script.getPositionalArgs()
if len( args ) < 3:
  usage()
  DIRAC.exit( 2 )

exitCode = 0

processing = str( args[0] )
run = args[1]
flag = str( args[2] )

#gLogger.error('Please wait for the OK from Stefan before actually flagging')
#gLogger.error('Do ELOG the flag in the DQ ELOG fr future rerefence.')
#DIRAC.exit(2)

#
# Processing pass needs to start as "/Real Data" for FULL stream flagging
#
m = re.search( '/Real Data', processing )
if not m:
  print 'You forgot /Real Data in the processing pass:  ', processing
  DIRAC.exit( 2 )
#
# Make sure it is a known processing pass
#
irun = int( run )

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

knownPasses = {}
knownPasses = GetProcessingPasses( bkDict, '/Real Data', knownPasses )
if not knownPasses.has_key( processing ):
  gLogger.error( "%s is not a valid processing pass." % ( processing ) )
  DIRAC.exit( 2 )

recoPasses = {}
recoPasses = GetProcessingPasses( bkDict, processing, recoPasses )

# Flag the run '/Real Data' first

res = bkClient.setRunAndProcessingPassDataQuality( run, processing, flag )

if not res['OK']:
  print res['Message']
  DIRAC.exit( 2 )
else:
  print 'Run %s Processing Pass %s flagged %s' % ( str( run ), processing, flag )

# Now the reconstruction and stripping processing passes
for thisPass in recoPasses.keys():
  res = bkClient.setRunAndProcessingPassDataQuality( run, thisPass, flag )

  if not res['OK']:
    print res['Message']
    DIRAC.exit( 2 )
  else:
    print 'Run %s Processing Pass %s flagged %s' % ( str( run ), thisPass, flag )

DIRAC.exit( 0 )

