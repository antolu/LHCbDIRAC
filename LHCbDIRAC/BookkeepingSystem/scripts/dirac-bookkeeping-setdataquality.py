#! /usr/bin/env python
"""
This script is used to flag a given files which belongs a certain run. For example:
/Real Data 1 OK - the run 1 will be flagged OK
/Real Data/Reco 2 OK  -the files which belongs Reco will be flagged and also the RAW files. 
"""
import DIRAC
from DIRAC           import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base import Script
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

import sys
import re

bkClient = BookkeepingClient()

################################################################################
#                                                                              #
# checkDQFlag:                                                                 #
#                                                                              #
# Make sure the DQ flag is a known one.                                        #
#                                                                              #
################################################################################
def checkDQFlag( dqFlag ):
  res = bkClient.getAvailableDataQuality()
  if not res['OK']:
    return res

  if dqFlag in res['Value']:
    return S_OK()
  else:
    return S_ERROR( "Data quality flag is not in the db" )

################################################################################
#                                                                              #
# flagBadRun:                                                                  #
#                                                                              #
# A run is BAD, flag it and all its files BAD.                                 #
#                                                                              #
################################################################################
def flagBadRun( runNumber, processingpass ):
  """
  This flags everithing which belongs to this run. Real Data, Reco, Stripping, etc...
  """
  procpass = processingpass if processingpass else '/Real Data'
  res = getProcessingPasses( runNumber, procpass )
  if not res['OK']:
    gLogger.error( 'flagBadRun: %s' % ( res['Message'] ) )
    return res
        
  if not processingpass:
    allProcPass = res['Value'] + ['/Real Data']
  else:
    allProcPass = res['Value']
  
  for procName in allProcPass:
    res = bkClient.setRunAndProcessingPassDataQuality( int( runNumber ), procName, 'BAD' )
    if not res['OK']:
      gLogger.error( 'flagBadRun: %s' % ( res['Message'] ) )
      return res
    else:
      gLogger.notice( "Run: %d  Processing pass : %s flagged as BAD" % ( runNumber, procName ) )
      
  return S_OK()
################################################################################
#                                                                              #
# flagFileList:                                                                #
#                                                                              #
# Flag a LFN or a list of LFN contained in a file.                             #
#                                                                              #
################################################################################
def flagFileList( filename, dqFlqg ):
  lfns = []

  #
  # Load the list of LFN
  #
  try:
    with open( filename ) as f:
      for lfn in f:
        lfns.append( lfn.strip() )
  except IOError:
    lfns = filename.split( ',' )

  #
  # Now flag the LFN
  #
    
  res = bkClient.setFileDataQuality( lfns, dqFlag )

  if not res['OK']:
    return res
  else:
    glogger.notice( 'The data quality has been set %s for the following files:' % dqFlag )
    for l in lfns:
      gLogger.notice( l )
            
  return S_OK()

################################################################################
#                                                                              #
# flagRun:                                                                     #
#                                                                              #
# Flag a run given its number, the processing pass and the DQ flag.            #
#                                                                              #
################################################################################
def flagRun( runNumber, procPass, dqFlag ):
   
  res = getProcessingPasses( runNumber, '/Real Data' )
  if not res['OK']:
    return S_ERROR( 'flagRun: %s' % res['Message'] )
          
  allProcPass = res['Value'] + ['/Real Data']

  #
  # Make sure the processing pass entered by the operator is known
  #
  
  gLogger.notice( "Available processing passes:", allProcPass )
  
  for thisPass in procPass:
    if thisPass not in  allProcPass:
      return S_ERROR( '%s is not a valid processing pass.' % thisPass )

  #
  # Add to the list all other processing pass, like stripping, calo-femto..
  #

  allProcPass = procPass
  for proc in procPass:
    res = getProcessingPasses( runNumber, proc )
    if not res['OK']:
      return S_ERROR( 'flagRun: %s' % res['Message'] )        
    allProcPass.extend( res['Value'] )

  
    # Flag the processing passes
  for thisPass in allProcPass:
    res = bkClient.setRunAndProcessingPassDataQuality( runNumber,
                                                      thisPass,
                                                      dqFlag )
    if not res['OK']:
      return S_ERROR( 'flagRun: processing pass %s\n error: %s' % ( thisPass, res['Message'] ) )
    else:
      gLogger.notice( 'Run %d Processing Pass %s flagged %s' % ( runNumber,
                                                                 thisPass,
                                                                 dqFlag ) )
  
  return S_OK()

################################################################################
#                                                                              #
# getProcessingPasses:                                                         #
#                                                                              #
# Find all known processing passes for the selected configurations.            #
#                                                                              #
################################################################################
def getProcessingPasses( runNumber, procPass ):
  res = bkClient.getRunInformations( int( runNumber ) )
  if not res['OK']:
    return S_ERROR( res['Messgage'] )

  cfgName = res['Value']['Configuration Name']
  cfgVersion = res['Value']['Configuration Version']
  dqDesc = res['Value']['DataTakingDescription']
  
  bkDict = {'ConfigName'    : cfgName,
            'ConfigVersion' : cfgVersion,
            'RunNumber'     : runNumber,
            'ConditionDescription': dqDesc}

  passes = []
  res = browseBkkPath( bkDict, procPass, passes )
  if not res['OK']:
    return res
  else:
    return S_OK( passes )
     

def browseBkkPath ( bkDict, processingPass, passes = [] ):
  res = bkClient.getProcessingPass( bkDict, processingPass )
  if not res['OK']:
    gLogger.error( 'Cannot load the processing passes for head % in Version %s Data taking condition %s' % ( processingPass,
                                                                                                             bkDict['ConfigVersion'],
                                                                                                             bkDict['ConditionDescription'] ) )
    gLogger.error( res['Message'] )
    return res

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
        recoName = processingPass + '/' + reco[0]
        passes.append( recoName )  
        browseBkkPath( bkDict, recoName, passes )
    return S_OK()
  
################################################################################
#                                                                              #
#                                  >>> Main <<<                                #
#                                                                              #
################################################################################


Script.registerSwitch( "l:", "lfnfile=", "Flag a LFN or list of LFN" )
Script.registerSwitch( "r:", "run=", "Flag a run" )
Script.registerSwitch( "p:", "processingPass=", "Processing pass for which a run should be flagged" )
Script.registerSwitch( "q:", "dataqualityflag=", "Data quality flag" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    '\nArguments:',
                                    '  lfnfile (string) : [LFN|filename]',
                                    '  processingPass (string) : porcessing pass(es)',
                                    '  runNumber (int): run number to be flagged',
                                    '  dataqualityflag (string): data quality\n', ] ) )



Script.parseCommandLine( ignoreErrors = True )


exitCode = 0

params = {'lfn'    : None,
          'runnumber' : None,
          'dqflag': None,
          'processingpass': None}

for switch in Script.getUnprocessedSwitches():
  if switch[ 0 ].lower() in ( 'l', 'lfnfile' ):
    params[ 'lfn' ] = switch[ 1 ]
  elif switch[ 0 ].lower() in ( 'r', 'run' ):
    params[ 'runnumber' ] = int( switch[ 1 ] )
  elif switch[ 0 ].lower() in ( 'q', 'dataqualityflag' ):
    params[ 'dqflag' ] = switch[ 1 ]
  elif switch[ 0 ].lower() in ( 'p', 'processingPass' ):
    params[ 'processingpass' ] = switch[ 1 ].split( ',' )

if params['lfn'] is None and params['runnumber'] is None:
  gLogger.fatal( 'Please specify run number or an lfn!' )
  DIRAC.exit( 1 )
if params['dqflag'] is None:
  gLogger.fatal( 'Please specify the data quality flag!' )
  DIRAC.exit( 1 )

if params['runnumber'] and params[ 'processingpass' ] is None and params['dqflag'] != 'BAD' :
  gLogger.fatal( "Please specify the processing pass!" )
  DIRAC.exit( 1 )
  
res = checkDQFlag( params['dqflag'] )
if not res['OK']:
  gLogger.fatal( "%s - %s" % ( params['dqflag'], res['Message'] ) )
  DIRAC.exit( 1 )

if params['lfn']:
  res = flagFileList( params['lfn'], params['dqflag'] )
  if res['OK']:
    gLogger.notice( "Files are flagged!" )
    DIRAC.exit( exitCode )
  else:
    gLogger.fatal( res['Message'] )

if params['runnumber']:
  if params['dqflag'] == 'BAD':
    res = flagBadRun( params['runnumber'], params['processingpass'].pop() )
    if not res['OK']:
      gLogger.fatal( res['Message'] )
      DIRAC.exit( 1 )
  else:
    res = flagRun( params['runnumber'], params['processingpass'], params['dqflag'] )
    if not res['OK']:
      gLogger.fatal( res['Message'] )
      DIRAC.exit( 1 )              
    
DIRAC.exit( exitCode )
