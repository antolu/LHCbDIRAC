#! /usr/bin/env python
"""
   Fix the luminosity of all descendants of a set of RAW files, if hte run is Finished
"""

__RCSID__ = "$Id: dirac-bookkeeping-get-stats.py 69357 2013-08-08 13:33:31Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def updateDescendantsLumi( parentLumi, doIt = False ):
  if not parentLumi:
    return
  # Get descendants:
  res = bk.getFileDescendants( parentLumi.keys(), depth = 1, checkreplica = False )
  if not res['OK']:
    gLogger.error( 'Error getting descendants', res['Message'] )
    return
  success = res['Value']['WithMetadata']
  descLumi = {}
  fileTypes = {}
  fileLumi = {}
  for lfn in success:
    for desc in success[lfn]:
      fileType = success[lfn][desc]['FileType']
      if fileType not in ( 'LOG', ):
        descLumi.setdefault( desc, 0. )
        descLumi[desc] += parentLumi[lfn]
        fileTypes[desc] = fileType
        fileLumi[desc] = success[lfn][desc]['Luminosity']
  if not descLumi:
    return

  prStr = 'Updating' if doIt else 'Would update'
  nDesc = len( descLumi )
  for lfn in fileLumi:
    if abs( fileLumi[lfn] - descLumi[lfn] ) < 1:
      descLumi.pop( lfn, None )
  if descLumi:
    gLogger.notice( '%s lumi of %d descendants out of %d (file types: %s) of %d files' % ( prStr, len( descLumi ), nDesc, ','.join( sorted( set( fileTypes.values() ) ) ), len( parentLumi ) ) )
  else:
    gLogger.notice( 'All %d descendants (file types: %s) of %d files are OK' % ( nDesc, ','.join( sorted( set( fileTypes.values() ) ) ), len( parentLumi ) ) )
    return
  if doIt:
    for desc in descLumi:
      res = bk.updateFileMetaData( desc, {'Luminosity':descLumi[desc]} )
      if not res['OK']:
        gLogger.error( 'Error setting Luminosity', res['Message'] )
  # Ignore now histogram files
  for desc in fileTypes:
    if 'HIST' in fileTypes[desc]:
      descLumi.pop( desc, None )
  updateDescendantsLumi( descLumi, doIt = doIt )

def updateRunLumi( run, evtType, fileInfo, doIt = False ):
  """
  Updates the files luminosity from the run nformation and the files statistics
  run : run number (int)
  evtType: event type (int)
  fileInfo: list of tuples containing run files information for that event type [(lfn, nbevts, lumi), ...]
  """
  res = bk.getRunInformations( run )
  if not res['OK']:
    gLogger.error( 'Error from BK getting run information', res['Message'] )
    return
  info = res['Value']
  runLumi = info['TotalLuminosity']
  runEvts = dict( zip( info['Stream'], info['Number of events'] ) )[evtType]
  filesLumi = sum( [lumi for _lfn, _evts, lumi in fileInfo] )
  # Check luminosity
  if abs( runLumi - filesLumi ) > 1:
    prStr = 'Updating' if doIt else 'Would update'
    gLogger.notice( "%s %d files as run %d and files lumi don't match: runLumi %d, filesLumi %d" % ( prStr, run, len( fileInfo ), runLumi, filesLumi ) )
    for info in fileInfo:
      # Split the luminosity according to nb of events
      info[2] = float( runLumi ) * info[1] / runEvts
      if doIt:
        res = bk.updateFileMetaData( info[0], {'Luminosity':info[2]} )
        if not res['OK']:
          gLogger.error( 'Error setting Luminosity', res['Message'] )
  else:
    gLogger.notice( 'Run %d: %d RAW files are OK' % ( run, len( fileInfo ) ) )

  # Now update descendants
  fileLumi = dict( [( lfn, lumi ) for lfn, _evts, lumi in fileInfo] )
  updateDescendantsLumi( fileLumi, doIt = doIt )

def execute():

  doIt = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'DoIt':
      doIt = True
    pass

  bkQuery = dmScript.getBKQuery()
  if not bkQuery:
    print "No BK query given..."
    DIRAC.exit( 1 )

  fileType = bkQuery.getFileTypeList()
  if fileType != ['RAW']:
    gLogger.notice( 'This script only works from RAW files' )
    DIRAC.exit( 2 )

  bkQueryDict = bkQuery.getQueryDict()
  evtTypes = bkQuery.getEventTypeList()

  runChecked = False
  for evtType in evtTypes:
    gLogger.notice( '**** Event type %s' % evtType )
    bkQueryDict['EventType'] = evtType
    res = bk.getFilesWithMetadata( bkQueryDict )
    if not res['OK']:
      gLogger.fatal( 'Error getting BK files', res['Message'] )
    parameterNames = res['Value']['ParameterNames']
    info = res['Value']['Records']
    runFiles = {}
    for item in info:
      metadata = dict( zip( parameterNames, item ) )
      run = metadata['RunNumber']
      lfn = metadata['FileName']
      lumi = metadata['Luminosity']
      evts = metadata['EventStat']
      runFiles.setdefault( run, [] ).append( [lfn, evts, lumi] )
    if not runChecked:
      res = bk.getRunStatus( runFiles.keys() )
      if not res['OK']:
        gLogger.fatal( 'Error getting run status', res['Message'] )
        DIRAC.exit( 3 )
      runNotFinished = sorted( [str( run ) for run in res['Value']['Successful'] if res['Value']['Successful'][run]['Finished'] != 'Y'] )
      if runNotFinished:
        gLogger.notice( 'Found %d runs that are not Finished: %s' % ( len( runNotFinished ), ','.join( runNotFinished ) ) )
      runFinished = sorted( [run for run in res['Value']['Successful'] if res['Value']['Successful'][run]['Finished'] == 'Y'] )
      if runFinished:
        gLogger.notice( 'Found %d runs that are Finished' % len( runFinished ) )
      else:
        gLogger.notice( 'No Finished run found' )
        DIRAC.exit( 0 )
    for run in runFinished:
      updateRunLumi( run, int( evtType ), runFiles[run], doIt = doIt )

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.registerSwitch( '', 'DoIt', '   Fix the BK database (default No)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()

  execute()

