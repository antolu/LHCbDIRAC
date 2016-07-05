#! /usr/bin/env python
"""
   Set the destination for a set of runs, based on the majority of reco output
"""
from itertools import count

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, ProgressBar

def execute():
  """
  Parse the options and execute the script
  """
  bkQuery = dmScript.getBKQuery()
  fileType = bkQuery.getFileTypeList()
  if not set( fileType ) & {'FULL.DST', 'RDST'}:
    gLogger.error( "Please provide a reconstruction BK path" )
    DIRAC.exit( 1 )

  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, resolveSEGroup

  bk = BookkeepingClient()
  tr = TransformationClient()
  dm = DataManager()
  dmsHelper = DMSHelpers()

  bkQueryDict = bkQuery.getQueryDict()
  gLogger.notice( "For BK Query:", str( bkQueryDict ) )
  progressBar = ProgressBar( 1, title = "Running BK query...", step = 1 )
  res = bk.getFilesWithMetadata( bkQueryDict )
  if not res['OK']:
    gLogger.error( "Error getting files from BK", res['Message'] )
    DIRAC.exit( 2 )

  if 'ParameterNames' in res.get( 'Value', {} ):
    parameterNames = res['Value']['ParameterNames']
    info = res['Value']['Records']
    progressBar.endLoop( "Obtained %d files" % len( info ) )
  else:
    gLogger.error( '\nNo metadata found' )
    DIRAC.exit( 3 )
  lfns = []
  runLFNs = {}
  for item in info:
    metadata = dict( zip( parameterNames, item ) )
    lfn = metadata['FileName']
    lfns.append( lfn )
    runLFNs.setdefault( metadata['RunNumber'], [] ).append( lfn )

  chunkSize = 1000
  progressBar = ProgressBar( len( lfns ), title = 'Getting replicas of %d files' % len( lfns ), chunk = chunkSize )
  replicas = {}
  errors = {}
  for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
    progressBar.loop()
    res = dm.getReplicas( lfnChunk, getUrl = False )
    if not res['OK']:
      errors.setdefault( res['Message'], [] ).extend( lfnChunk )
    else:
      replicas.update( res['Value']['Successful'] )
      for lfn, error in res['Value']['Failed'].iteritems():
        errors.setdefault( error, [] ).append( lfn )
  progressBar.endLoop()
  for error, lfns in errors.iteritems():
    gLogger.error( error, 'for %d files' % len( lfns ) )

  tier1RDST = set( resolveSEGroup( 'Tier1-RDST' ) )
  setOK = 0
  errors = {}
  progressBar = ProgressBar( len( runLFNs ), title = 'Defining destination for %d runs' % len( runLFNs ), step = 10 )
  for run, lfns in runLFNs.iteritems():
    progressBar.loop()
    # print 'Run', run, len( lfns ), 'Files', lfns[:3]
    seCounts = {}
    for lfn in lfns:
      for se in tier1RDST.intersection( replicas.get( lfn, [] ) ):
        seCounts[se] = seCounts.setdefault( se, 0 ) + 1
    # print seCounts
    maxi = 0
    seMax = None
    for se, count in seCounts.iteritems():
      if count > maxi:
        seMax = se
        maxi = count
    if seMax:
      # SE found, get its site
      res = dmsHelper.getLocalSiteForSE( seMax )
      if res['OK']:
        site = res['Value']
        res = tr.setDestinationForRun( run, site )
        if not res['OK']:
          errors.setdefault( res['Message'], [] ).append( str( run ) )
        else:
          setOK += 1
  progressBar.endLoop( 'Successfully set destination for %d runs' % setOK )
  for error, runs in errors.iteritems():
    gLogger.error( error, 'for runs %s' % ','.join( runs ) )


if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  execute()

