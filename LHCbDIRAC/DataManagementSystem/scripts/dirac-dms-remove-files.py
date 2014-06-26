#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
  Remove the given file or a list of files from the File Catalog and from the storage
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'SetProcessed', '  Forced to set Removed the files in status Processed (default:not reset)' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine()

  import sys
  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Core.Utilities.List import breakListIntoChunks


  for lfn in Script.getPositionalArgs():
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if bkQuery.getQueryDict().keys() not in ( [''], ['Visible'] ):
    print "Executing BKQuery:", bkQuery
    lfnList += bkQuery.getLFNs()

  fixTrans = True
  setProcessed = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'SetProcessed':
      setProcessed = True

  useRequest = dmScript.getOption( 'Async', False )
  if useRequest:
    print "Using Requests"

    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    from DIRAC.RequestManagementSystem.Client.Request import Request
    from DIRAC.RequestManagementSystem.Client.Operation import Operation
    from DIRAC.RequestManagementSystem.Client.File import File
    from DIRAC.RequestManagementSystem.private.RequestValidator import gRequestValidator
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.ConfigurationSystem.Client.Helpers.Registry  import getDNForUsername

    import hashlib
    import time

    proxyInfo = getProxyInfo()
    if not proxyInfo['OK']:
      print "No proxy available. Please generate a proxy first"
      DIRAC.exit( 1 )

    proxyInfo = proxyInfo['Value']
    username = proxyInfo['username']
    ownerGroup = proxyInfo['group']
    ownerDN = ''
    res = getDNForUsername( username )
    if not res['OK']:
      print "Cannot get DN for user: %s" % res['Message']
      DIRAC.exit( 1 )

    ownerDN = res['Value'][0]
    oRequest = Request()
    requestName = "userscript_%s_%s" % ( username, hashlib.md5( repr( time.time() ) ).hexdigest()[:16] )
    oRequest.RequestName = requestName
    oRequest.SourceComponent = 'dirac-dms-remove-files'
    oRequest.OwnerDN = ownerDN
    oRequest.OwnerGroup = ownerGroup
    chunkSize = 100

    for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):

      oOperation = Operation()
      oOperation.Type = 'RemoveFile'


      for lfn in lfnChunk:
        rarFile = File()
        rarFile.LFN = lfn
        oOperation.addFile( rarFile )

      oRequest.addOperation( oOperation )


    isValid = gRequestValidator.validate( oRequest )
    if not isValid['OK']:
      print "Request is not valid: ", isValid['Message']
      DIRAC.exit( 1 )


    print oRequest.toJSON()


    result = ReqClient().putRequest( oRequest )
    if result['OK']:
      print 'Request %d Submitted' % result['Value']
      print 'You can monitor it using "dirac-rms-show-request %s"' % ( requestName )
    else:
      print 'Failed to submit Request: ', result['Message']



    DIRAC.exit( 0 )

  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  dm = DataManager()
  fc = FileCatalog()

  if fixTrans:
    from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    transClient = TransformationClient()

  errorReasons = {}
  successfullyRemoved = []
  notExisting = []
  # Avoid spurious error messages
  gLogger.setLevel( 'FATAL' )
  chunkSize = 100
  verbose = len( lfnList ) >= 5 * chunkSize
  if verbose:
    sys.stdout.write( "Removing %d files (chunks of %d) " % ( len( lfnList ), chunkSize ) )
  for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
    if verbose:
      sys.stdout.write( '.' )
      sys.stdout.flush()
    res = dm.removeFile( lfnChunk, force = False )
    if not res['OK']:
      gLogger.fatal( "Failed to remove data", res['Message'] )
      DIRAC.exit( -2 )
    for lfn, reason in res['Value']['Failed'].items():
      reasonStr = str( reason )
      if type( reason ) == type( {} ) and reason == {'BookkeepingDB': 'File does not exist'}:
        pass
      elif 'No such file or directory' in reasonStr or 'File does not exist' in reasonStr:
        notExisting.append( lfn )
      else:
        errorReasons.setdefault( reasonStr, [] ).append( lfn )
    successfullyRemoved += res['Value']['Successful'].keys()
  if verbose:
    print ''
  gLogger.setLevel( 'ERROR' )

  if fixTrans and successfullyRemoved + notExisting:
    res = transClient.getTransformationFiles( {'LFN':successfullyRemoved + notExisting } )
    if not res['OK']:
      gLogger.error( "Error getting transformation files", res['Message'] )
    else:
      transFiles = res['Value']
      lfnsToSet = {}
      if setProcessed:
        ignoreStatus = ( 'Removed' )
      else:
        ignoreStatus = ( 'Processed', 'Removed' )
        ignoredFiles = {}
        for fileDict in [fileDict for fileDict in transFiles if fileDict['Status'] == 'Processed']:
          ignoredFiles.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
        if ignoredFiles:
          for transID, lfns in ignoredFiles.items():
            gLogger.always( 'Ignored %d files in status Processed in transformation %d' % ( len( lfns ), transID ) )

      for fileDict in [fileDict for fileDict in transFiles if fileDict['Status'] not in ignoreStatus]:
        lfnsToSet.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
      # If required, set files Removed in transformations
      for transID, lfns in lfnsToSet.items():
        res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = True )
        if not res['OK']:
          gLogger.error( 'Error setting %d files to Removed' % len( lfns ), res['Message'] )
        else:
          gLogger.always( 'Successfully set %d files as Removed in transformation %d' % ( len( lfns ), transID ) )

  if notExisting:
    # The files are not yet removed from the catalog!! :(((
    if verbose:
      sys.stdout.write( "Removing %d non-existing files from FC (chunks of %d) " % ( len( notExisting ), chunkSize ) )
    notExistingRemoved = []
    for lfnChunk in breakListIntoChunks( notExisting, chunkSize ):
      if verbose:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      res = dm.getReplicas( lfnChunk )
      if not res['OK']:
        gLogger.error( "Error getting replicas of %d non-existing files" % len( lfnChunk ), res['Message'] )
        errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
      else:
        replicas = res['Value']['Successful']
        for lfn in replicas:
          for se in replicas[lfn]:
            res = fc.removeReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
            if not res['OK']:
              gLogger.error( 'Error removing replica in the FC for a non-existing file', res['Message'] )
              errorReasons.setdefault( str( res['Message'] ), [] ).append( lfn )
            else:
              for lfn, reason in res['Value']['Failed'].items():
                errorReasons.setdefault( str( reason ), [] ).append( lfn )
                lfnChunk.remove( lfn )
        if lfnChunk:
          res = fc.removeFile( lfnChunk )
          if not res['OK']:
            gLogger.error( "Error removing %d non-existing files from the FC" % len( lfnChunk ), res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
          else:
            for lfn, reason in res['Value']['Failed'].items():
              errorReasons.setdefault( str( reason ), [] ).append( lfn )
              lfnChunk.remove( lfn )
        notExistingRemoved += lfnChunk
    if verbose:
      print ''
    if notExistingRemoved:
      successfullyRemoved += notExistingRemoved
      gLogger.always( "Removed from FC %d non-existing files" % len( notExistingRemoved ) )

  if successfullyRemoved:
    gLogger.always( "Successfully removed %d files" % len( successfullyRemoved ) )
  for reason, lfns in errorReasons.items():
    gLogger.always( "Failed to remove %d files with error: %s" % ( len( lfns ), reason ) )
  DIRAC.exit( 0 )

