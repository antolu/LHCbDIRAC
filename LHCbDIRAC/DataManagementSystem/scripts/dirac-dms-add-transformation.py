#!/usr/bin/env python

"""
 Create a new dataset replication or removal transformation according to plugin
"""

__RCSID__ = "$Id$"


if __name__ == "__main__":

  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.TransformationSystem.Utilities.PluginScript import PluginScript
  import time, os

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()
  pluginScript.registerFileSwitches()
  test = False
  start = False
  force = False
  invisible = False
  fcCheck = True
  unique = False
  bkQuery = None
  depth = 0
  userGroup = None

  Script.registerSwitch( "", "SetInvisible", "Before creating the transformation, set the files in the BKQuery as invisible (default for DeleteDataset)" )
  Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic [False]" )
  Script.registerSwitch( "", "Force", "   Force transformation to be submitted even if no files found" )
  Script.registerSwitch( "", "Test", "   Just print out but not submit" )
  Script.registerSwitch( "", "NoFCCheck", "   Suppress the check in LFC for removal transformations" )
  Script.registerSwitch( "", "Unique", "   Refuses to create a transformation with an existing name" )
  Script.registerSwitch( "", "Depth=", "   Depth in path for replacing /... in processing pass" )
  Script.registerSwitch( "", "Chown=", "   Give user/group for chown of the directories of files in the FC" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt in ( 's', 'Start' ):
      start = True
    elif opt == 'Test':
      test = True
    elif opt == "Force":
      force = True
    elif opt == "SetInvisible":
      invisible = True
    elif opt == "NoFCCheck":
      fcCheck = False
    elif opt == "Unique":
      unique = True
    elif opt == 'Chown':
      userGroup = val.split( '/' )
      if len( userGroup ) != 2 or not userGroup[1].startswith( 'lhcb_' ):
        gLogger.fatal( "Wrong user/group" )
        DIRAC.exit( 2 )
    elif opt == "Depth":
      try:
        depth = int( val )
      except:
        gLogger.fatal( "Illegal integer depth:", val )
        DIRAC.exit( 2 )

  if userGroup:
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    res = getProxyInfo()
    if not res['OK']:
      gLogger.fatal( "Can't get proxy info", res['Message'] )
      exit( 1 )
    properties = res['Value'].get( 'groupProperties', [] )
    if not 'FileCatalogManagement' in properties:
      gLogger.error( "You need to use a proxy from a group with FileCatalogManagement" )
      exit( 5 )


  plugin = pluginScript.getOption( 'Plugin' )
  if not plugin:
    gLogger.fatal( "ERROR: No plugin supplied..." )
    Script.showHelp()
    DIRAC.exit( 0 )
  prods = pluginScript.getOption( 'Productions' )
  requestID = pluginScript.getOption( 'RequestID' )
  fileType = pluginScript.getOption( 'FileType' )
  pluginParams = pluginScript.getPluginParameters()
  pluginSEParams = pluginScript.getPluginSEParameters()
  requestedLFNs = pluginScript.getOption( 'LFNs' )

  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import getRemovalPlugins, getReplicationPlugins
  from LHCbDIRAC.DataManagementSystem.Utilities.FCUtilities import chown

  transType = None
  if plugin in getRemovalPlugins():
    transType = "Removal"
  elif plugin in getReplicationPlugins():
    transType = "Replication"
  else:
    gLogger.notice( "This script can only create Removal or Replication plugins" )
    gLogger.notice( "Replication :", str( getReplicationPlugins() ) )
    gLogger.notice( "Removal     :", str( getRemovalPlugins() ) )
    gLogger.notice( "If needed, ask for adding %s to the known list of plugins" % plugin )
    DIRAC.exit( 2 )

  bk = BookkeepingClient()
  tr = TransformationClient()

  if plugin in ( "DestroyDataset", 'DestroyDatasetWhenProcessed' ) or prods:
    # visible = 'All'
    fcCheck = False

  processingPass = [None]
  if not requestedLFNs:
    bkQuery = pluginScript.getBKQuery()
    if not bkQuery:
      gLogger.fatal( "No LFNs and no BK query were given..." )
      Script.showHelp()
      DIRAC.exit( 2 )
    transBKQuery = bkQuery.getQueryDict()
    processingPass = transBKQuery.get( 'ProcessingPass', '' )
    if processingPass.endswith( '...' ):
      basePass = os.path.dirname( processingPass )
      wildPass = os.path.basename( processingPass ).replace( '...', '' )
      bkQuery.setProcessingPass( basePass )
      processingPasses = bkQuery.getBKProcessingPasses().keys()
      for processingPass in list( processingPasses ):
        if not processingPass.startswith( os.path.join( basePass, wildPass ) ) or processingPass == basePass or ( depth and len( processingPass.replace( basePass, '' ).split( '/' ) ) != ( depth + 1 ) ):
          processingPasses.remove( processingPass )
      if processingPasses:
        processingPasses.sort()
        gLogger.notice( "Transformations will be launched for the following list of processing passes:" )
        gLogger.notice( '\n\t'.join( [''] + processingPasses ) )
      else:
        gLogger.notice( "No processing passes matching the request" )
        DIRAC.exit( 0 )
    else:
      processingPasses = [processingPass]
  else:
    transBKQuery = {}
    processingPasses = [None]

  reqID = pluginScript.getRequestID()
  if not requestID and reqID:
    requestID = reqID

  transGroup = plugin
  for processingPass in processingPasses:
    if len( processingPasses ) > 1:
      gLogger.notice( "**************************************" )
    # Create the transformation
    transformation = Transformation()
    transformation.setType( transType )
    transName = transType

    # In case there is a loop on processing passes
    try:
      if processingPass:
        bkQuery.setProcessingPass( processingPass )
        transBKQuery['ProcessingPass'] = processingPass
    except:
      pass
    if requestedLFNs:
      longName = transGroup + " for %d LFNs" % len( requestedLFNs )
      transName += '-LFNs'
    elif prods:
      if not fileType:
        fileType = ["All"]
      prodsStr = ','.join( [str( p ) for p in prods] )
      fileStr = ','.join( fileType )
      longName = transGroup + " of " + fileStr + " for productions %s " % prodsStr
      if len( prods ) > 5:
        prodsStr = '%d-productions' % len( prods )
      transName += '-' + fileStr + '-' + prodsStr
    elif 'BKPath' not in pluginScript.getOptions():
      if type( transBKQuery['FileType'] ) == type( [] ):
        strQuery = ','.join( transBKQuery['FileType'] )
      else:
        strQuery = str( transBKQuery['FileType'] )
      longName = transGroup + " for fileType " + strQuery
      transName += '-' + str( transBKQuery['FileType'] )
    else:
      queryPath = bkQuery.getPath()
      if '...' in queryPath:
        queryPath = bkQuery.makePath()
      longName = transGroup + " for BKQuery " + queryPath
      transName += '-' + queryPath

    if requestID:
      transName += '-Request%s' % ( requestID )
    # Check if transformation exists
    if unique:
      res = tr.getTransformation( transName )
      if res['OK']:
        gLogger.notice( "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] ) )
        continue
      res = tr.getTransformation( transName + '/' )
      if res['OK']:
        gLogger.notice( "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] ) )
        continue
      res = tr.getTransformation( transName.replace( '-/', '-' ) )
      if res['OK']:
        gLogger.notice( "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] ) )
        continue
    transformation.setTransformationName( transName )
    transformation.setTransformationGroup( transGroup )
    transformation.setDescription( longName )
    transformation.setLongDescription( longName )
    transformation.setType( transType )
    transBody = None
    if transType == "Removal":
      if plugin == "DestroyDataset":
        transBody = "removal;RemoveFile"
      elif plugin == "DestroyDatasetWhenProcessed":
        plugin = "DeleteReplicasWhenProcessed"
        transBody = "removal;RemoveFile"
        # Set the polling period to 0 if not defined
        pluginParams.setdefault( 'Period', 0 )
      else:
        transBody = "removal;RemoveReplica"
      transformation.setBody( transBody )

    if pluginSEParams:
      for key, val in pluginSEParams.items():
        res = transformation.setSEParam( key, val )
        if not res['OK']:
          gLogger.error( 'Error setting SE parameter', res['Message'] )
          DIRAC.exit( 1 )
    if pluginParams:
      for key, val in pluginParams.items():
        res = transformation.setAdditionalParam( key, val )
        if not res['OK']:
          gLogger.error( 'Error setting additional parameter', res['Message'] )
          DIRAC.exit( 1 )

    transformation.setPlugin( plugin )

    if test:
      gLogger.notice( "Transformation type:", transType )
      gLogger.notice( "Transformation Name:", transName )
      gLogger.notice( "Transformation group:", transGroup )
      gLogger.notice( "Long description:", longName )
      gLogger.notice( "Transformation body:", transBody )
      if transBKQuery:
        gLogger.notice( "BK Query:", transBKQuery )
      elif requestedLFNs:
        gLogger.notice( "List of%d LFNs" % len( requestedLFNs ) )
      else:
        # Should not happen here, but who knows ;-)
        gLogger.error( "No BK query provided..." )
        Script.showHelp()
        DIRAC.exit( 0 )

    if force:
      lfns = []
    elif transBKQuery:
      gLogger.notice( "Executing the BK query:", bkQuery )
      startTime = time.time()
      lfns = bkQuery.getLFNs( printSEUsage = ( transType == 'Removal' and not pluginScript.getOption( 'Runs' ) ), printOutput = test )
      bkTime = time.time() - startTime
      nfiles = len( lfns )
      gLogger.notice( "Found %d files in %.3f seconds" % ( nfiles, bkTime ) )
    else:
      lfns = requestedLFNs
    nfiles = len( lfns )

    if test:
      gLogger.notice( "Plugin:", plugin )
      gLogger.notice( "Parameters:", pluginParams )
      gLogger.notice( "RequestID:", requestID )
      continue

    if not force and nfiles == 0:
      gLogger.notice( "No files found from BK query %s" % str( bkQuery ) )
      gLogger.notice( "If you anyway want to submit the transformation, use option --Force" )
      continue

    if userGroup:
      directories = set( [os.path.dirname( lfn ) for lfn in lfns] )
      res = chown( directories, user = userGroup[0], group = userGroup[1] )
      if not res['OK']:
        gLogger.fatal( "Error changing ownership", res['Message'] )
        DIRAC.exit( 3 )
      gLogger.notice( "Successfully changed owner/group for %d directories" % res['Value'] )
    # If the transformation is a removal transformation, check all files are in the LFC. If not, remove their replica flag
    if fcCheck and transType == 'Removal':
      from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
      fc = FileCatalog()
      success = 0
      missingLFNs = set()
      startTime = time.time()
      for chunk in breakListIntoChunks( lfns, 500 ):
        res = fc.exists( chunk )
        if res['OK']:
          success += res['Value']['Successful'].values().count( True )
          missingLFNs |= res['Value']['Failed']
          missingLFNs |= set( lfn for lfn in res['Value']['Successful'] if not res['Value']['Successful'][lfn] )
        else:
          gLogger.fatal( "Error checking files in the FC", res['Message'] )
          DIRAC.exit( 2 )
      gLogger.notice( "Files checked in LFC in %.3f seconds" % ( time.time() - startTime ) )
      if missingLFNs:
        gLogger.notice( '%d are in the LFC, %d are not. Attempting to remove GotReplica' % ( success, len( missingLFNs ) ) )
        res = bk.removeFiles( list( missingLFNs ) )
        if res['OK']:
          gLogger.notice( "Replica flag successfully removed in BK" )
        else:
          gLogger.fatal( "Error removing BK flag", res['Message'] )
          DIRAC.exit( 2 )
      else:
        gLogger.notice( 'All files are in the LFC' )

    # Prepare the transformation
    if transBKQuery:
      #### !!!! Hack in order to remain compatible with hte BKQuery table...
      for transKey, bkKey in ( ( 'ProductionID', 'Production' ), ( 'RunNumbers', 'RunNumber' ), ( 'DataQualityFlag', 'DataQuality' ) ):
        if bkKey in transBKQuery:
          transBKQuery[transKey] = transBKQuery.pop( bkKey )
      transformation.setBkQuery( transBKQuery )

    # If the transformation uses the RemoveDataset plugin, set the files invisible in the BK...
    setInvisiblePlugins = ( "RemoveDataset", )
    if invisible or plugin in setInvisiblePlugins:
      res = bk.setFilesInvisible( lfns )
      if res['OK']:
        gLogger.notice( "%d files were successfully set invisible in the BK" % len( lfns ) )
        if transBKQuery:
          transBKQuery.pop( "Visible", None )
          transformation.setBkQuery( transBKQuery )
      else:
        gLogger.error( "Failed to set the files invisible:" , res['Message'] )
        continue

    trial = 0
    errMsg = ''
    while True:
      result = transformation.addTransformation()
      if not result['OK']:
        if not unique and result['Message'].find( "already exists" ) >= 0:
          trial += 1
          tName = transName + "-" + str( trial )
          transformation.setTransformationName( tName )
          continue
        else:
          errMsg = "Couldn't create transformation:\n%s" % result['Message']
          break
      result = transformation.getTransformationID()
      if result['OK']:
        transID = result['Value']
      else:
        errMsg = "Error getting transformationID: %s" % res['Message']
        break
      if requestedLFNs:
        from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import addFilesToTransformation
        res = addFilesToTransformation( transID, requestedLFNs, addRunInfo = ( transType != 'Removal' ) )
        if not res['OK']:
          errMsg = "Could not add %d files to transformation: %s" % ( len( requestedLFNs ), res['Message'] )
          break
        gLogger.notice( "%d files successfully added to transformation" % len( res['Value'] ) )
      if requestID:
        transformation.setTransformationFamily( requestID )
      if start:
        transformation.setStatus( 'Active' )
        transformation.setAgentType( 'Automatic' )
      gLogger.notice( "Transformation %d created" % transID )
      gLogger.notice( "Name: %s, Description:%s" % ( transName, longName ) )
      gLogger.notice( "Transformation body:", transBody )
      gLogger.notice( "Plugin:", plugin )
      if pluginParams:
        gLogger.notice( "Additional parameters:", pluginParams )
      if requestID:
        gLogger.notice( "RequestID:", requestID )
      break
    if errMsg:
      gLogger.notice( errMsg )

  DIRAC.exit( 0 )
