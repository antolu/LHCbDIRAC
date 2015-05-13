#!/usr/bin/env python

"""
 Create a new dataset replication or removal transformation according to plugin
"""

__RCSID__ = "$Id$"


if __name__ == "__main__":

  import DIRAC
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.TransformationSystem.Utilities.PluginScript import PluginScript
  import time

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()
  pluginScript.registerFileSwitches()
  test = False
  start = False
  force = False
  invisible = False
  lfcCheck = True
  unique = False
  bkQuery = None
  depth = 0

  Script.registerSwitch( "", "SetInvisible", "Before creating the transformation, set the files in the BKQuery as invisible (default for DeleteDataset)" )
  Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic [False]" )
  Script.registerSwitch( "", "Force", "   Force transformation to be submitted even if no files found" )
  Script.registerSwitch( "", "Test", "   Just print out but not submit" )
  Script.registerSwitch( "", "NoLFCCheck", "   Suppress the check in LFC for removal transformations" )
  Script.registerSwitch( "", "Unique", "   Refuses to create a transformation with an existing name" )
  Script.registerSwitch( "", "Depth=", "   Depth in path for replacing /... in processing pass" )

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
    elif opt == "NoLFCCheck":
      lfcCheck = False
    elif opt == "Unique":
      unique = True
    elif opt == "Depth":
      try:
        depth = int( val )
      except:
        print "Illegal integer depth:", val
        DIRAC.exit( 2 )

  plugin = pluginScript.getOption( 'Plugin' )
  if not plugin:
    print "ERROR: No plugin supplied..."
    Script.showHelp()
    DIRAC.exit( 0 )
  prods = pluginScript.getOption( 'Productions' )
  requestID = pluginScript.getOption( 'RequestID' )
  fileType = pluginScript.getOption( 'FileType' )
  pluginParams = pluginScript.getPluginParameters()
  requestedLFNs = pluginScript.getOption( 'LFNs' )

  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  from LHCbDIRAC.TransformationSystem.Client.Utilities import getRemovalPlugins, getReplicationPlugins

  transType = None
  if plugin in getRemovalPlugins():
    transType = "Removal"
  elif plugin in getReplicationPlugins():
    transType = "Replication"
  else:
    print "This script can only create Removal or Replication plugins"
    print "Replication :", str( getReplicationPlugins() )
    print "Removal     :", str( getRemovalPlugins() )
    print "If needed, ask for adding %s to the known list of plugins" % plugin
    DIRAC.exit( 2 )

  bk = BookkeepingClient()
  tr = TransformationClient()

  if plugin in ( "DestroyDataset", 'DestroyDatasetWhenProcessed' ) or prods:
    # visible = 'All'
    lfcCheck = False

  processingPass = [None]
  if not requestedLFNs:
    bkQuery = pluginScript.getBKQuery()
    transBKQuery = bkQuery.getQueryDict()
    if transBKQuery.keys() in ( [], ['Visible'] ):
      print "No BK query was given..."
      Script.showHelp()
      DIRAC.exit( 2 )
    processingPass = transBKQuery.get( 'ProcessingPass', '' )
    if processingPass.endswith( '...' ):
      import os
      basePass = os.path.dirname( processingPass )
      wildPass = os.path.basename( processingPass ).replace( '...', '' )
      bkQuery.setProcessingPass( basePass )
      processingPasses = bkQuery.getBKProcessingPasses().keys()
      for processingPass in list( processingPasses ):
        if not processingPass.startswith( os.path.join( basePass, wildPass ) ) or processingPass == basePass or ( depth and len( processingPass.replace( basePass, '' ).split( '/' ) ) != ( depth + 1 ) ):
          processingPasses.remove( processingPass )
      if processingPasses:
        processingPasses.sort()
        print "Transformations will be launched for the following list of processing passes:", '\n\t'.join( [''] + processingPasses )
      else:
        print "No processing passes matching the request"
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
      print "**************************************"
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
        print "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] )
        continue
      res = tr.getTransformation( transName + '/' )
      if res['OK']:
        print "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] )
        continue
      res = tr.getTransformation( transName.replace( '-/', '-' ) )
      if res['OK']:
        print "Transformation %s already exists with ID %d" % ( transName, res['Value']['TransformationID'] )
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

    if pluginParams:
      for key, val in pluginParams.items():
        if key.endswith( "SE" ) or key.endswith( "SEs" ):
          res = transformation.setSEParam( key, val )
        else:
          res = transformation.setAdditionalParam( key, val )
        if not res['OK']:
          print res['Message']
          continue

    transformation.setPlugin( plugin )

    if test:
      print "Transformation type:", transType
      print "Transformation Name:", transName
      print "Transformation group:", transGroup
      print "Long description:", longName
      print "Transformation body:", transBody
      if transBKQuery:
        print "BK Query:", transBKQuery
      elif requestedLFNs:
        print "List of%d LFNs" % len( requestedLFNs )
      else:
        # Should not happen here, but who knows ;-)
        print "No BK query provided..."
        Script.showHelp()
        DIRAC.exit( 0 )

    if force:
      lfns = []
    elif transBKQuery:
      print "Executing the BK query:", bkQuery
      startTime = time.time()
      lfns = bkQuery.getLFNs( printSEUsage = ( transType == 'Removal' and not pluginScript.getOption( 'Runs' ) ), printOutput = test )
      bkTime = time.time() - startTime
      nfiles = len( lfns )
      print "Found %d files in %.3f seconds" % ( nfiles, bkTime )
    else:
      lfns = requestedLFNs
    nfiles = len( lfns )

    if test:
      print "Plugin:", plugin
      print "Parameters:", pluginParams
      print "RequestID:", requestID
      continue

    if not force and nfiles == 0:
      print "No files found from BK query %s" % str( bkQuery )
      print "If you anyway want to submit the transformation, use option --Force"
      continue

    # If the transformation is a removal transformation, check all files are in the LFC. If not, remove their replica flag
    if lfcCheck and transType == 'Removal':
      from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
      fc = FileCatalog()
      success = 0
      missingLFNs = []
      startTime = time.time()
      for chunk in breakListIntoChunks( lfns, 500 ):
        res = fc.exists( chunk )
        if res['OK']:
          success += len ( [lfn for lfn in chunk if lfn in res['Value']['Successful'] and  res['Value']['Successful'][lfn]] )
          missingLFNs += [lfn for lfn in chunk if lfn in res['Value']['Failed']] + [lfn for lfn in chunk if lfn in res['Value']['Successful'] and not res['Value']['Successful'][lfn]]
      print "Files checked in LFC in %.3f seconds" % ( time.time() - startTime )
      if missingLFNs:
        print '%d are in the LFC, %d are not. Attempting to remove GotReplica' % ( success, len( missingLFNs ) )
        res = bk.removeFiles( missingLFNs )
        if res['OK']:
          print "Replica flag successfully removed in BK"
      else:
        print 'All files are in the LFC'

    # Prepare the transformation
    if transBKQuery:
      #### !!!! Hack in order to remain compatible with hte BKQuery table...
      if 'Production' in transBKQuery:
        transBKQuery['ProductionID'] = transBKQuery['Production']
        transBKQuery.pop( 'Production' )
      if 'RunNumber' in transBKQuery:
        transBKQuery['RunNumbers'] = transBKQuery['RunNumber']
        transBKQuery.pop( 'RunNumber' )
      transformation.setBkQuery( transBKQuery )

    # If the transformation uses the DeleteDataset plugin, set the files invisible in the BK...
    setInvisiblePlugins = ( "DeleteDataset" )
    if invisible or plugin in setInvisiblePlugins:
      res = bk.setFilesInvisible( lfns )
      if res['OK']:
        print "%d files were successfully set invisible in the BK" % len( lfns )
        if transBKQuery:
          transBKQuery.pop( "Visible", None )
          transformation.setBkQuery( transBKQuery )
      else:
        print "Failed to set the files invisible: %s" % res['Message']
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
        from LHCbDIRAC.TransformationSystem.Client.Utilities import addFilesToTransformation
        res = addFilesToTransformation( transID, requestedLFNs, addRunInfo = ( transType != 'Removal' ) )
        if not res['OK']:
          errMsg = "Could not add %d files to transformation: %s" % ( len( requestedLFNs ), res['Message'] )
          break
        print "%d files successfully added to transformation" % len( res['Value'] )
      if requestID:
        transformation.setTransformationFamily( requestID )
      if start:
        transformation.setStatus( 'Active' )
        transformation.setAgentType( 'Automatic' )
      print "Transformation %d created" % transID
      print "Name:", transName, ", Description:", longName
      print "Transformation body:", transBody
      print "Plugin:", plugin
      if pluginParams:
        print "Additional parameters:", pluginParams
      if requestID:
        print "RequestID:", requestID
      break
    if errMsg:
      print errMsg

  DIRAC.exit( 0 )
