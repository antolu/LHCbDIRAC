#!/usr/bin/env python

"""
 Create a new replication or removal transformation according to plugin
"""

__RCSID__ = "$Id$"


if __name__ == "__main__":

  import DIRAC
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import PluginScript
  import time

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()
  test = False
  start = False
  force = False
  invisible = False
  lfcCheck = True

  Script.registerSwitch( "", "SetInvisible", "Before creating the transformation, set the files in the BKQuery as invisible (default for DeleteDataset)" )
  Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic [False]" )
  Script.registerSwitch( "", "Force", "   Force transformation to be submitted even if no files found" )
  Script.registerSwitch( "", "Test", "   Just print out but not submit" )
  Script.registerSwitch( "", "NoLFCCheck", "   Suppress the check in LFC for removal transformations" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors=True )

  plugin = pluginScript.getOption( 'Plugin' )
  prods = pluginScript.getOption( 'Productions' )
  requestID = pluginScript.getOption( 'RequestID' )
  fileType = pluginScript.getOption( 'FileType' )
  pluginParams = pluginScript.getPluginParameters()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    opt = switch[0].lower()
    val = switch[1]
    if opt in ( 's', 'start' ):
      start = True
    elif opt == 'test':
      test = True
    elif opt == "force":
      force = True
    elif opt == "setinvisible":
      invisible = True
    elif opt == "nolfccheck":
      lfcCheck = False

  if not plugin:
    print "ERROR: No plugin supplied..."
    Script.showHelp()
    DIRAC.exit( 0 )
  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

  transType = None
  if plugin in pluginScript.getRemovalPlugins():
    transType = "Removal"
  elif plugin in pluginScript.getReplicationPlugins():
    transType = "Replication"
  else:
    print "This script can only create Removal or Replication plugins"
    print "Replication :", str( replicationPlugins )
    print "Removal     :", str( removalPlugins )
    print "If needed, ask for adding %s to the known list of plugins" % plugin
    DIRAC.exit( 2 )

  # Create the transformation
  transformation = Transformation()

  visible = True
  if plugin == "DestroyDataset" or prods:
    visible = False
  bkQuery = pluginScript.getBKQuery( visible=visible )
  transBKQuery = bkQuery.getQueryDict()
  if not transBKQuery:
    print "No BK query was given..."
    Script.showHelp()
    DIRAC.exit( 2 )

  reqID = pluginScript.getRequestID()
  if not requestID and reqID:
    requestID = reqID

  transformation.setType( transType )

  transGroup = plugin
  transName = transType
  if prods:
    if not fileType:
      fileType = ["All"]
    prodsStr = ','.join( [str( p ) for p in prods] )
    fileStr = ','.join( fileType )
    longName = transGroup + " of " + fileStr + " for productions %s " % prodsStr
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
    longName = transGroup + " for BKQuery " + queryPath
    transName += '-' + queryPath

  if requestID:
    transName += '-Request%s' % ( requestID )
  transformation.setTransformationName( transName )
  transformation.setTransformationGroup( transGroup )
  transformation.setDescription( longName )
  transformation.setLongDescription( longName )
  transformation.setType( transType )
  transBody = None
  if transType == "Removal":
    if plugin == "DestroyDataset":
      transBody = "removal;removeFile"
    else:
      transBody = "removal;replicaRemoval"
    transformation.setBody( transBody )

  if pluginParams:
    for key, val in pluginParams.items():
      if key.endswith( "SE" ) or key.endswith( "SEs" ):
        res = transformation.setSEParam( key, val )
      else:
        res = transformation.setAdditionalParam( key, val )
      if not res['OK']:
        print res['Message']
        DIRAC.exit( 2 )

  transformation.setPlugin( plugin )

  transformation.setBkQuery( transBKQuery )

  if test:
    print "Transformation type:", transType
    print "Transformation Name:", transName
    print "Transformation group:", transGroup
    print "Long description:", longName
    print "Transformation body:", transBody
    print "BK Query:", transBKQuery

  if transBKQuery:
    print "Executing the BK query..."
    startTime = time.time()
    lfns = bkQuery.getLFNs( printSEUsage=( transType == 'Removal' and not pluginScript.getOption( 'Runs' ) ), visible=visible )
    bkTime = time.time() - startTime
    nfiles = len( lfns )
    print "Found %d files in %.3f seconds" % ( nfiles, bkTime )
  else:
    print "No BK query provided..."
    Script.showHelp()
    DIRAC.exit( 0 )

  if test:
    print "Plugin:", plugin
    print "Parameters:", pluginParams
    print "RequestID:", requestID
    DIRAC.exit( 0 )

  if not force and nfiles == 0:
    print "No files found from BK query"
    print "If you anyway want to submit the transformation, use option --Force"
    DIRAC.exit( 0 )

  # If the transformation is a removal transformation, check all files are in the LFC. If not, remove their replica flag
  if lfcCheck and transType == 'Removal':
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
    rm = ReplicaManager()
    bk = BookkeepingClient()
    success = 0
    missingLFNs = []
    startTime = time.time()
    for chunk in breakListIntoChunks( lfns, 500 ):
      res = rm.getCatalogExists( chunk )
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

  # If the transformation uses the DeleteDataset plugin, set the files invisible in the BK...
  setInvisiblePlugins = ( "DeleteDataset" )
  if invisible or plugin in setInvisiblePlugins:
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
    res = BookkeepingClient().setFilesInvisible( lfns )
    if res['OK']:
      print "%d files were successfully set invisible in the BK" % len( lfns )
      transBKQuery.pop( "Visible" )
      transformation.setBkQuery( transBKQuery )
    else:
      print "Failed to set the files invisible: %s" % res['Message']
      DIRAC.exit( 2 )

  trial = 0
  while True:
    result = transformation.addTransformation()
    if not result['OK']:
      print "Couldn't create transformation:"
      print result['Message']
      if result['Message'].find( "already exists" ) >= 0:
        trial += 1
        tName = transName + "-" + str( trial )
        transformation.setTransformationName( tName )
        print "Retrying with name:", tName
        continue
      else:
        DIRAC.exit( 2 )
    if requestID:
      transformation.setTransformationFamily( requestID )
    if start:
      transformation.setStatus( 'Active' )
      transformation.setAgentType( 'Automatic' )
    result = transformation.getTransformationID()
    if result['OK']:
      print "Transformation %d created" % result['Value']
      print "Name:", transName, ", Description:", longName
      print "Transformation body:", transBody
      print "Plugin:", plugin
      if pluginParams:
        print "Additional parameters:", pluginParams
      print "RequestID:", requestID
      DIRAC.exit( 0 )
    else:
      print result['Message']
      DIRAC.exit( 2 )
