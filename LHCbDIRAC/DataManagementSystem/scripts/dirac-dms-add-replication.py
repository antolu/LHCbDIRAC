#!/usr/bin/env python

"""
 Create a new replication or removal transformation according to plugin
"""

__RCSID__ = "$Id$"


if __name__ == "__main__":

  import DIRAC
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import PluginScript

  removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )
  replicationPlugins = ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset", "RAWShares", 'FakeReplication' )

  pluginScript = PluginScript( useBKQuery = True )
  pluginScript.registerPluginSwitches()
  test = False
  start = False
  force = False
  invisible = False


  Script.registerSwitch( "", "Invisible", "Before creating the transformation, set the files in the BKQuery as invisible (default for DeleteDataset)" )
  Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic [False]" )
  Script.registerSwitch( "", "Force", "   Force transformation to be submitted even if no files found" )
  Script.registerSwitch( "", "Test", "   Just print out but not submit" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  plugin = pluginScript.getOption( 'Plugin' )
  prods = pluginScript.getOption( 'Productions' )
  requestID = pluginScript.getOption( 'RequestID' )
  fileType = pluginScript.getOption( 'FileType' )
  pluginParams = pluginScript.getOption( 'Parameters', {} )
  for key in pluginScript.options:
      if key.endswith( "SE" ) or key.endswith( "SEs" ):
        pluginParams[key] = pluginScript.options[key]
  nbCopies = pluginScript.getOption( 'Replicas' )
  groupSize = pluginScript.getOption( 'GroupSize' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )


  switches = Script.getUnprocessedSwitches()
  import DIRAC
  for switch in switches:
    opt = switch[0].lower()
    val = switch[1]
    if opt in ( 's', 'start' ):
      start = True
    elif opt == 'test':
      test = True
    elif opt == "force":
      force = True
    elif opt == "invisible":
      invisible = True

  if not plugin:
    print "ERROR: No plugin supplied..."
    Script.showHelp()
    DIRAC.exit( 0 )
  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  transType = None
  if plugin in removalPlugins:
    transType = "Removal"
  elif plugin in replicationPlugins:
    transType = "Replication"
  else:
    print "This script can only create Removal or Replication plugins"
    print "If needed, add %s to the known list of plugins" % plugin
    DIRAC.exit( 2 )

  # Create the transformation
  transformation = Transformation()

  visible = True
  if plugin == "DestroyDataset" or prods:
    visible = False
  bkQuery = pluginScript.getBKQuery( visible = visible )
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
  elif 'BKQuery' not in pluginScript.options:
    longName = transGroup + "for fileType " + str( transBKQuery['FileType'] )
    transName += '-' + str( transBKQuery['FileType'] )
  else:
    query = pluginScript.options['BKQuery']
    longName = transGroup + " for BKQuery " + query
    transName += '-' + query

  if requestID:
    transName += '-Request%d' % ( requestID )
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

  # Add parameters
  if nbCopies != None:
    pluginParams['NumberOfReplicas'] = nbCopies
  if groupSize != None and 'GroupSize' not in pluginParams:
    pluginParams['GroupSize'] = groupSize

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
    lfns = bkQuery.getLFNs( printSEUsage = ( transType == 'Removal' ), visible = visible )
    nfiles = len( lfns )
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

  # If the transformation uses the DeleteDataset plugin, set the files invisible in the BK...
  setInvisiblePlugins = ( "DeleteDataset" )
  if invisible or plugin in setInvisiblePlugins:
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
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
