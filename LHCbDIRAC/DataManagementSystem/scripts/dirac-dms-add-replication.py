#!/usr/bin/env python

"""
 Create a new replication or removal transformation according to plugin
"""

__RCSID__ = "$Id:  $"


if __name__ == "__main__":

  from DIRAC.Core.Base import Script

  plugin = None
  test = False
  groupSize = 5
  removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )
  replicationPlugins = ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset" )
  parameterSEs = ( "KeepSEs", "Archive1SEs", "Archive2SEs", "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs" )

  Script.registerSwitch( "", "Plugin=", "   Plugin name (mandatory)" )
  Script.registerSwitch( "P:", "Production=", "   Production ID to search (comma separated list)" )
  Script.registerSwitch( "f:", "FileType=", "   File type (to be used with --Prod) [All]" )
  Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path" )
  Script.registerSwitch( "r:", "Run=", "   Run or range of runs (r1:r2)" )

  Script.registerSwitch( "", "SEs=", "   List of SEs (dest for replication, source for removal)" )
  Script.registerSwitch( "", "Copies=", "   Number of copies in the list of SEs" )
  Script.registerSwitch( "", "Parameters=", "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}" )
  Script.registerSwitch( "k:", "KeepSEs=", "   List of SEs where to keep replicas (for plugins %s)" % str( removalPlugins ) )
  for param in parameterSEs[1:]:
    Script.registerSwitch( "", param + '=', "   List of SEs for the corresponding parameter of the plugin" )
  Script.registerSwitch( "g:", "GroupSize=", "   GroupSize parameter for merging (GB) [%d]" % groupSize )

  Script.registerSwitch( "R:", "Request=", "   Assign request number [<id of query production>]" )
  Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic [False]" )
  Script.registerSwitch( "", "Force", "   Force transformation to be submitted even if no files found" )
  Script.registerSwitch( "", "Test", "   Just print out but not submit" )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  prods = None
  fileType = None
  bkQuery = None
  requestID = 0
  start = False
  pluginParams = {}
  listSEs = None
  nbCopies = None
  keepSEs = None
  runs = None
  force = False

  switches = Script.getUnprocessedSwitches()
  import DIRAC
  for switch in switches:
    opt = switch[0].lower()
    val = switch[1]
    for param in parameterSEs:
      if opt == param.lower():
        if val.lower() == 'none':
          val = []
        else:
          val = val.split( ',' )
        pluginParams[param] = val
    if opt in ( "p", "production" ):
      prods = []
      for prod in val.split( ',' ):
        if prod.find( ':' ) > 0:
          pLimits = prod.split( ':' )
          for p in range( int( pLimits[0] ), int( pLimits[1] ) + 1 ):
            prods.append( str( p ) )
        else:
          prods.append( prod )
    elif opt in ( "f", "filetype" ):
      fileType = val.split( ',' )
    elif opt in ( "b", "bkquery" ):
      bkQuery = val
    elif opt in ( 'r', 'request' ):
      try:
        requestID = int( val )
      except:
        print "--Request requires an integer"
        DIRAC.exit( 2 )
    elif opt in ( 'p', 'plugin' ):
      plugin = val
    elif opt in ( 's', 'start' ):
      start = True
    elif opt == "ses":
      listSEs = val.split( ',' )
    elif opt == "copies":
      try:
        nbCopies = int( val )
      except:
        print "--Copies must be an integer"
        DIRAC.exit( 2 )
    elif opt == 'test':
      test = True
    elif opt == 'parameters':
      try:
        pluginParams = eval( val )
      except:
        print "Error parsing parameters: ", val
        DIRAC.exit( 2 )
    elif opt in ( 'g', 'groupsize' ):
      if float( int( val ) ) == int( val ):
        groupSize = int( val )
      else:
        groupSize = float( val )
    elif opt in ( 'r', 'run' ):
      runs = val.split( ':' )
      if len( runs ) == 1:
        runs[1] = runs[0]
    elif opt == "force":
      force == True

  if not plugin:
    print "ERROR: No plugin supplied..."
    Script.showHelp()
    DIRAC.exit( 0 )
  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import buildBKQuery, testBKQuery

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

  ( transBKQuery, reqID ) = buildBKQuery( bkQuery, prods, fileType, runs )
  if not transBKQuery:
    Script.showhelp()
    DIRAC.exit( 2 )

  if not requestID and reqID:
    requestID = reqID

  transformation.setType( transType )

  # Add parameters
  if nbCopies != None:
    pluginParams['NumberOfReplicas'] = nbCopies

  transGroup = plugin
  transName = transType

  if prods:
    if not fileType:
      fileType = ["All"]
    longName = transGroup + " of " + ','.join( fileType ) + " for production%s " % s + ','.join( prods )
    transName += '-' + '/'.join( fileType ) + '-' + '/'.join( prods )
  else:
    longName = transGroup + " for BKQuery " + bkQuery
    transName += '-' + bkQuery

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
    lfns = testBKQuery( transBKQuery, transType )
    nfiles = len( lfns )
  else:
    print "No NK query provided..."
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
      #print "Transformation %d created" % result['Value']
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
