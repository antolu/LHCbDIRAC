#!/usr/bin/env python

"""
 Test a plugin
"""

__RCSID__ = "$Id: $"

class fakeClient:
  def __init__( self, trans, transID, transBKQuery ):
    self.trans = trans
    self.transID = transID
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    self.transClient = TransformationClient()

    ( self.files, self.replicas ) = self.prepareForPlugin( transBKQuery )

  def getReplicas( self ):
    return self.replicas

  def getFiles( self ):
    return self.files

  def getBookkeepingQueryForTransformation ( self, transID ):
    return self.trans.getBkQuery()

  def getTransformationRuns( self, condDict ) :
    transRuns = []
    if condDict.has_key( 'RunNumber' ):
      runs = condDict['RunNumber']
      for run in runs:
        transRuns.append( {'RunNumber':run, 'Status':"Active", "SelectedSite":None} )
    return DIRAC.S_OK( transRuns )

  def getTransformationFiles( self, condDict = None ):
    if condDict['TransformationID'] == self.transID:
      transFiles = []
      if condDict.has_key( 'Status' ) and not 'Unused' in condDict['Status']:
        return DIRAC.S_OK( transFiles )
      if condDict.has_key( 'RunNumber' ):
        runs = condDict['RunNumber']
        for file in self.files:
          if file['RunNumber'] in runs:
            transFiles.append( {'LFN':file['LFN'], 'Status':'Unused'} )
        return DIRAC.S_OK( transFiles )
    else:
      return self.transClient.getTransformationFiles( condDict = condDict )

  def setTransformationRunStatus( self, transID, runID, status ):
    return DIRAC.S_OK()

  def setTransformationRunsSite( self, transID, runID, site ):
    return DIRAC.S_OK()

  def setFileStatusForTransformation( self, transID, status, lfns ):
    return DIRAC.S_OK()

  def prepareForPlugin( self, transBKQuery ):

    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    bk = BookkeepingClient()
    type = self.trans.getType()['Value']
    lfns = testBKQuery( transBKQuery, type )
    if not lfns:
      return ( None, None )
    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
    rm = ReplicaManager()
    res = bk.getFileMetadata( lfns )
    if res['OK']:
      files = []
      for lfn, metadata in res['Value'].items():
        runID = metadata.get( 'RunNumber', 0 )
        runDict = {"RunNumber":runID, "LFN":lfn}
        files.append( runDict )
    replicas = {}
    for lfnChunk in breakListIntoChunks( lfns, 200 ):
      if type.lower() in ( "replication", "removal" ):
        res = rm.getReplicas( lfnChunk )
      else:
        res = rm.getActiveReplicas( lfnChunk )
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
    return ( files, replicas )

if __name__ == "__main__":

  from DIRAC.Core.Base import Script

  plugin = None
  removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )
  replicationPlugins = ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset" )
  parameterSEs = ( "KeepSEs", "Archive1SEs", "Archive2SEs", "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs" )
  groupSize = 5

  Script.registerSwitch( "", "Plugin=", "   Plugin name (mandatory)" )
  Script.registerSwitch( "t:", "Type=", "   Transformation type [Replication] (Removal automatic)" )
  Script.registerSwitch( "P:", "Production=", "   Production ID to search (comma separated list)" )
  Script.registerSwitch( "f:", "FileType=", "   File type (to be used with --Prod) [All]" )
  Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path" )
  Script.registerSwitch( "r:", "Run=", "   Run or range of runs (r1:r2)" )

  Script.registerSwitch( "", "NumberOfReplicas=", "   Number of copies to create or to remove" )
  Script.registerSwitch( "k:", "KeepSEs=", "   List of SEs where to keep replicas (for plugins %s)" % str( removalPlugins ) )
  for param in parameterSEs[1:]:
    Script.registerSwitch( "", param + '=', "   List of SEs for the corresponding parameter of the plugin" )
  Script.registerSwitch( "g:", "GroupSize=", "   GroupSize parameter for merging (GB) [%d]" % groupSize )
  Script.registerSwitch( "", "Parameters=", "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  prods = None
  fileType = None
  bkQuery = None
  requestID = 0
  pluginParams = {}
  nbCopies = None
  runs = None

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
    elif opt in ( 'p', 'plugin' ):
      plugin = val
    elif opt == "numberofreplicas":
      try:
        nbCopies = int( val )
      except:
        print "--NumberOfReplicas must be an integer"
        DIRAC.exit( 2 )
    elif opt == 'parameters':
      try:
        pluginParams = eval( val )
      except:
        print "Error parsing parameters: ", val
        DIRAC.exit( 2 )
    elif opt == "type":
      transType = val
    elif opt in ( 'g', 'groupsize' ):
      val = float( val )
      if float( int( val ) ) == val:
        groupSize = int( val )
      else:
        groupSize = val
    elif opt in ( 'r', 'run' ):
      runs = val.split( ':' )
      if len( runs ) == 1:
        runs[1] = runs[0]
    elif opt in ( 't', 'type' ):
      transType = val

  if not plugin:
    print "Plugin is a mandatory argument..."
    Script.showHelp()
    DIRAC.exit( 0 )

  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import buildBKQuery, testBKQuery
  # Create the transformation
  transformation = Transformation()

  ( transBKQuery, reqID ) = buildBKQuery( bkQuery, prods, fileType, runs )
  if not transBKQuery:
    Script.showhelp()
    DIRAC.exit( 2 )

  if not requestID and reqID:
    requestID = reqID

  transType = None
  if plugin in removalPlugins:
    transType = "Removal"
  elif plugin in replicationPlugins:
    transType = "Replication"
  else:
    transType = "Processing"
  transformation.setType( transType )

  # Add parameters
  if nbCopies != None:
    pluginParams['NumberOfReplicas'] = nbCopies

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

  print "Transformation type:", transType
  print "BK Query:", transBKQuery
  print "Plugin:", plugin
  print "Parameters:", pluginParams
  if requestID:
    print "RequestID:", requestID
  print "\nNow testing the plugin %s" % plugin

  from DIRAC.Core.Utilities.List                                         import sortList
  from LHCbDIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin
  transID = -9999
  pluginParams['TransformationID'] = transID
  pluginParams['Status'] = "Active"
  if not pluginParams.has_key( "GroupSize" ):
    pluginParams['GroupSize'] = groupSize
  fakeClient = fakeClient( transformation, transID, transBKQuery )
  oplugin = TransformationPlugin( plugin, transClient = fakeClient )
  oplugin.setParameters( pluginParams )
  replicas = fakeClient.getReplicas()
  files = fakeClient.getFiles()
  if not replicas:
    print "No replicas were found, exit..."
    DIRAC.exit( 2 )
  oplugin.setInputData( replicas )
  oplugin.setTransformationFiles( files )
  res = oplugin.generateTasks()
  print ""
  if res['OK']:
    print len( res['Value'] ), "tasks created"
    i = 0
    for task in res['Value']:
      i += 1
      location = []
      for lfn in task[1]:
        l = ','.join( sortList( replicas[lfn].keys() ) )
        if not l in location:
          location.append( l )
      targets = task[0].split( ',' )
      print i, '- Target SEs:', task[0], "- %d files" % len( task[1] ), " - Current locations:", location
      if transType == "Removal":
        remain = []
        for l in location:
          r = ','.join( [se for se in l.split( ',' ) if not se in targets] )
          remain.append( r )
        print "    Remaining SEs:", remain
      if transType == "Replication":
        total = []
        for l in location:
          r = l + ',' + ','.join( [se for se in targets if not se in l.split( ',' )] )
          total.append( r )
        print "    Final SEs:", total
  else:
    print res['Message']
  DIRAC.exit( 0 )

