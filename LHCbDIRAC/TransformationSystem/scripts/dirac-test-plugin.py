#!/usr/bin/env python

"""
 Test a plugin
"""

__RCSID__ = "$Id$"

class fakeClient:
  def __init__( self, trans, transID, lfns, aIfProd ):
    self.trans = trans
    self.transID = transID
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    self.transClient = TransformationClient()
    self.asIfProd = asIfProd

    ( self.files, self.replicas ) = self.prepareForPlugin( lfns )

  def getReplicas( self ):
    return self.replicas

  def getFiles( self ):
    return self.files

  def getCounters( self, table, attrList, condDict ):
    if condDict['TransformationID'] == self.transID:
      condDict['TransformationID'] = self.asIfProd
      return self.transClient.getCounters( table, attrList, condDict )
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'SARA-RAW', 'PIC-RAW', 'RAL-RAW']
    counters = []
    for se in possibleTargets:
      counters.append( ( {'UsedSE':se}, 1 ) )
    return DIRAC.S_OK( counters )

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
      if 'Status' in condDict and 'Unused' not in condDict['Status']:
        return DIRAC.S_OK( transFiles )
      runs = None
      if 'RunNumber' in condDict:
        runs = condDict['RunNumber']
        if type( runs ) != type( [] ):
          runs = [runs]
      for file in self.files:
        if not runs or file['RunNumber'] in runs:
          transFiles.append( {'LFN':file['LFN'], 'Status':'Unused'} )
      return DIRAC.S_OK( transFiles )
    else:
      return self.transClient.getTransformationFiles( condDict = condDict )

  def getTransformationFilesCount( self, transID, field, selection = None ):
    if transID == self.transID or selection['TransformationID'] == self.transID:
      if field != 'Status':
        return DIRAC.S_ERROR( 'Not implemented for field ' + field )
      runs = None
      if 'RunNumber' in selection:
        runs = selection['RunNumber']
        if type( runs ) != type( [] ):
          runs = [runs]
      counters = {'Unused':0}
      for file in self.files:
        if not runs or file['RunNumber'] in runs:
          counters['Unused'] += 1
      counters['Total'] = counters['Unused']
      return DIRAC.S_OK( counters )
    else:
      return self.transClient.getTransformationFilesCount( transID, field, selection = selection )

  def getTransformationRunStats( self, transIDs ):
    counters = {}
    for transID in transIDs:
      if transID == self.transID:
        for file in self.files:
          runID = file['RunNumber']
          counters[transID][runID]['Unused'] = counters.setdefault( transID, {} ).setdefault( runID, {} ).setdefault( 'Unused', 0 ) + 1
        for runID in counters[transID]:
          counters[transID][runID]['Total'] = counters[transID][runID]['Unused']
      else:
        res = self.transClient.getTransformationRunStats( transIDs )
        if res['OK']:
          counters.update( res['Value'] )
        else:
          return res
    return DIRAC.S_OK( counters )

  def setTransformationRunStatus( self, transID, runID, status ):
    return DIRAC.S_OK()

  def setTransformationRunsSite( self, transID, runID, site ):
    return DIRAC.S_OK()

  def setFileStatusForTransformation( self, transID, status, lfns ):
    return DIRAC.S_OK()

  def prepareForPlugin( self, lfns ):

    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    bk = BookkeepingClient()
    type = self.trans.getType()['Value']
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
      #print lfnChunk
      if type.lower() in ( "replication", "removal" ):
        res = rm.getReplicas( lfnChunk )
      else:
        res = rm.getActiveReplicas( lfnChunk )
      #print res
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
    return ( files, replicas )

if __name__ == "__main__":

  def printFinalSEs( transType, location, targets ):
    targets = targets.split( ',' )
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

  import DIRAC
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.TransformationSystem.Client.Utilities   import PluginScript

  removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )
  replicationPlugins = ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset" )

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()

  Script.registerSwitch( '', 'AsIfProduction=', '   Production # that this test using as source of information' )
  Script.registerSwitch( '', 'AllFiles', '   Sets visible = False (useful if files were marked invisible)' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  asIfProd = None
  allFiles = False
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'AsIfProduction':
      asIfProd = int( val )
    elif opt == 'AllFiles':
      allFiles = True
  #print pluginScript.options
  plugin = pluginScript.options.get( 'Plugin' )
  requestID = pluginScript.options.get( 'RequestID', 0 )
  pluginParams = pluginScript.options.get( 'Parameters', {} )
  for key in pluginScript.options:
      if key.endswith( "SE" ) or key.endswith( "SEs" ):
        pluginParams[key] = pluginScript.options[key]
  nbCopies = pluginScript.options.get( 'Replicas' )
  groupSize = pluginScript.options.get( 'GroupSize' )

  if not plugin:
    print "Plugin is a mandatory argument..."
    Script.showHelp()
    DIRAC.exit( 0 )

  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  # Create the transformation
  transformation = Transformation()

  visible = True
  if allFiles or plugin == "DestroyDataset" or pluginScript.options.get( 'Productions' ) or plugin not in removalPlugins + replicationPlugins:
    visible = False
  transBKQuery = pluginScript.buildBKQuery( visible = visible )
  if not transBKQuery:
    print "No BK query was given..."
    Script.showHelp()
    DIRAC.exit( 2 )
  reqID = pluginScript.getRequestID()

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
  # Create a fake transformation client
  lfns = pluginScript.getLFNsForBKQuery( printSEUsage = ( transType == 'Removal' ), visible = visible )
  fakeClient = fakeClient( transformation, transID, lfns, asIfProd )
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
    previousTask = { 'First': 0, 'SEs':None, 'Location':None, 'Tasks': 0 }
    for task in res['Value']:
      i += 1
      location = []
      for lfn in task[1]:
        l = ','.join( sortList( replicas[lfn].keys() ) )
        #print "LFN", lfn, l
        if not l in location:
          location.append( l )
      if len( task[1] ) == 1:
        if previousTask['Tasks']:
          if task[0] == previousTask['SEs'] and location == previousTask['Location']:
            previousTask['Tasks'] += 1
            continue
          else:
            print '%d:%d (%d tasks)' % ( previousTask['First'], i - 1, i - previousTask['First'] ), '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
            printFinalSEs( transType, previousTask['Location'], previousTask['SEs'] )
        previousTask = { 'First': i, 'SEs':task[0], 'Location':location, 'Tasks': 1 }
      else:
        if previousTask['Tasks']:
            print '%d:%d (%d tasks)' % ( previousTask['First'], i - 1, i - previousTask['First'] ), '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
            printFinalSEs( transType, previousTask['Location'], previousTask['SEs'] )
            previousTask = { 'First': 0, 'SEs':None, 'Location':None, 'Tasks': 0 }
        print i, '- Target SEs:', task[0], "- %d files" % len( task[1] ), " - Current locations:", location
        printFinalSEs( transType, location, task[0] )
    if previousTask['Tasks']:
      i += 1
      print '%d:%d (%d tasks)' % ( previousTask['First'], i - 1, i - previousTask['First'] ), '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
      printFinalSEs( transType, previousTask['Location'], previousTask['SEs'] )
  else:
    print res['Message']
  DIRAC.exit( 0 )

