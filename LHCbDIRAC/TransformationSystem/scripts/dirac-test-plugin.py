#!/usr/bin/env python

"""
 Test a plugin
"""

__RCSID__ = "$Id$"

class fakeClient:
  def __init__( self, trans, transID, lfns, asIfProd ):
    self.trans = trans
    self.transID = transID
    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    self.transClient = TransformationClient()
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    self.bk = BookkeepingClient()
    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    self.rm = ReplicaManager()
    self.asIfProd = asIfProd

    ( self.files, self.replicas ) = self.prepareForPlugin( lfns )

  def getTransformation( self, transID ):
    if transID == self.transID and self.asIfProd:
      transID = self.asIfProd
    if transID != self.transID:
      return self.transClient.getTransformation( transID )
    res = self.trans.getType()
    return DIRAC.S_OK( {'Type':res['Value']} )

  def getReplicas( self ):
    return self.replicas

  def getFiles( self ):
    return self.files

  def getCounters( self, table, attrList, condDict ):
    if condDict['TransformationID'] == self.transID and self.asIfProd:
      condDict['TransformationID'] = self.asIfProd
    if condDict['TransformationID'] != self.transID:
      return self.transClient.getCounters( table, attrList, condDict )
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'SARA-RAW', 'PIC-RAW', 'RAL-RAW']
    counters = []
    for se in possibleTargets:
      counters.append( ( {'UsedSE':se}, 1 ) )
    return DIRAC.S_OK( counters )

  def getBookkeepingQueryForTransformation ( self, transID ):
    return self.trans.getBkQuery()

  def getTransformationRuns( self, condDict ) :
    if condDict['TransformationID'] == self.transID and self.asIfProd:
      condDict['TransformationID'] = self.asIfProd
    if condDict['TransformationID'] == self.transID:
      transRuns = []
      if condDict.has_key( 'RunNumber' ):
        runs = condDict['RunNumber']
        for run in runs:
          transRuns.append( {'RunNumber':run, 'Status':"Active", "SelectedSite":None} )
        return DIRAC.S_OK( transRuns )
    else:
      return self.transClient.getTransformationRuns( condDict )

  def getTransformationFiles( self, condDict=None ):
    if condDict.get( 'TransformationID' ) == self.transID and self.asIfProd:
      condDict['TransformationID'] = self.asIfProd
    if condDict.get( 'TransformationID' ) == self.transID:
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
      return self.transClient.getTransformationFiles( condDict=condDict )

  def getTransformationFilesCount( self, transID, field, selection=None ):
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
      return self.transClient.getTransformationFilesCount( transID, field, selection=selection )

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

  def setFileUsedSEForTransformation( self, transID, se, lfns ):
    return DIRAC.S_OK()

  def prepareForPlugin( self, lfns ):
    import time
    print "Preparing the plugin input data (%d files)" % len( lfns )
    type = self.trans.getType()['Value']
    if not lfns:
      return ( None, None )
    res = self.bk.getFileMetadata( lfns )
    if res['OK']:
      files = []
      for lfn, metadata in res['Value'].items():
        runID = metadata.get( 'RunNumber', 0 )
        runDict = {"RunNumber":runID, "LFN":lfn}
        files.append( runDict )
    replicas = {}
    startTime = time.time()
    from DIRAC.Core.Utilities.List                        import breakListIntoChunks
    for lfnChunk in breakListIntoChunks( lfns, 200 ):
      #print lfnChunk
      if type.lower() in ( "replication", "removal" ):
        res = self.rm.getReplicas( lfnChunk )
      else:
        res = self.rm.getActiveReplicas( lfnChunk )
      #print res
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
    print "Obtained replicas of %d files in %.3f seconds" % ( len( lfns ), time.time() - startTime )
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
  from LHCbDIRAC.TransformationSystem.Client.Utilities import *

  pluginScript = PluginScript()
  pluginScript.registerPluginSwitches()

  Script.registerSwitch( '', 'AsIfProduction=', '   Production # that this test using as source of information' )
  Script.registerSwitch( '', 'AllFiles', '   Sets visible = False (useful if files were marked invisible)' )
  Script.registerSwitch( '', 'NoReplicaFiles', '   Also gets the files without replica (just for BK test)' )
  Script.registerSwitch( '', 'Debug', '   Sets a debug flag in the plugin' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors=True )

  asIfProd = None
  allFiles = False
  debugPlugin = False
  noRepFiles = False
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'AsIfProduction':
      asIfProd = int( val )
    elif opt == 'AllFiles':
      allFiles = True
    elif opt == 'Debug':
      debugPlugin = True
    elif opt == 'NoReplicaFiles':
      noRepFiles = True
  #print pluginScript.getOptions()
  plugin = pluginScript.getOption( 'Plugin' )
  requestID = pluginScript.getOption( 'RequestID', 0 )
  pluginParams = pluginScript.getPluginParameters()
  #print pluginParams

  from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from DIRAC import gLogger
  gLogger.setLevel( 'INFO' )
  # Create the transformation
  transformation = Transformation()
  transType = None
  if plugin in getRemovalPlugins():
    transType = "Removal"
  elif plugin in getReplicationPlugins():
    transType = "Replication"
  else:
    transType = "Processing"
  transType = pluginScript.getOption( 'Type', transType )
  transformation.setType( transType )

  visible = True
  if allFiles or not plugin or plugin == "DestroyDataset" or pluginScript.getOption( 'Productions' ) or transType == 'Processing':
    visible = False
  bkQuery = pluginScript.getBKQuery( visible=visible )
  if noRepFiles and not plugin:
    bkQuery.setOption( 'ReplicaFlag', "All" )
  bkQueryDict = bkQuery.getQueryDict()
  if not bkQueryDict:
    print "No BK query was given..."
    Script.showHelp()
    DIRAC.exit( 2 )

  reqID = pluginScript.getRequestID()
  if not requestID and reqID:
    requestID = reqID

  if pluginParams:
    for key, val in pluginParams.items():
      if ( key.endswith( "SE" ) or key.endswith( "SEs" ) ) and val:
        res = transformation.setSEParam( key, val )
      else:
        res = transformation.setAdditionalParam( key, val )
      if not res['OK']:
        print res['Message']
        DIRAC.exit( 2 )

  print "Transformation type:", transType
  print "BK Query:", bkQueryDict
  print "Plugin:", plugin
  print "Parameters:", pluginParams
  if requestID:
    print "RequestID:", requestID
  # get the list of files from BK
  print "Getting the files from BK"
  lfns = bkQuery.getLFNs( printSEUsage=( transType == 'Removal' and not pluginScript.getOption( 'Runs' ) ), visible=visible )
  if len( lfns ) == 0:
    print "No files found in BK...Exiting now"
    DIRAC.exit( 0 )

  if not plugin:
    print "No plugin to be tested..."
    DIRAC.exit( 0 )

  print "\nNow testing the %s plugin %s" % ( transType.lower(), plugin )
  transformation.setPlugin( plugin )
  transformation.setBkQuery( bkQueryDict )

  from DIRAC.Core.Utilities.List import sortList
  from LHCbDIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin
  transID = -9999
  pluginParams['TransformationID'] = transID
  pluginParams['Status'] = "Active"
  # Create a fake transformation client
  fakeClient = fakeClient( transformation, transID, lfns, asIfProd )
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  oplugin = TransformationPlugin( plugin,
                                  transClient = fakeClient,
                                  replicaManager = ReplicaManager(),
                                  bkkClient = BookkeepingClient(),
                                  debug = debugPlugin )
  oplugin.setParameters( pluginParams )
  replicas = fakeClient.getReplicas()
  # Special case of RAW files registered in CERN-RDST...
  if plugin == "AtomicRun":
    for lfn in [lfn for lfn in replicas if "CERN-RDST" in replicas[lfn]]:
      ses = {}
      for se in replicas[lfn]:
        pfn = replicas[lfn][se]
        if se == "CERN-RDST":
          se = "CERN-RAW"
        ses[se] = pfn
      replicas[lfn] = ses
  files = fakeClient.getFiles()
  if not replicas:
    print "No replicas were found, exit..."
    DIRAC.exit( 2 )
  oplugin.setInputData( replicas )
  oplugin.setTransformationFiles( files )
  import time
  startTime = time.time()
  res = oplugin.generateTasks()
  print "Plugin took %.1f seconds" % ( time.time() - startTime )
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
        # Only 1 file in task
        if previousTask['Tasks']:
          if task[0] == previousTask['SEs'] and location == previousTask['Location']:
            # Accumulate tasks for the same site and with same origin
            previousTask['Tasks'] += 1
            continue
          else:
            # Print out previous tasks
            if i - previousTask['First'] == 1:
              print '%d' % previousTask['First'], '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
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
      if i - previousTask['First'] == 1:
        print '%d' % previousTask['First'], '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
      else:
        print '%d:%d (%d tasks)' % ( previousTask['First'], i - 1, i - previousTask['First'] ), '- Target SEs:', previousTask['SEs'], "- 1 file", " - Current locations:", previousTask['Location']
      printFinalSEs( transType, previousTask['Location'], previousTask['SEs'] )
  else:
    print res['Message']
  DIRAC.exit( 0 )

