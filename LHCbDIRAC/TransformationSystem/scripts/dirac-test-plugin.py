#!/usr/bin/env python

"""
 Test a plugin
"""

__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script

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
    res = transformation.testBkQuery( transBKQuery )
    if not res['OK']:
      print "**** ERROR in BK query ****"
      print res['Message']
      return ( None, None )
    else:
      lfns = res['Value']
      lfns.sort()
      print "in directories:"
      dirs = {}
      import os
      for lfn in lfns:
        dir = os.path.dirname( lfn )
        if not dirs.has_key( dir ):
          dirs[dir] = 0
        dirs[dir] += 1
      for dir in dirs.keys():
        print dir, dirs[dir], "files"

    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
    bk = BookkeepingClient()
    rm = ReplicaManager()
    res = bk.getFileMetadata( lfns )
    if res['OK']:
      files = []
      for lfn, metadata in res['Value'].items():
        runID = metadata.get( 'RunNumber', 0 )
        runDict = {"RunNumber":runID, "LFN":lfn}
        files.append( runDict )
    res = self.trans.getType()
    type = res['Value']
    replicas = {}
    for lfnChunk in breakListIntoChunks( lfns, 200 ):
      if type.lower() in ( "replication", "removal" ):
        res = rm.getReplicas( lfnChunk )
      else:
        res = rm.getActiveReplicas( lfnChunk )
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
    return ( files, replicas )


plugin = None
transType = "Replication"
removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )
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
bkFields = ["ConfigName", "ConfigVersion", "DataTakingConditions", "ProcessingPass", "EventType", "FileType"]
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
    if float( int( val ) ) == int( val ):
      groupSize = int( val )
    else:
      groupSize = float( val )
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

if plugin in removalPlugins:
  transType = "Removal"
# Add parameters
if nbCopies != None:
  pluginParams['NumberOfReplicas'] = nbCopies

transBKQuery = {'Visible': 'Yes'}

if runs:
  transBKQuery['StartRun'] = runs[0]
  transBKQuery['EndRun'] = runs[1]

if prods:
  if not fileType:
    fileType = ['All']
  if prods[0].upper() != 'ALL':
    transBKQuery['ProductionID'] = prods
    if len( prods ) == 1 and not requestID:
      res = TransformationClient().getTransformation( int( prods[0] ) )
      if res['OK']:
        requestID = int( res['Value']['TransformationFamily'] )
  if fileType[0].upper() != 'ALL':
    transBKQuery['FileType'] = fileType
  if len( prods ) == 1:
    s = 's'
  else:
    s = ''
else:
  if not bkQuery:
    Script.showHelp()
    DIRAC.exit( 2 )
  if bkQuery[0] == '/':
    bkQuery = bkQuery[1:]
  bk = bkQuery.split( '/' )
  try:
    bkNodes = [bk[0], bk[1], bk[2], '/' + '/'.join( bk[3:-2] ), bk[-2], bk[-1]]
  except:
    print "Incorrect BKQuery...\nSyntax: %s" % '/'.join( bkFields )
    Script.showHelp()
    DIRAC.exit( 2 )
  if bkNodes[0] == "MC" or bk[-2][0] != '9':
    bkFields[2] = "SimulationConditions"
  for i in range( len( bkFields ) ):
    if not bkNodes[i].upper().endswith( 'ALL' ):
      transBKQuery[bkFields[i]] = bkNodes[i]

transformation = Transformation()

transformation.setType( transType )
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
print "\nNow testing the plugin\n"

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

