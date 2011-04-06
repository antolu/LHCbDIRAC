#!/usr/bin/env python

"""
 Create a new replication or removal transformation according to plugin
"""

__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script

plugin = None
test = False
groupSize = 5
removalPlugins = ( "DestroyDataset", "DeleteDataset", "DeleteReplicas" )

Script.registerSwitch( "", "Plugin=", "   Plugin name (mandatory)" )
Script.registerSwitch( "P:", "Production=", "   Production ID to search (comma separated list)" )
Script.registerSwitch( "f:", "FileType=", "   File type (to be used with --Prod) [All]" )
Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path" )
Script.registerSwitch( "r:", "Run=", "   Run or range of runs (r1:r2)" )

Script.registerSwitch( "", "SEs=", "   List of SEs (dest for replication, source for removal)" )
Script.registerSwitch( "", "Copies=", "   Number of copies in the list of SEs" )
Script.registerSwitch( "", "Parameters=", "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}" )
Script.registerSwitch( "k:", "KeepSEs=", "   List of SEs where to keep replicas (for plugins %s)" % str( removalPlugins ) )
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
bkFields = ["ConfigName", "ConfigVersion", "DataTakingConditions", "ProcessingPass", "EventType", "FileType"]
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
  elif opt in ( 'k', 'keepses' ):
    keepSEs = val.split( ',' )
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

if plugin in removalPlugins:
  transType = "Removal"
else:
  transType = "Replication"
transName = transType
# Add parameters
if listSEs:
  if transType == "Replication":
    pluginParams['destinationSEs'] = listSEs
  elif transType == "Removal":
    pluginParams['FromSEs'] = listSEs
if nbCopies != None:
  pluginParams['NumberOfReplicas'] = nbCopies
if keepSEs:
  pluginParams['keepSEs'] = keepSEs

transGroup = plugin

transBKQuery = {'Visible': 'Yes'}

if runs:
  if runs[0]:
    transBKQuery['StartRun'] = runs[0]
  if runs[1]:
    transBKQuery['EndRun'] = runs[1]

if prods:
  if not fileType:
    fileType = ["All"]
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
  longName = transGroup + " of " + ','.join( fileType ) + " for production%s " % s + ','.join( prods )
  transName += '-' + '/'.join( fileType ) + '-' + '/'.join( prods )
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
  if bkNodes[0] == "MC":
    bkFields[2] = "SimulationConditions"
  for i in range( len( bkFields ) ):
    if not bkNodes[i].upper().endswith( 'ALL' ):
      transBKQuery[bkFields[i]] = bkNodes[i]
  longName = transGroup + " for BKQuery " + bkQuery
  transName += '-' + bkQuery


transformation = Transformation()

if requestID:
  transName += '-Request%d' % ( requestID )
transformation.setTransformationName( transName )
transformation.setTransformationGroup( transGroup )
transformation.setDescription( longName )
transformation.setLongDescription( longName )
transformation.setType( transType )
if transType == "Removal":
  if plugin == "DestroyDataset":
    transformation.setBody( "removal;removeFile" )
  else:
    transformation.setBody( "removal;replicaRemoval" )
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
  print "BK Query:", transBKQuery

if transBKQuery:
  res = transformation.testBkQuery( transBKQuery )
  if not res['OK']:
    print "**** ERROR in BK query ****"
    print res['Message']
    DIRAC.exit( 2 )
  else:
    lfns = res['Value']
    lfns.sort()
    nfiles = len( lfns )
    if test:
      print "BKQuery obtained %d files" % nfiles
    dirs = {}
    import os
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      if not dirs.has_key( dir ):
        dirs[dir] = 0
      dirs[dir] += 1
    if test:
      for dir in dirs.keys():
        print dir, dirs[dir], "files"

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
    print "Transformation %d created" % result['Value']
    print "Name:", transName, ", Description:", longName
    if transBKQuery:
      print "BK Query:", transBKQuery
      print nfiles, "files found for that query"
      for dir in dirs.keys():
        print dir, dirs[dir], "files"
    print "Plugin:", plugin
    if pluginParams:
      print "Additional parameters:", pluginParams
    print "RequestID:", requestID
    DIRAC.exit( 0 )
  else:
    print result['Message']
    DIRAC.exit( 2 )
