#!/usr/bin/env python

"""
 Create a new replication transformation
"""

from DIRAC.Core.Base import Script

plugin = "ArchiveDataset"
test = False

Script.registerSwitch( "P:", "Production=", "   Production ID to search (comma separated list)" )
Script.registerSwitch( "f:", "FileType=", "   File type (to be used with --Prod" )
Script.registerSwitch( "B:", "BKQuery=", "   Bookkeeping query path" )
Script.registerSwitch( "r:", "Request=", "   Assign request number [0]" )
Script.registerSwitch( "p:", "Plugin=", "   Plugin name %s" % plugin )
Script.registerSwitch( "", "Parameters=", "   Additional parameters ({<key>:<val>,[<key>:val>]}" )
Script.registerSwitch( "", "SEs=", "   List of SEs" )
Script.registerSwitch( "", "Copies=", "   Number of copies in the list of SEs" )
Script.registerSwitch( "S", "Start", "   If set, the transformation is set Active and Automatic" )
Script.registerSwitch( "g:", "Group=", "   Transformation group [<plugin name>]" )
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
transGroup = None
pluginParams = {}
transName = 'Replication'
listSEs = None
nbCopies = None

for switch in Script.getUnprocessedSwitches():
  opt = switch[0].lower()
  val = switch[1]
  if opt in ( "p", "production" ):
    prods = val.split( ',' )
  elif opt in ( "f", "filetype" ):
    fileType = val.split( ',' )
  elif opt in ( "b", "bkquery" ):
    bkQuery = val
  elif opt in ( 'r', 'request' ):
    requestID = val
  elif opt in ( 'p', 'plugin' ):
    plugin = val
  elif opt in ( 's', 'start' ):
    start = True
  elif opt in ( 'g', 'group' ):
    transGroup = val
  elif opt == "ses":
    listSEs = val.split( ',' )
  elif opt == "copies":
    try:
      nbCopies = int( val )
    except:
      print "--Copies must be an integer"
      import DIRAC
      DIRAC.exit( 2 )
  elif opt == 'test':
    test = True
  elif opt == 'parameters':
    try:
      pluginParams = eval( val )
    except:
      import DIRAC
      print "Error parsing parameters: ", val
      DIRAC.exit( 2 )

import DIRAC
from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

# Add parameters
if listSEs:
  pluginParams['destinationSEs'] = listSEs
if nbCopies:
  pluginParams['NumberOfReplicas'] = nbCopies

if not transGroup:
  transGroup = plugin

if prods:
  if not fileType:
    Script.showHelp()
    DIRAC.exit( 2 )
  transBKQuery = {  'ProductionID'           : prods,
                    'FileType'               : fileType
                    }
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
  transBKQuery = {}
  if bkNodes[0] == "MC":
    bkFields[2] = "SimulationConditions"
  for i in range( len( bkFields ) ):
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
transformation.setType( 'Replication' )
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
  print "Transformation Name:", transName
  print "Transformation group:", transGroup
  print "Long description:", longName
  print "BK Query:", transBKQuery
  if transBKQuery:
    res = transformation.testBkQuery( transBKQuery )
    if res['OK']:
      nfiles = len( res['Value'] )
      print nfiles, "files found for that query"
    else:
      print "**** ERROR in BK query ****"
      print res['Message']
  print "Plugin:", plugin
  print "Parameters:", pluginParams
  print "RequestID:", requestID
  if start: print "Transformation will be started"
  DIRAC.exit( 0 )

if transBKQuery:
  res = transformation.testBkQuery( transBKQuery )
  if res['OK']:
    nfiles = len( res['Value'] )
  else:
    print "**** ERROR in BK query ****"
    print res['Message']
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
    if transBKQuery:
      print "BK Query:", transBKQuery
      print nfiles, "files found for that query"
    print "Plugin:", plugin
    if pluginParams:
      print "Additional parameters:", pluginParams
    print "RequestID:", requestID
    DIRAC.exit( 0 )
  else:
    print result['Message']
    DIRAC.exit( 2 )
