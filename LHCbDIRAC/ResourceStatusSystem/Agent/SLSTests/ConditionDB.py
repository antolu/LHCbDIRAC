#'''  ConditionDB
#
#  Module that runs the tests for the CondDB SLS sensors.
#
#'''
#
#import os
#import pwd
#import re
#import subprocess
#
#from DIRAC                                                  import gLogger, gConfig, S_ERROR, S_OK
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
#
#from LHCbDIRAC.Core.Utilities                               import ProductionEnvironment
#from LHCbDIRAC.ResourceStatusSystem.Utilities               import SLSXML
#
#__RCSID__  = '$Id: $'
#   
#def getProbeElements():  
#  '''
#  Gets the elements that are going to be evaluated by the probes. In this case,
#  all ConditionDBs defined in the RSS.
#  '''
#  
##  try:
#  
#  rsc     = ResourceStatusClient()
#  condDBs = rsc.getService( serviceType = 'CondDB', meta = { 'columns' : 'SiteName' } )
#    
#  if not condDBs[ 'OK' ]:
#    return S_ERROR( 'No CondDBs found on the RSS: %s' % condDBs[ 'Message' ] )
#    
#  if not os.environ.has_key( 'USER' ):
#    # Workaround: on some VOBOXes, the dirac process runs without a USER env variable.
#    os.environ[ 'USER' ] = pwd.getpwuid( os.getuid() )[0]
#    
#  env = ProductionEnvironment.getProjectEnvironment( 'x86_64-slc5-gcc43-opt', 'LHCb' )
#  if not env[ 'OK' ]:
#    return env
#  env = env[ 'Value' ]
#    
#  return S_OK( [ ( condDB, env, ) for condDB in condDBs[ 'Value' ] ] )
#    
##  except Exception, e:
##    _msg = '%s: Exception gettingProbeElements'
##    gLogger.debug( 'ConditionDB: %s: \n %s' % ( _msg, e ) )
##    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 
#
#def setupProbes( testConfig ):
#  '''
#  Sets up the environment to run the probes. In this case, it ensures the 
#  directory where temp files are going to be written exists, and writes the
#  gaudi options file.
#  '''  
#
#  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
#
#  try:
#    os.makedirs( path )
#  except OSError:
#    pass # The dir exist already, or cannot be created: do nothing
#
##  try:
#    
#  writeOptionsFile( path  )
#  return S_OK()
#    
##  except Exception, e:  
##    _msg = '%s: Exception settingProbe'
##    gLogger.debug( 'ConditionDB: %s: \n %s' % ( _msg, e ) )
##    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 
#
#def runProbe( probeInfo, testConfig ):
#  '''
#  Runs the probe and formats the results for the XML generation. The probe is a 
#  simple gaudirun.  
#  '''
#
#  workdir = testConfig[ 'workdir' ]
#  condDB  = probeInfo[ 0 ]
#  env     = probeInfo[ 1 ]
##  rmc     = probeInfo[ 2 ]
#  
#  condDBPath = '/Resources/CondDB/%s' % condDB  
#  config     = gConfig.getOptionsDict( condDBPath )
#  
#  loadTime, availability = 0, 0
#  
#  if not config[ 'OK' ]:
#    _msg = 'not found config for %s.\n %s' % ( condDBPath, config[ 'Message' ] )
#    gLogger.error( 'ConditionDB: %s' % _msg )  
#    availavilityinfo = _msg   
#  
#  else:  
#    writeDBlookup( testConfig, config[ 'Value' ] )
#    writeAuthentication( testConfig, config[ 'Value' ] )
#    
#    _resultPath = '%s/condDB_result.log' % workdir
#    res_log = open( _resultPath, 'w' )
#    
#    try:
#      ret = subprocess.call( [ 'gaudirun.py', '%s/options.py' % workdir ], env = env, 
#                             stdout = res_log, stderr = subprocess.STDOUT )
#    finally:
#      res_log.close()       
#    
#    if ret == 0:
#    
#      res = open( _resultPath, 'r' )
#      try:
#        res_string = res.read()
#      finally:
#        res.close()
#        
#        _str = 'ToolSvc.Sequenc...\s+INFO\s+LoadDDDB\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+(\d+\.\d)\s+\|\s+(\d)\s+\|\s+(\d+\.\d+)'
#        regExp = re.compile( _str )
#        reRes  = regExp.search( res_string )
#      
#        loadTime     = float( reRes.group( 6 ) )
#        availability = 100   
#        availabilityinfo = 'Run gaudirun.py without problems.'
#  
#    else:  
#      availabilityinfo = 'Error running gaudirun.py'
#  
#    # Update results to DB
#    # Utils.unpack(insert_slsconddb(Site=site, Availability=availability, AccessTime=loadTime))       
#
#  ## XML generation
#  
#  xmlDict = {}
#  xmlDict[ 'id' ]               = 'LHCb_ConditionDB_%s' % condDB
#  xmlDict[ 'target' ]           = condDB
#  xmlDict[ 'availability']      = availability
#  xmlDict[ 'metric']            = ( availability and 1 ) or -1
#  xmlDict[ 'availabilityinfo' ] = availabilityinfo
#  xmlDict[ 'data' ]             = [ #node name, name attr, desc attr, node value
#                                 ( 'numericvalue', 'Time to access ConditionDB', None, loadTime ),
#                                 ( 'textvalue'   , None, None, 'ConditionDB access time' )
#                                ] 
#    
#  return { 'xmlDict' : xmlDict, 'config' : testConfig }#, 'rmc' : rmc }         
#       
#################################################################################
#      
#def writeDBlookup( testConfig, condDBConfig ):
#    
#  service = [ { 'tag' : 'service', 'attrs' : [ ( 'accessMode', 'readonly' ),
#                                               ( 'authentication', 'password'),
#                                               ( 'name', '%s/lhcb_conddb' % condDBConfig['Connection'] ) 
#                                             ]
#             }]
#  logicse = [ { 'tag' : 'logicalservice', 'attrs' : [ ( 'name', 'CondDB' ) ], 'nodes' : service } ]
#  xmlList = [ { 'tag' : 'servicelist', 'nodes' : logicse }]
#  
#  xmlDict = { 'xmlList' : xmlList, 'filename' : 'dblookup.xml', 
#              'useStub' : False, 'config' : testConfig }
#    
#  SLSXML.writeXml( 0, xmlDict )
#    
#def writeAuthentication( testConfig, condDBConfig ):
#    
#  connlist, conn = [], []
#    
#  conn.append( { 'tag' : 'parameter',  'attrs' : [ ( 'name', 'user' ), ( 'value', condDBConfig['Username'] ) ] } )
#  conn.append( { 'tag' : 'parameter',  'attrs' : [ ( 'name', 'password' ), ( 'value', condDBConfig['Password'] ) ] } )
#  connection = { 'tag' : 'connection', 'attrs' : [ ( 'name', '%s/lhcb_conddb' % condDBConfig['Connection'] ) ], 'nodes' : conn }
#    
#  connlist.append( connection )
#    
#  role = { 'tag' : 'role', 'attrs' : [ ( 'name','reader' )], 'nodes' : conn }
#  connlist.append( role )
#    
#  connection[ 'attrs' ] = [ ( 'name', '%s/lhcb_online_conddb' % condDBConfig['Connection'] ) ]
#  connlist.append( connection )
#    
#  ### WTF !!?? This dict should not be appended
#  connlist.append( role )
#    
#  xmlList = [ { 'tag' : 'connectionlist', 'nodes' : connlist } ]
#    
#  xmlDict = { 'xmlList' : xmlList, 'filename' : 'authentication.xml', 
#              'useStub' : False , 'config' : testConfig }  
#    
#  SLSXML.writeXml( 0, xmlDict )
#    
#      
#def writeOptionsFile( workdir ):
#    
#  options_text = """from Gaudi.Configuration import *
#from Configurables import LHCbApp
#
#from Configurables import LoadDDDB
#from Configurables import CondDB
#
#from Configurables import COOLConfSvc
#def disableLFC():
#    COOLConfSvc(UseLFCReplicaSvc = False)
#appendPostConfigAction(disableLFC)
#
#
## ---------- option to use Oracle CondDB instead of SQLDDDB
#CondDB(UseOracle = True, IgnoreHeartBeat = True)
#
#LHCbApp(DataType = '2010')
#
#ApplicationMgr().EvtSel     = "NONE"
#ApplicationMgr().EvtMax     = 1
#
#ApplicationMgr().TopAlg  = [ "GaudiSequencer" ]
#GaudiSequencer().Members += [ "LoadDDDB" ]
#GaudiSequencer().MeasureTime = True
#
## ---------- option to select only a subtree
#LoadDDDB(Node = '/dd/Structure/LHCb')
#"""   
#  
#  options_file = open( '%s/options.py' % workdir, 'w' )
#  try:
#    options_file.write( options_text )
#  finally:
#    options_file.close()
#       
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF