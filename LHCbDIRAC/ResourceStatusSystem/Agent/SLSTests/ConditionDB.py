################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gLogger, gConfig, S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

from LHCbDIRAC.Core.Utilities                               import ProductionEnvironment
from LHCbDIRAC.ResourceStatusSystem.Utilities               import SLSXML

import os, pwd, re, time, subprocess
   
def getProbeElements( self ):  
  
  try:
  
    rsc     = ResourceStatusClient()
    condDBs = rsc.getService( serviceType = 'CondDB', meta = { 'columns' : 'SiteName' } )
    
    if not condDBs[ 'OK' ]:
      return S_ERROR( 'No CondDBs found on the RSS: %s' % condDBs[ 'Message' ] )
    
    if not os.environ.has_key( 'USER' ):
      # Workaround: on some VOBOXes, the dirac process runs without a USER env variable.
      os.environ[ 'USER' ] = pwd.getpwuid( os.getuid() )[0]
    
    env = ProductionEnvironment.getProjectEnvironment( 'x86_64-slc5-gcc43-opt', 'LHCb' )
    
    return S_OK( [ ( condDB, env ) for condDB in condDBs[ 'Value' ] ] )
    
  except Exception, e:
    _msg = '%s: Exception gettingProbeElements'
    gLogger.debug( '%s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 

################################################################################

def setupProbe( testConfig ):

  try:
    
    writeOptionsFile( testConfig[ 'workdir' ]  )
    
  except Exception, e:  
    _msg = '%s: Exception settingProbe'
    gLogger.debug( '%s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 
    
################################################################################

def runProbe( probeInfo, testConfig ):

  workdir = testConfig[ 'workdir' ]
  condDB  = probeInfo[ 0 ]
  
  conDBPath = '/Resources/CondDB/%s' % condDB  
  config    = gConfig.getOptionsDict( condDBPath )
  
  loadTime, availability = 0, 0
  
  if not config[ 'OK' ]:
    gLogger.error( 'ConditionDB: not found config for %s.\n %s' % ( conDBPath, config[ 'Message' ] ) )     
  
  else:  
    writeDBlookup( config[ 'Value' ] )
    writeAuthentication( config[ 'Value' ] )
    
    _resultPath = '%s/condDB_result.log' % workdir
    res_log = open( _resultPath, 'w' )
    
    try:
      ret = subprocess.call( [ 'gaudirun.py', '%s/options.py' % workdir ], env = env, stdout = f, stderr=subprocess.STDOUT )
    finally:
      res_log.close()       
    
    if ret == 0:
    
      res = open( _resultPath, 'r' )
      try:
        res_string = res.read()
      finally:
        res.close()
        
        _str = 'ToolSvc.Sequenc...\s+INFO\s+LoadDDDB\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+(\d+\.\d)\s+\|\s+(\d)\s+\|\s+(\d+\.\d+)'
        regExp = re.compile( _str )
        reRes  = regExp.search( res_string )
      
        loadTime     = float( reRes.group( 6 ) )
        availability = 100   
  
    # Update results to DB
    # Utils.unpack(insert_slsconddb(Site=site, Availability=availability, AccessTime=loadTime))       

  ## XML generation
    
  xmlList = []
  xmlList.append( { 'tag' : 'id', 'nodes' : '%s_CondDB' % condDB } )
  xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )  
    
  xmlList.append( { 'tag' : 'refreshperiod'    , 'nodes' : testConfig[ 'refreshperiod' ] })
  xmlList.append( { 'tag' : 'validityduration' , 'nodes' : testConfig[ 'validityduration' ] } )
    
  dataNodes = []
  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Time to access CondDB' ) ], 'nodes' : loadTime } )
  dataNodes.append( { 'tag' : 'textvalue', 'nodes' : 'ConditionDB access timex' } )
    
  xmlList.append( { 'tag' : 'data', 'nodes' : dataNodes } )
  xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( '%Y-%m-%dT%H:%M:%S' ) })
    
  return ( xmlList, testConfig )         
       
################################################################################
      
def writeDBlookup( config ):
    
  service = [ { 'tag' : 'service', 'attrs' : [ ( 'accessMode', 'readonly' ),
                                               ( 'authentication', 'password'),
                                               ( 'name', '%s/lhcb_conddb' % config['Connection'] ) 
                                             ]
             }]
  logicse = [ { 'tag' : 'logicalservice', 'attrs' : [ ( 'name', 'CondDB' ) ], 'nodes' : service } ]
  xmlList = [ { 'tag' : 'servicelist', 'nodes' : logicse }]
    
  writeXml( xmlList, 'dblookup.xml', useStub = False )
    
def writeAuthentication( config ):
    
  connlist, conn = [], []
    
  conn.append( { 'tag' : 'parameter',  'attrs' : [ ( 'name', 'user' ), ( 'value', config['Username'] ) ] } )
  conn.append( { 'tag' : 'parameter',  'attrs' : [ ( 'name', 'password' ), ( 'value', config['Password'] ) ] } )
  connection = { 'tag' : 'connection', 'attrs' : [ ( 'name', '%s/lhcb_conddb' % config['Connection'] ) ], 'nodes' : conn }
    
  connlist.append( connection )
    
  role = { 'tag' : 'role', 'attrs' : [ ( 'name','reader' )], 'nodes' : conn }
  connlist.append( role )
    
  connection[ 'attrs' ] = [ ( 'name', '%s/lhcb_online_conddb' % config['Connection'] ) ]
  connlist.append( connection )
    
  ### WTF !!?? This dict should not be appended
  connlist.append( role )
    
  xmlList = [ { 'tag' : 'connectionlist', 'nodes' : connlist } ]
    
  writeXml( xmlList, 'authentication.xml', useStub = False )
    
      
def writeOptionsFile( workdir ):
    
  options_file = """from Gaudi.Configuration import *
from GaudiConf.Configuration import *

from Configurables import LoadDDDB
from Configurables import CondDB

from Configurables import COOLConfSvc
def disableLFC():
    COOLConfSvc(UseLFCReplicaSvc = False)
appendPostConfigAction(disableLFC)


# ---------- option to use Oracle CondDB instead of SQLDDDB
CondDB(UseOracle = True, IgnoreHeartBeat = True)

LHCbApp(DataType = '2010')

ApplicationMgr().EvtSel     = "NONE"
ApplicationMgr().EvtMax     = 1

ApplicationMgr().TopAlg  = [ "GaudiSequencer" ]
GaudiSequencer().Members += [ "LoadDDDB" ]
GaudiSequencer().MeasureTime = True

# ---------- option to select only a subtree
LoadDDDB(Node = '/dd/Structure/LHCb')
"""   
  
  file = open( '%s/options.py' % workdir, 'w' )
  try:
    file.write( options_file )
  finally:
    file.close()
       
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF