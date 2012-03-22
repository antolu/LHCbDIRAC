__RCSID__ = "$Id$"
AGENT_NAME = 'ResourceStatus/SLSAgent'

from DIRAC import gLogger, gConfig, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Base.DB import DB
from DIRAC.ResourceStatusSystem.Utilities                   import CS, Utils
from DIRAC.ResourceStatusSystem.Utilities.Utils             import xml_append

from LHCbDIRAC.Core.Utilities                               import ProductionEnvironment

import xml.dom, xml.sax
import time, string
import urlparse, urllib
import sys, os, re, subprocess, pwd

import lfc2, lcg_util

impl = xml.dom.getDOMImplementation()
rmDB = DB( "ResourceManagementDB", "ResourceStatus/ResourceManagementDB", 10 )

# Generate MySQL INSERT queries
def gen_mysql( n, d, keys ):
  def norm( v ):
    if type( v ) == str: return '"' + v.translate( string.maketrans( "\"", "'" ), ";" ) + '"'
    else: return str( v )

  req = "INSERT INTO " + n + " (" + ", ".join( d.keys() ) + ") VALUES ("
  req += ", ".join( [norm( v ) for v in d.values()] ) + ") "
  req += "ON DUPLICATE KEY UPDATE " + ", ".join( [
      ( "%s=%s" % ( k, norm( v ) ) ) for ( k, v ) in d.items() if k not in keys ] )
  return req

# Convenience funs
def insert_slsservice( **kw ):
  return rmDB._update( gen_mysql( "SLSService", kw, ["System", "Service"] ) )

def insert_slst1service( **kw ):
  return rmDB._update( gen_mysql( "SLST1Service", kw, ["Site", "System"] ) )

def insert_slslogse( **kw ):
  return rmDB._update( gen_mysql( "SLSLogSE", kw, ["Name"] ) )

def insert_slsstorage( **kw ):
  return rmDB._update( gen_mysql( "SLSStorage", kw, ["Site", "Token"] ) )

def insert_slsconddb( **kw ):
  return rmDB._update( gen_mysql( "SLSCondDB", kw, ["Site"] ) )

def insert_slsrmstats( **kw ):
  return rmDB._update( gen_mysql( "SLSRMStats", kw, ["Site", "System", "Name"] ) )

def gen_xml_stub():
  doc = impl.createDocument( "http://sls.cern.ch/SLS/XML/update",
                            "serviceupdate",
                            None )
  doc.documentElement.setAttribute( "xmlns", "http://sls.cern.ch/SLS/XML/update" )
  doc.documentElement.setAttribute( "xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance' )
  doc.documentElement.setAttribute( "xsi:schemaLocation",
                                   "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd" )
  return doc

#### Helper functions to send a warning mail to a site (for space-token test)

def get_pledged_value_for_token( se, st ):
  val = float( gConfig.getValue( "/Resources/Shares/Disk/" + se + "/" + st ) )
  return ( val if val != None else 0 )

def contact_mail_of_site( site ):
  return gConfig.getValue( "/Resources/Shares/Disk/" + site + "/Mail" )

def send_mail_to_site( site, token, pledged, total ):
  from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
  nc = NotificationClient()
  subject = "%s provide insufficient space for space-token %s" % ( site, token )
  body = """
Hi ! Our RSS monitoring systems informs us that %s has
pledged %f TB to the space-token %s, but in reality, only %f TB of
space is available. Thanks to solve the problem if possible.

""" % ( site, pledged, token, total )
  address = contact_mail_of_site( site )
  if address:
    res = nc.sendMail( address, subject, body )
    if res['OK'] == False:
      gLogger.warn( "Unable to send mail to %s: %s" % ( address, res['Message'] ) )
    else:
      gLogger.info( "Sent mail to %s OK!" % address )

class TestBase( object ):
  def __init__( self, am ):
    self.am = am

  def getAgentOption( self, name, defaultValue = None ):
    return self.am.am_getOption( name, defaultValue )

  def getTestOption( self, name, defaultValue = None ):
    return self.am.am_getOption( self.__class__.__name__ + "/" + name, defaultValue )

  getAgentValue = getAgentOption
  getTestValue = getTestOption

class SpaceTokenOccupancyTest( TestBase ):

  def __init__( self, am ):
    super( SpaceTokenOccupancyTest, self ).__init__( am )
    self.xmlPath = rootPath + "/" + self.getAgentOption( "webRoot" ) + self.getTestOption( "dir" )
    self.SEs = CS.getSpaceTokenEndpoints()

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dir exist already, or cannot be created: do nothing

    try:
      import lcg_util
      for site in self.SEs:
        for st in CS.getSpaceTokens():
          try:
            self.generate_xml_and_dashboard( site, st, lcg_util )
          except:
            gLogger.warn( 'SpaceTokenOccupancyTest crashed at %s, %s' % ( site, st ) )
    except ImportError:
      gLogger.warn( "SpaceTokenOccupancyTest cannot import lcg_util, aborting." )

  def generate_xml_and_dashboard( self, site, st, lcg_util ):
    url = self.SEs[site]['Endpoint']
    total = 0
    guaranteed = 0
    free = 0
    validity = 'PT0M'
    availability = 0

    answer = lcg_util.lcg_stmd( st, url, True, 0 )

    if answer[0] == 0:
      output = answer[1][0]
      total = float( output['totalsize'] ) / 1e12 # Bytes to Terabytes
      guaranteed = float( output['guaranteedsize'] ) / 1e12
      free = float( output['unusedsize'] ) / 1e12
      availability = 100 if free > 4 else ( free * 100 / total if total != 0 else 0 )
      validity = self.getTestOption( "validity" )
    else:
      gLogger.info( "StorageSpace: problew with lcg_util:\
 lcg_util.lcg_stmd('%s', '%s', True, 0) = (%d, %s)" % ( st, url, answer[0], answer[1] ) )
      gLogger.info( str( answer ) )

    doc = gen_xml_stub()
    xml_append( doc, "id", site + "_" + st )
    xml_append( doc, "availability", availability )
    elt = xml_append( doc, "availabilitythresholds" )
    xml_append( doc, "threshold", value_ = self.getTestOption( "Thresholds/available" ), elt_ = elt, level = "available" )
    xml_append( doc, "threshold", value_ = self.getTestOption( "Thresholds/affected" ), elt_ = elt, level = "affected" )
    xml_append( doc, "threshold", value_ = self.getTestOption( "Thresholds/degraded" ), elt_ = elt, level = "degraded" )
    xml_append( doc, "availabilityinfo", "Free=" + str( free ) + " Total=" + str( total ) )
    xml_append( doc, "availabilitydesc", self.getTestValue( "availabilitydesc" ) )
    xml_append( doc, "refreshperiod", self.getTestValue( "refreshperiod" ) )
    xml_append( doc, "validityduration", validity )
    elt = xml_append( doc, "data" )
    elt2 = xml_append( doc, "grp", name = "Space occupancy", elt_ = elt )
    xml_append( doc, "numericvalue", value_ = str( total - free ), elt_ = elt2, name = "Consumed" )
    xml_append( doc, "numericvalue", value_ = str( total ), elt_ = elt2, name = "Capacity" )
    xml_append( doc, "numericvalue", value_ = str( free ), elt_ = elt, name = "Free space" )
    xml_append( doc, "numericvalue", value_ = str( total - free ), elt_ = elt, name = "Occupied space" )
    xml_append( doc, "numericvalue", value_ = str( total ), elt_ = elt, name = "Total space" )
    xml_append( doc, "textvalue", "Storage space for the specific space token", elt_ = elt )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )

    Utils.unpack( insert_slsstorage( Site = site, Token = st, Availability = availability,
                      RefreshPeriod = "PT27M", ValidityDuration = validity,
                      TotalSpace = int( total ), GuaranteedSpace = int( guaranteed ),
                      FreeSpace = int( free ) ) )

    xmlfile = open( self.xmlPath + site + "_" + st + ".xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

    # Send notifications
    # pledged = get_pledged_value_for_token(site, st)
    # if not fake and total+1 < pledged:
    #   gLogger.info("%s/%s: pledged = %f, total = %f, sending mail to site..." % (site, st, pledged, total))
    #   send_mail_to_site(site, st, pledged, total)

    # Dashboard
    dbfile = open( self.xmlPath + site + "_" + st + "_space_monitor", "w" )
    try:
      dbfile.write( st + ' ' + str( total ) + ' ' + str( guaranteed ) + ' ' + str( free ) + '\n' )
    finally:
      dbfile.close()

    gLogger.info( "SpaceTokenOccupancyTest: %s/%s done." % ( site, st ) )

class DIRACTest( TestBase ):
  def __init__( self, am ):
    super( DIRACTest, self ).__init__( am )
    self.setup = gConfig.getValue( 'DIRAC/Setup', "" )
    self.setupDict = CS.getTypedDictRootedAt( root = "/DIRAC/Setups", relpath = self.setup )
    self.xmlPath = rootPath + "/" + self.getAgentValue( "webRoot" ) + self.getTestValue( "dir" )
    from DIRAC.Interfaces.API.Dirac import Dirac
    self.dirac = Dirac()

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dir exist already, or cannot be created: do nothing

#    self.xml_gw()
    self.run_xml_sensors()
    self.run_t1_xml_sensors()

  def run_xml_sensors( self ):
    # For each service of each system, run xml_sensors...
    systems = CS.getTypedDictRootedAt( root = "", relpath = "/Systems" )
    discovered_services = []
    for s in systems:
      try:
        services = systems[s][self.setupDict[s]]['Services']
        for k in services:
          discovered_services.append( ( s, k ) )
      except KeyError:
        try:
          gLogger.warn( "DIRACTest: No /Systems/%s/%s/Services in CS." % ( s, self.setupDict[s] ) )
        except KeyError:
          gLogger.warn( "DIRACTest: No /Systems/%s in CS." % s )

    gLogger.info( "DIRACTest: discovered %d services" % len( discovered_services ) )

    for ( s, srv ) in discovered_services:
      try:
        self.xml_sensor( s, srv )
      except:
        gLogger.warn( 'DIRACTest.xml_sensors crashed on %s, %s' % ( s, srv ) )

  def run_t1_xml_sensors( self ):
    # For each T0/T1 VO-BOXes, run xml_t1_sensors...
    request_management_urls = gConfig.getValue( "/Systems/RequestManagement/Development/URLs/allURLS", [] )
    configuration_urls = gConfig.getServersList()
    framework_urls = gConfig.getValue( "/DIRAC/Framework/SystemAdministrator", [] )

    gLogger.info( "DIRACTest: discovered %d request management url(s), %d configuration url(s) and %d framework url(s)"
                 % ( len( request_management_urls ), len( configuration_urls ), len( framework_urls ) ) )
    for url in request_management_urls + configuration_urls + framework_urls:
      try:
        self.xml_t1_sensor( url )
      except:
        gLogger.warn( 'DIRACTest.t1_xml_sensors crashed on %s' % url )

  # XML GENERATORS

  # This test is an isolated SLS test for one service.. Why is it different ?
  def xml_gw( self ):
    try:
      sites = gConfig.getSections( '/Resources/Sites/LCG' )['Value']
    except KeyError:
      gLogger.error( "SLSAgent, DIRACTest: Unable to query CS" )
      sites = []

    doc = gen_xml_stub()
    xml_append( doc, "id", "Framework_Gateway" )
    xml_append( doc, "webpage", self.getTestValue( "webpage" ) )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )

    if sites == []:
      xml_append( doc, "availability", 0 )
      xml_append( doc, "notes",
                 "Retrieved 0 sites out of the Configuration Service.\
 Please check the CS is up and running otherwise is the Gateway" )

    else:
      xml_append( doc, "availability", 100 )
      xml_append( doc, "notes", "Retrieved " + str( len( sites ) ) + "\
 sites out of the Configuration Service through the Gateway" )

    xmlfile = open( self.xmlPath + "Framework_Gateway.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def xml_sensor( self, system, service ):
    res = self.dirac.ping( system, service )
    doc = gen_xml_stub()
    xml_append( doc, "id", system + "_" + service )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )

    if res["OK"]:
      gLogger.info( "%s/%s successfully pinged" % ( system, service ) )
      res = res["Value"]
      host = urlparse.urlparse( res['service url'] ).netloc.split( ":" )[0]
      xml_append( doc, "availability", 100 )
      xml_append( doc, "webpage", "http://lemonweb.cern.ch/lemon-web/info.php?entity=" + host )
      elt = xml_append( doc, "data" )
      xml_append( doc, "numericvalue", res['service uptime'], elt_ = elt,
                 name = "Service Uptime", desc = "Seconds since last restart of service" )
      xml_append( doc, "numericvalue", res['host uptime'], elt_ = elt,
                 name = "Host Uptime", desc = "Seconds since last restart of machine" )
      xml_append( doc, "numericvalue", res['load'].split()[0], elt_ = elt,
                 name = "Load", desc = "Instantaneous load" )
      xml_append( doc, "notes", "Service " + res['service url'] + " completely up and running" )

      # Fill database
      Utils.unpack( insert_slsservice( System = system, Service = service, Availability = 100,
                                     Host = host,
                                     ServiceUptime = res['service uptime'],
                                     HostUptime = res['host uptime'],
                                     InstantLoad = res['load'].split()[0],
                                     Message = ( "Service " + res['service url'] + " completely up and running" ) ) )
    else:
      gLogger.info( "%s/%s does not respond to ping" % ( system, service ) )
      xml_append( doc, "availability", 0 )
      xml_append( doc, "notes", res['Message'] )
      Utils.unpack( insert_slsservice( System = system, Service = service, Availability = 0, Message = res["Message"] ) )

    xmlfile = open( self.xmlPath + system + "_" + service + ".xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def xml_t1_sensor( self, url ):
    parsed = urlparse.urlparse( url )
    if sys.version_info >= ( 2, 6 ):
      system, _service = parsed.path.strip( "/" ).split( "/" )
      site = parsed.netloc.split( ":" )[0]
    else:
      site, system, _service = parsed[2].strip( "/" ).split( "/" )
      site = site.split( ":" )[0]

    pinger = RPCClient( url )
    res = pinger.ping()

    if system == "RequestManagement":
      res2 = Utils.unpack( pinger.getDBSummary() )

    doc = gen_xml_stub()
    xml_append( doc, "id", site + "_" + system )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )

    if res['OK']:
      res = res['Value']

      xml_append( doc, "availability", 100 )
      xml_append( doc, "notes", "Service " + url + " completely up and running" )

      elt = xml_append( doc, "data" )
      xml_append( doc, "numericvalue", res.get( 'service uptime', 0 ), elt_ = elt,
                 name = "Service Uptime", desc = "Seconds since last restart of service" )
      xml_append( doc, "numericvalue", res.get( 'host uptime', 0 ), elt_ = elt,
                 name = "Host Uptime", desc = "Seconds since last restart of machine" )

      Utils.unpack( insert_slst1service( Site = site, System = system, Availability = 100,
                                       Version = res.get( "version", "unknown" ),
                                       ServiceUptime = int( res.get( 'service uptime', 0 ) ),
                                       HostUptime = int( res.get( 'host uptime', 0 ) ),
                                       Message = ( "Service " + url + " completely up and running" ) ) )

      if system == "RequestManagement":
        for k, v in res2.items():
          xml_append( doc, "numericvalue", v["Assigned"], elt_ = elt,
                     name = k + " - Assigned", desc = "Number of Assigned " + k + " requests" )
          xml_append( doc, "numericvalue", v["Waiting"], elt_ = elt,
                     name = k + " - Waiting", desc = "Number of Waiting " + k + " requests" )
          xml_append( doc, "numericvalue", v["Done"], elt_ = elt,
                     name = k + " - Done", desc = "Number of Done " + k + " requests" )

          Utils.unpack( insert_slsrmstats( Site = site, System = system, Name = k,
                                         Assigned = int( v["Assigned"] ),
                                         Waiting = int( v["Waiting"] ),
                                         Done = int( v["Done"] ) ) )

      gLogger.info( "%s/%s successfully pinged" % ( site, system ) )

    else:
      xml_append( doc, "availability", 0 )
      xml_append( doc, "notes", res['Message'] )
      Utils.unpack( insert_slst1service( Site = site, System = system, Availability = 0,
                          Message = res["Message"] ) )

      gLogger.info( "%s/%s does not respond to ping" % ( site, system ) )

    xmlfile = open( self.xmlPath + site + "_" + system + ".xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

class LOGSETest( TestBase ):
  def __init__( self, am ):
    super( LOGSETest, self ).__init__( am )
    self.xmlPath = rootPath + "/" + self.getAgentValue( "webRoot" ) + self.getTestValue( "dir" )

    self.entities = self.getTestValue( "entities" )
    self.lemon_url = self.getTestValue( "lemon_url" )

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dir exist already, or cannot be created: do nothing

    # Generate XML files
    self.partition()
    self.logse( "gridftpd", "PT2H" )
    self.logse( "cert", "PT24H" )
    self.logse( "httpd", "PT2H" )

  # LOG SE Partition
  def partition( self ):
    input_xml = self.getxml( entities = self.entities, metrics = self.getTestOption( "/metrics/partition" ) )
    handler = self.LemonHandler()
    xml.sax.parse( input_xml, handler )

    for d in handler.data:
      if d['data'][0] == "/data":
        ts = int( d['ts'] )
        space = int( d['data'][3] )
        percent = int( d['data'][4] )

    doc = gen_xml_stub()
    xml_append( doc, "id", "log_se_partition" )
    xml_append( doc, "validityduration", "PT12H" )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S", time.gmtime( ts ) ) )
    xml_append( doc, "availability", ( 100 if percent < 90 else ( 5 if percent < 99 else 0 ) ) )
    elt = xml_append( doc, "data" )
    xml_append( doc, "numericvalue", percent, elt_ = elt, name = "LogSE data partition used" )
    xml_append( doc, "numericvalue", space, elt_ = elt, name = "Total space on data partition" )

    Utils.unpack( insert_slslogse( Name = "partition", ValidityDuration = "PT12H",
                                 TimeStamp = time.strftime( "%Y-%m-%dT%H:%M:%S", time.gmtime( ts ) ),
                                 Availability = ( 100 if percent < 90 else ( 5 if percent < 99 else 0 ) ),
                                 DataPartitionUsed = percent, DataPartitionTotal = space ) )
    gLogger.info( "LogSE partition test done" )

    xmlfile = open( self.xmlPath + "log_se_partition.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def logse( self, name, validity_duration ):
    input_xml = self.getxml( entities = self.entities, metrics = self.getTestOption( "/metrics/" + name ) )
    handler = self.LemonHandler()
    xml.sax.parse( input_xml, handler )
    data = handler.data[0]

    doc = gen_xml_stub()
    xml_append( doc, "id", "log_se_" + name )
    xml_append( doc, "validityduration", validity_duration )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S", time.gmtime( data['ts'] ) ) )
    xml_append( doc, "availability", int( round( float( data['data'][0] ) * 100 ) ) )

    Utils.unpack( insert_slslogse( Name = name, ValidityDuration = validity_duration,
                    Availability = int( round( float( data['data'][0] ) * 100 ) ) ) )
    gLogger.info( "LogSE " + name + " test done" )

    xmlfile = open( self.xmlPath + "log_se_" + name + ".xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def getxml( self, **kw ):
    params = urllib.urlencode( kw )
    xml_ = urllib.urlopen( self.lemon_url, params )
    return xml_

  class LemonHandler( xml.sax.handler.ContentHandler ):
    def __init__( self ):
      xml.sax.handler.ContentHandler.__init__( self )
      self.node = ""
      self.metric = -1
      self.ts = 0
      self.cur_list = []
      self.inside_d = False
      self.data = []

    def startElement( self, name, attrs ):
      if name == "data":
        self.node = attrs.getValue( "node" )

      elif name == "metric":
        self.metric = int( attrs.getValue( "id" ) )

      elif name == "d":
        self.inside_d = True

      elif name == "r":
        self.ts = int( attrs.getValue( "ts" ) )
        self.cur_list = []

    def endElement( self, name ):
      if name == "r":
        self.data.append( { 'node'   : self.node,
                           'metric' : self.metric,
                           'ts'     : self.ts,
                           'data'   : self.cur_list } )

      elif name == "d":
        self.inside_d = False

    def characters( self, content ):
      if self.inside_d:
        self.cur_list.append( content )

class CondDBTest( TestBase ):
  def __init__( self, am ):
    super( CondDBTest, self ).__init__( am )

    # Get ConDB infos
    self.CDB_infos = CS.getTypedDictRootedAt( root = "", relpath = "/Resources/CondDB" )
    self.xmlPath = rootPath + "/" + self.getAgentValue( "webRoot" ) + self.getTestValue( "dir" )

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dir exist already, or cannot be created: do nothing


    # Go to work directory
    oldcwd = os.getcwd()
    os.chdir( am.am_getWorkDirectory() )

    # Generate options file
    options = """from Gaudi.Configuration import *
from Configurables import LHCbApp

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
    options_file = open( "options.py", "w" )
    try:
      options_file.write( options )
    finally:
      options_file.close()

    # For each CondDB, run the test and generate XML file
    for site in self.CDB_infos:
      gLogger.info( "Starting SLS CondDB test for site %s" % site )
      try:
        self.run_test( site )
      except:
        gLogger.warn( 'CondDBTest crashed at %s' % site )

    # Go back to previous directory
    os.chdir( oldcwd )
    ### END OF TEST

  def run_test( self, site ):
    # Generate the dblookup.xml and authentication.xml files needed by gaudirun.py
    self.generate_lookup_file( site )
    self.generate_authentication_file( site )

    if not os.environ.has_key( "USER" ):
      # Workaround: on some VOBOXes, the dirac process runs without a USER env variable.
      os.environ["USER"] = pwd.getpwuid( os.getuid() )[0]

    try:
      env = Utils.unpack( ProductionEnvironment.getProjectEnvironment( 'x86_64-slc5-gcc43-opt', "LHCb" ) )
    except Utils.RPCError:
      gLogger.warn( "Unable to run CondDB test for site %s: environment cannot be set. Aborting." % site )
      return

    f = open( "result.log", "w" )
    try:
      ret = subprocess.call( ["gaudirun.py", "options.py"], env = env, stdout = f, stderr = subprocess.STDOUT )
    finally:
      f.close()

    if ret == 0:
      res = open( "result.log", "r" )
      try:
        res_string = res.read()
      finally:
        res.close()

        regExp = re.compile( "ToolSvc.Sequenc...\s+INFO\s+LoadDDDB\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+(\d+\.\d)\s+\|\s+(\d)\s+\|\s+(\d+\.\d+)" )
        reRes = regExp.search( res_string )
        loadTime = float( reRes.group( 6 ) )
        availability = 100

    else:
      loadTime = 0
      availability = 0

    # Update results to DB
    Utils.unpack( insert_slsconddb( Site = site, Availability = availability, AccessTime = loadTime ) )

    # Generate XML file
    self.generate_xml( site, loadTime, availability )

  def generate_lookup_file( self, site ):
    doc = impl.createDocument( None, "servicelist", None )
    elt = xml_append( doc, "logicalservice", name = "CondDB" )
    xml_append( doc, "service", elt_ = elt, name = self.CDB_infos[site]['Connection'] + "/lhcb_conddb",
               accessMode = "readonly", authentication = "password" )
    elt2 = xml_append( doc, "logicalservice", name = "CondDBOnline" )
    xml_append( doc, "service", elt_ = elt2, name = self.CDB_infos[site]['Connection'] + "/lhcb_online_conddb",
               accessMode = "readonly", authentication = "password" )
    xmlfile = open( "dblookup.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def generate_authentication_file( self, site ):
    doc = impl.createDocument( None, "connectionlist", None )
    elt = xml_append( doc, "connection", name = self.CDB_infos[site]['Connection'] + "/lhcb_conddb" )
    xml_append( doc, "parameter", elt_ = elt, name = "user", value = self.CDB_infos[site]["Username"] )
    xml_append( doc, "parameter", elt_ = elt, name = "password", value = self.CDB_infos[site]["Password"] )
    elt2 = xml_append( doc, "role", name = "reader" )
    xml_append( doc, "parameter", elt_ = elt2, name = "user", value = self.CDB_infos[site]["Username"] )
    xml_append( doc, "parameter", elt_ = elt2, name = "password", value = self.CDB_infos[site]["Password"] )

    elt = xml_append( doc, "connection", name = self.CDB_infos[site]['Connection'] + "/lhcb_online_conddb" )
    xml_append( doc, "parameter", elt_ = elt, name = "user", value = self.CDB_infos[site]["Username"] )
    xml_append( doc, "parameter", elt_ = elt, name = "password", value = self.CDB_infos[site]["Password"] )
    elt2 = xml_append( doc, "role", name = "reader" )
    xml_append( doc, "parameter", elt_ = elt2, name = "user", value = self.CDB_infos[site]["Username"] )
    xml_append( doc, "parameter", elt_ = elt2, name = "password", value = self.CDB_infos[site]["Password"] )


    xmlfile = open( "authentication.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

  def generate_xml( self, site, time_, availability ):
    # Insert into DB
    Utils.unpack( insert_slsconddb( Site = site, Availability = availability, AccessTime = time_ ) )

    doc = impl.createDocument( "http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None )
    doc.documentElement.setAttribute( "xmlns", "http://sls.cern.ch/SLS/XML/update" )
    doc.documentElement.setAttribute( "xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance' )
    doc.documentElement.setAttribute( "xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd" )

    xml_append( doc, "id", site + "_" + "CondDB" )
    xml_append( doc, "availability", availability )
    xml_append( doc, "refreshperiod", self.getTestValue( "refreshperiod" ) )
    xml_append( doc, "validityduration", self.getTestValue( "validityduration" ) )
    elt2 = xml_append( doc, "data" )
    xml_append( doc, "numericvalue", str( time_ ), elt_ = elt2, name = "Time to access CondDB" )
    xml_append( doc, "textvalue", "ConditionDB access timex", elt_ = elt2 )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )


    xmlfile = open( self.xmlPath + site + "_" + "CondDB.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()



class LFCTest( TestBase ):
  def __init__( self, am ):
    super( LFCTest, self ).__init__( am )

    self.mirrors = [
                     'prod-lfc-lhcb-ro.cern.ch',
                     'lhcb-lfc.gridpp.rl.ac.uk',
                     'lfc-lhcb-ro.in2p3.fr',
                     'lhcb-lfc-fzk.gridka.de',
                     'lfc-lhcb.grid.sara.nl',
                     'lfclhcb.pic.es',
                     'lfc-lhcb-ro.cr.cnaf.infn.it',
                     'lfc-lhcb-ro.cern.ch',
                   ]

    self.master = 'lfc-lhcb.cern.ch'

    self.xmlPath = rootPath + "/" + self.getAgentValue( "webRoot" ) + self.getTestValue( "dir" )

    from DIRAC.Interfaces.API.Dirac import Dirac
    self.diracAPI = Dirac()
    self.workdir = am.am_getWorkDirectory()

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dir exist already, or cannot be created: do nothi

    _register = False
    try:
      lfn = self.runMasterTest()
      _register = True
    except Exception, e:
      pass

    if not _register:
      gLogger.error( 'Skipping tests, file not registered' )
      return S_ERROR( 'Error registering file' )

    counter = 0

    for mirror in self.mirrors:

      try:
        counter = self.runMirrorTest( lfn, mirror, counter )
        gLogger.info( '%s %s' % ( counter, mirror ) )
      except:
        gLogger.error( 'Exception on %s' % mirror )

    self.cleanMasterTest( lfn )


  def runMasterTest( self ):

    os.environ[ 'LFC_HOST' ] = self.master

    lfnDir = '/lhcb/test/lfc_mirror_test/streams_propagation_test'
    gridDir = '/grid' + lfnDir

    _create, _remove = False, False

    try:
      lfc2.lfc_rmdir( gridDir )
    except:
      pass

    try:

      lfc2.lfc_mkdir( gridDir , 0777 )
      _create = True
      gLogger.info( 'created %s' % gridDir )
      lfc2.lfc_rmdir( gridDir )
      _remove = True
      gLogger.info( 'removed %s' % gridDir )

    except ValueError:
      _lfcMsg = 'Error manipulating directory, are you sure it does not exist ?'
      gLogger.error( _lfcMsg )
    except Exception, e:
      gLogger.error( e )

    lfnPath = '/lhcb/test/lfc-replication/%s/' % self.master
    fileName = 'testFile.%s' % time.time()

    lfn = lfnPath + fileName
    fullPath = self.workdir + '/' + fileName
    diracSE = 'CERN-USER'

    gLogger.info( 'Getting time till file %s exists' % lfn )

    f = open( fullPath, 'w' )
    f.write( 'SLSAgent' )
    f.write( str( time.time() ) )
    f.close()

    gLogger.info( 'Registering file %s at %s' % ( lfn, diracSE ) )

    gLogger.info( fullPath )

    res = self.diracAPI.addFile( lfn, fullPath, diracSE )

    if not res[ 'OK' ]:
      gLogger.error( res[ 'Message' ] )
      res = False
    else:
      if res[ 'Value' ][ 'Successful' ].has_key( lfn ):
        res = True
      else:
        gLogger.warn( res[ 'Value' ] )
        res = False

    res = res and _create and _remove

    doc = impl.createDocument( "http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None )
    doc.documentElement.setAttribute( "xmlns", "http://sls.cern.ch/SLS/XML/update" )
    doc.documentElement.setAttribute( "xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance' )
    doc.documentElement.setAttribute( "xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd" )

    xml_append( doc, "id", "LHCb_LFC_Master" )
    xml_append( doc, "availability", ( res and 100 ) or 0 )
    xml_append( doc, "validityduration", "PT2H" )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )
    xml_append( doc, "notes", "Either 0 or 100, 0 no basic operations performed, 100 all working." )

    xmlfile = open( self.xmlPath + "LHCb_LFC_Master.xml", "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

    return lfn

  def runMirrorTest( self, lfn, mirror, counter ):

    if counter > 19:
      counter = 18

    while 1:

      fullLfn = '%s%s' % ( '/grid', lfn )
      value = lfc2.lfc_access( fullLfn, 0 )

      if value == 0 or counter == 20:
        break

      counter += 0.5
      time.sleep( 0.5 )

    availability = ( ( counter < 20 ) and 100 ) or 0

    doc = impl.createDocument( "http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None )
    doc.documentElement.setAttribute( "xmlns", "http://sls.cern.ch/SLS/XML/update" )
    doc.documentElement.setAttribute( "xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance' )
    doc.documentElement.setAttribute( "xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd" )

    xml_append( doc, "id", "LHCb_LFC_%s" % mirror )
    xml_append( doc, "availability", availability )
    xml_append( doc, "validityduration", "PT2H" )
    xml_append( doc, "timestamp", time.strftime( "%Y-%m-%dT%H:%M:%S" ) )
    xml_append( doc, "notes", "Either 0 or 100, 0 no basic operations performed, 100 all working." )

    xmlfile = open( self.xmlPath + "LHCb_LFC_%s.xml" % mirror, "w" )
    try:
      xmlfile.write( doc.toxml() )
    finally:
      xmlfile.close()

    return counter


  def cleanMasterTest( self, lfn ):

    try:

      res = self.diracAPI.removeFile( lfn )

      if not res['OK']:
        gLogger.error( res['Message'] )
        return False

      if res['Value']['Successful'].has_key( lfn ):
        return True

      gLogger.warn( res[ 'Value' ] )
      return False

    except Exception, e:
      gLogger.error( e )
      return False


class SLSAgent( AgentModule ):
  def initialize( self ):
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):

    # Future me, forgive me for this. TO BE Fixed.
    try:
      SpaceTokenOccupancyTest( self )
    except Exception, e:
      gLogger.warn( 'SpaceTokenOccupancyTest crashed with %s' % e )
    try:
      DIRACTest( self )
    except Exception, e:
      gLogger.warn( 'DIRACTest crashed with %s' % e )
    try:
      LOGSETest( self )
    except Exception, e:
      gLogger.warn( 'LOGSETest crashed with %s' % e )
    try:
      CondDBTest( self )
    except Exception, e:
      gLogger.warn( 'ConDBTest crashed with %s' % e )
    try:
      LFCTest( self )
    except Exception, e:
      gLogger.warn( 'LFCTest crashed with %s' % e )

    return S_OK()
