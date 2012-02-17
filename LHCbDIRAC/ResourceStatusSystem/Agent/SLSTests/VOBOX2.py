################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gConfig 
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from LHCbDIRAC.ResourceStatusSystem.Agent.SLSTests.TestBase import TestBase

import sys, time, urlparse

class TestModule( TestBase ):

  def launchTest( self ):
    '''
      Main method
    '''
    
    '''
     # Development hardcoded !!!!!!
    '''
    
    rm_urls = gConfig.getValue( '/Systems/RequestManagement/Development/URLs/allURLS', [] )
    cs_urls = gConfig.getServersList()
    
    gLogger.info( 'Discovered %s request management urls' % len( rm_urls ) )
    gLogger.info( 'Discovered %s configuration urls' % len( cs_urls ) )
    
    for url in rm_urls + cs_urls:
    
      try:
        
        self.__launchTest( url )
        
      except Exception, e:
        gLogger.exception( 'VOBOX for %s.\n %s' % ( url, e ) ) 
    
  def __launchTest( self, url ):
    
    availability, suptime, muptime = 0, 0, 0
    
    parsed = urlparse.urlparse( url )
    
    if sys.version_info >= (2,6):
      system, _service = parsed.path.strip("/").split("/")
      site = parsed.netloc.split(":")[0]
    else:
      site, system, _service = parsed[2].strip("/").split("/")
      site = site.split(":")[0]

    pinger = RPCClient( url )
    res    = pinger.ping()  

    if res[ 'OK' ]:
      
      res = res[ 'Value' ]
       
      availability = 100
      suptime      = res.get( 'service uptime', 0 )
      muptime      = res.get( 'host uptime', 0 )
      notes        = 'Service %s completely up and running' % url
    
    else:
      
      notes = res[ 'Message' ]  

    xmlList = []
    xmlList.append( { 'tag' : 'id', 'nodes' : '%s_%s' % ( site, system ) } )
    xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )
    xmlList.append( { 'tag' : 'notes', 'nodes' : notes } )
    
    dataNodes = []
    dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Seconds since last restart of service' ),
                                                            ( 'name', 'Service Uptime') ], 'nodes' : suptime } )
    dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Seconds since last restart of machine' ),
                                                            ( 'name', 'Host Uptime') ], 'nodes' : muptime } )
    
    if system == 'RequestManagement':
      res = pinger.getDBSummary()
      if not res[ 'OK' ]:
        gLogger.warn( 'Error getting DB Summary for %s \n. %s' % ( url, res[ 'Message' ] ) )
      else:  
    
        # WTF is this !! ??
    
        for k,v in res.items():
          
          dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Assigned %s requests' % k ),
                                                                  ( 'name', '%s - Assigned' % k) ], 
                              'nodes' : v[ 'Assigned' ] } )
          
          dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Waiting %s requests' % k ),
                                                                  ( 'name', '%s - Waiting' % k) ], 
                              'nodes' : v[ 'Assigned' ] } )
          
          dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Done %s requests' % k ),
                                                                  ( 'name', '%s - Waiting' % k) ], 
                              'nodes' : v[ 'Done' ] } )
              
    xmlList.append( { 'tag' : 'data', 'nodes' : dataNodes } )
    xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) })
    
    self.writeXml( xmlList, '%s_%s.xml' % ( site, system ) )
    
################################################################################
  
#    parsed = urlparse.urlparse(url)
#    if sys.version_info >= (2,6):
#      system, _service = parsed.path.strip("/").split("/")
#      site = parsed.netloc.split(":")[0]
#    else:
#      site, system, _service = parsed[2].strip("/").split("/")
#      site = site.split(":")[0]
#
#    pinger = RPCClient(url)
#    res = pinger.ping()

    if system == "RequestManagement":
      res2 = Utils.unpack(pinger.getDBSummary())

#    doc = gen_xml_stub()
#    xml_append(doc, "id", site + "_" + system)
#    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S"))

    if res['OK']:
      res = res['Value']

      xml_append(doc, "availability", 100)
      xml_append(doc, "notes", "Service " + url + " completely up and running")

      elt = xml_append(doc, "data")
      xml_append(doc, "numericvalue", res.get('service uptime', 0), elt_=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res.get('host uptime', 0), elt_=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")

      Utils.unpack(insert_slst1service(Site=site, System=system, Availability=100,
                                       Version=res.get("version", "unknown"),
                                       ServiceUptime=int(res.get('service uptime', 0)),
                                       HostUptime=int(res.get('host uptime', 0)),
                                       Message=("Service " + url + " completely up and running")))

      if system == "RequestManagement":
        for k,v in res2.items():
          xml_append(doc, "numericvalue", v["Assigned"], elt_=elt,
                     name=k + " - Assigned", desc="Number of Assigned " + k + " requests")
          xml_append(doc, "numericvalue", v["Waiting"], elt_=elt,
                     name=k + " - Waiting", desc="Number of Waiting " + k + " requests")
          xml_append(doc, "numericvalue", v["Done"], elt_=elt,
                     name=k + " - Done", desc="Number of Done " + k + " requests")

          Utils.unpack(insert_slsrmstats(Site=site, System=system, Name=k,
                                         Assigned=int(v["Assigned"]),
                                         Waiting=int(v["Waiting"]),
                                         Done=int(v["Done"])))

      gLogger.info("%s/%s successfully pinged" % (site, system))

    else:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes", res['Message'])
      Utils.unpack(insert_slst1service(Site=site, System=system, Availability=0,
                          Message=res["Message"]))

      gLogger.info("%s/%s does not respond to ping" % (site, system))

    xmlfile = open(self.xmlPath + site + "_" + system + ".xml", "w")
    try:
      xmlfile.write(doc.toxml())
    finally:
      xmlfile.close()  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF