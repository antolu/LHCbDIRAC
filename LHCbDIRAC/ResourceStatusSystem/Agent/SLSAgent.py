from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient  import RPCClient

from DIRAC.ResourceStatusSystem.Utilities import CS, Utils
from DIRAC.ResourceStatusSystem.Utilities.Utils import xml_append

import xml.dom, xml.sax
import time
import urlparse, urllib

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SLSAgent'

impl = xml.dom.getDOMImplementation()

class SpaceTokenOccupancyTest(object):
  def __init__(self, xmlpath):
    self.xmlpath      = xmlpath
    self.SEs          = self.generate_SE_dict()

    for se in self.SEs:
      for st in self.SEs[se]:
        self.generate_xml_and_dashboard(se, st, self.SEs[se][st])


  @staticmethod
  def generate_SE_dict():
    """Returns a dict that associate a site and a space-token to an URL.
    For example: a['LCG.CERN.ch']['CERN-RAW'] == 'https://....'"""
    site_of_localSE = Utils.dict_invert(
      CS.getTypedDictRootedAt(root="", relpath="/Resources/SiteLocalSEMapping"))
    storageElts = CS.getTypedDictRootedAt(root="",
                                          relpath="/Resources/StorageElements")
    res = {}
    for k in storageElts:

      try:
        infos = storageElts[k]['AccessProtocol.1']
        URL = "httpg://" + infos['Host'] + ":" + str(infos['Port']) + infos['WSUrl']
      except KeyError: # No AccessProtocol.1, abnormal storage element
        continue
      except TypeError: # Not a storage element entry
        continue

      try:
        for i in site_of_localSE[k]:
          try:
            res[i][infos['SpaceToken']] = URL
          except KeyError:
            res[i] = {}
            res[i][infos['SpaceToken']] = URL
      except KeyError:
        pass

    return res

  def generate_xml_and_dashboard(self, se, st, url,fake=True):
    id_          = se + "_" + st
    total        = 0
    guaranteed   = 0
    free         = 0
    validity     = 'PT0M'
    availability = 0

    if not fake:
      import lcg_util
      answer = lcg_util.lcg_stmd(st, url, True, 0)
      if answer[0] == 0:
        output       = answer[1][0]
        total        = float(output['totalsize']) / 2**40 # Bytes to Terabytes
        guaranteed   = float(output['guaranteedsize']) / 2**40
        free         = float(output['unusedsize']) / 2**40
        availability = 100 if free > 4 else (free*100/total if total != 0 else 0)
        validity     = 'PT13H'

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                               "serviceupdate",
                               None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    doc.documentElement.setAttribute("xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance')
    doc.documentElement.setAttribute("xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd")

    xml_append(doc, "id", id_)
    xml_append(doc, "availability", availability)
    elt = xml_append(doc, "availabilitythresholds")
    # FIXME: Put the thresholds into the CS
    xml_append(doc, "threshold", value=15, elt=elt, level="available")
    xml_append(doc, "threshold", value=10, elt=elt, level="affected")
    xml_append(doc, "threshold", value=5, elt=elt, level="degraded")
    xml_append(doc, "availabilityinfo", "Free="+str(free)+" Total="+str(total))
    xml_append(doc, "availabilitydesc", "FreeSpace less than 4TB implies 0%-99% ;\
  FreeSpace greater than  4TB is always  100%")
    xml_append(doc, "refreshperiod", "PT27M")
    xml_append(doc, "validityduration", validity)
    elt = xml_append(doc, "data")
    elt2 = xml_append(doc, "grp", name="Space occupancy", elt=elt)
    xml_append(doc, "numericvalue", value=str(total-free), elt=elt2, name="Consumed")
    xml_append(doc, "numericvalue", value=str(total), elt=elt2, name="Capacity")
    xml_append(doc, "numericvalue", value=str(free), elt=elt, name="Free space")
    xml_append(doc, "numericvalue", value=str(total-free), elt=elt, name="Occupied space")
    xml_append(doc, "numericvalue", value=str(total), elt=elt, name="Total space")
    xml_append(doc, "textvalue", "Storage space for the specific space token", elt=elt)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    xmlfile = open(self.xmlpath + id_ + ".xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

    # Dashboard
    dbfile = open(self.xmlpath + id_ + "_space_monitor", "w")
    try:
      dbfile.write(id_ + ' ' + str(total) + ' ' + str(guaranteed) + ' ' + str(free) + '\n')
    finally:
      dbfile.close()


class DIRACTest(object):
  def __init__(self, xmlpath):
    self.setup = CS.getValue('DIRAC/Setup')
    self.xmlpath = xmlpath

    # Run xml_gw
    self.xml_gw()

    # For each service of each system, run xml_sensors...
    systems = CS.getTypedDictRootedAt(root="", relpath="/Systems")
    services = []
    for s in systems:
      try:
        sys, srv = systems[s][self.setup]['URLs'].split("/")[-2:]
        services.append((sys, srv))
      except KeyError:
        pass

    for (sys, srv) in services:
      self.xml_sensor(sys, srv)

    # For each T0/T1 VO-BOXes, run xml_t1_sensors...
    request_management_urls = Utils.list_(CS.getValue("/Systems/RequestManagement/Development/URLs/allURLS"))
    configuration_urls      = Utils.list_(CS.getValue("/DIRAC/Configuration/Servers"))
    for url in request_management_urls + configuration_urls:
      self.xml_t1_sensor(url)

  # XML GENERATORS

  def xml_gw(self):
    try:
      sites = gConfig.getSections('/Resources/Sites/LCG')['Value']
    except KeyError:
      # FIXME: log, etc...
      print "SLSAgent, DIRACTest: Unable to query CS."
      sites = []

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                               "serviceupdate",
                               None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")

    xml_append(doc, "id", "Framework_Gateway")
    # FIXME: Add to CS
    xml_append(doc, "webpage",
               "https://lhcbweb.pic.es/DIRAC/LHCb-Production/diracAdmin/info/general/diracOverview")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    if sites == []:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes",
                 "Retrieved 0 sites out of the Configuration Service.\
 Please check the CS is up and running otherwise is the Gateway")

    else:
      xml_append(doc, "availability", 100)
      xml_append(doc, "notes", "Retrieved "+str(len(sites))+"\
 sites out of the Configuration Service through the Gateway")

    xmlfile = open(self.xmlpath + "Framework_Gateway.xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def xml_sensor(self, system, service):
    from DIRAC.Interfaces.API.Dirac import Dirac
    dirac = Dirac()

    res = dirac.ping(system, service)
    assert(res)

    try:
      host = urlparse.urlparse(res['Value']['service url']).netloc.split(":")[0]
    except KeyError:
      host = "unknown.cern.ch"

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                               "serviceupdate",
                               None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", system + "_" + service)
    xml_append(doc, "webpage", "http://lemonweb.cern.ch/lemon-web/info.php?entity=" + host)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    if res['OK']:
      res = res['Value']
      xml_append(doc, "availability", 100)
      elt = xml_append(doc, "data")
      xml_append(doc, "numericvalue", res['service uptime'], elt=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res['host uptime'], elt=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")
      xml_append(doc, "numericvalue", res['load'].split()[0], elt=elt,
                 name="Load", desc="Instantaneous load")
      xml_append(doc, "notes", "Service " + res['service url'] + " completely up and running")

    else:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes", res['Message'])

    xmlfile = open(self.xmlpath + system + "_" + service + ".xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def xml_t1_sensor(self, url):
    parsed = urlparse.urlparse(url)
    system, service = parsed.path.strip("/").split("/")
    site = parsed.netloc.split(":")[0]

    pinger = RPCClient(url)
    res = pinger.ping()

    if system == "RequestManagement":
      res2 = pinger.getDBSummary()

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", site + "_" + service)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    if res['OK']:
      res = res['Value']
      xml_append(doc, "availability", 100)
      xml_append(doc, "notes", "Service " + url + " completely up and running")

      elt = xml_append(doc, "data")
      xml_append(doc, "numericvalue", res['service uptime'], elt=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res['host uptime'], elt=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")

      if system == "RequestManagement":
        for k,v in res2["Value"].items():
          xml_append(doc, "numericvalue", v["Assigned"], elt=elt,
                     name=k + " - Assigned", desc="Number of Assigned " + k + "requests")
          xml_append(doc, "numericvalue", v["Waiting"], elt=elt,
                     name=k + " - Waiting", desc="Number of Waiting " + k + "requests")
          xml_append(doc, "numericvalue", v["Done"], elt=elt,
                     name=k + " - Done", desc="Number of Done " + k + "requests")

    else:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes", res['Message'])

    xmlfile = open(self.xmlpath + site + "_" + service + ".xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

class LOGSETest(object):
  def __init__(self, xmlpath):
    self.xmlpath = xmlpath
    self.entities = "volhcb15" # FIXME: Get from CS
    self.lemon_url = "http://lemon-gateway.cern.ch/lemon-xml/xml_gateway.php" # FIXME: Get from CS

    # Generate XML files

    self.partition("log_se_partition.xml")
    self.gridftpd("log_se_gridftpd.xml")
    self.cert("log_se_cert.xml")
    self.httpd("log_se_httpd.xml")

  # LOG SE Partition
  def partition(self, filename):
    input_xml = self.getxml(entities=self.entities, metrics=9104) # FIXME: Get metrics from CS
    handler = self.LemonHandler()
    xml.sax.parse(input_xml, handler)

    for d in handler.data:
      if d['data'][0] == "/data":
        ts = int(d['ts'])
        space = d['data'][3]
        percent = int(d['data'][4])

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", "log_se_partition")
    xml_append(doc, "validityduration", "PT12H")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)))
    xml_append(doc, "availability", (100 if percent < 90 else (5 if percent < 99 else 0)))
    elt = xml_append(doc, "data")
    xml_append(doc, "numericvalue", percent, elt=elt, name="LogSE data partition used")
    xml_append(doc, "numericvalue", space, elt=elt, name="Total space on data partition")

    xmlfile = open(self.xmlpath + filename, "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  # LOG SE GridFTPd
  def gridftpd(self, filename):
    input_xml = self.getxml(entities=self.entities, metrics=34) # FIXME: Get metrics from CS
    handler = self.LemonHandler()
    xml.sax.parse(input_xml, handler)
    data = handler.data[0]

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", "log_se_gridftp")
    xml_append(doc, "validityduration", "PT2H")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(data['ts'])))
    xml_append(doc, "availability", int(round(float(data['data'][0])*100)))

    xmlfile = open(self.xmlpath + filename, "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  # LOG SE Cert
  def cert(self, filename):
    input_xml = self.getxml(entities=self.entities, metrics=810) # FIXME: Get metrics from CS
    handler = self.LemonHandler()
    xml.sax.parse(input_xml, handler)
    data = handler.data[0]

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", "log_se_cert")
    xml_append(doc, "validityduration", "PT24H")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(data['ts'])))
    xml_append(doc, "availability", int(round(float(data['data'][0])*100)))

    xmlfile = open(self.xmlpath + filename, "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  # LOG SE HTTPd
  def httpd(self, filename):
    input_xml = self.getxml(entities=self.entities, metrics=4019) # FIXME: Get metrics from CS
    handler = self.LemonHandler()
    xml.sax.parse(input_xml, handler)
    data = handler.data[0]

    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    xml_append(doc, "id", "log_se_httpd")
    xml_append(doc, "validityduration", "PT2H")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(data['ts'])))
    xml_append(doc, "availability", int(round(float(data['data'][0])*100)))

    xmlfile = open(self.xmlpath + filename, "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def getxml(self, **kw):
    params = urllib.urlencode(kw)
    xml = urllib.urlopen(self.lemon_url, params)
    return xml

  class LemonHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
      xml.sax.handler.ContentHandler.__init__(self)
      self.node     = ""
      self.metric   = -1
      self.ts       = 0
      self.cur_list = []
      self.inside_d = False
      self.data     = []

    def startElement(self, name, attrs):
      if name == "data":
        self.node = attrs.getValue("node")

      elif name == "metric":
        self.metric = int(attrs.getValue("id"))

      elif name == "d":
        self.inside_d = True

      elif name == "r":
        self.ts = int(attrs.getValue("ts"))
        self.cur_list = []

    def endElement(self, name):
      if name == "r":
        self.data.append({ 'node':self.node, 'metric':self.metric, 'ts':self.ts, 'data':self.cur_list })

      elif name == "d":
        self.inside_d = False

    def characters(self, content):
      if self.inside_d:
        self.cur_list.append(content)

class SLSAgent(AgentModule):

  def execute(self):

    # FIXME: Get xmlpath from CS
    SpaceTokenOccupancyTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/storage_space/")
    DIRACTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/dirac_services/")
    LOGSETest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/log_se/")
    return S_OK()
