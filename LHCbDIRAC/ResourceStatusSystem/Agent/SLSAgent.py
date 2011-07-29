from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient  import RPCClient

from DIRAC.ResourceStatusSystem.Utilities import CS, Utils

import xml.dom
import time
import urlparse

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SLSAgent'

impl = xml.dom.getDOMImplementation()

def xml_append(doc, tag, value=None, elt=None, **kw):
  new_elt = doc.createElement(tag)
  for k in kw:
    new_elt.setAttribute(k, kw[k])
  if value != None:
    textnode = doc.createTextNode(str(value))
    new_elt.appendChild(textnode)
  if elt != None:
    return elt.appendChild(new_elt)
  else:
    return doc.documentElement.appendChild(new_elt)

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
    id_ = se + "_" + st
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

    try:
      xmlfile = open(self.xmlpath + id_ + ".xml", "w")
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

    # Dashboard
    try:
      dbfile = open(self.xmlpath + id_ + "_space_monitor", "w")
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

    try:
      xmlfile = open(self.xmlpath + "Framework_Gateway.xml", "w")
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

    try:
      xmlfile = open(self.xmlpath + system + "_" + service + ".xml", "w")
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

    try:
      xmlfile = open(self.xmlpath + site + "_" + service + ".xml", "w")
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()


class SLSAgent(AgentModule):

  def execute(self):

    # FIXME: Get xmlpath from CS
    SpaceTokenOccupancyTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/storage_space/")
    DIRACTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/dirac_services/")

    return S_OK()
