from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.ResourceStatusSystem.Utilities import CS, Utils

import xml.dom
import time
import lcg_util

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
  def __init__(self, site, space_token, endpoint, xmlpath):
    self.xmlpath      = xmlpath
    self.site         = site
    self.space_token  = space_token
    self.id           = site.split(".")[1] + "-" + space_token
    self.endpoint     = endpoint
    self.availability = 0
    self.free         = 0
    self.total        = 0
    self.guaranteed   = 0
    self.timestamp    = time.strftime("%Y-%m-%dT%H:%M:%S")
    self.validity     = 'PT0M'

    answer = lcg_util.lcg_stmd(space_token, endpoint, True, 0)
    if answer[0] == 0:
      output = answer[1][0]
      self.total      = float(output['totalsize']) / 2**40 # Bytes to Terabytes
      self.guaranteed = float(output['guaranteedsize']) / 2**40
      self.free       = float(output['unusedsize']) / 2**40
      self.validity   = 'PT13H'
      self.availability = 100 if self.free > 4 else (self.free*100/self.total if self.total != 0 else 0)

    self.xml()
    self.dashboard()

  def xml(self):
    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                               "serviceupdate",
                               None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    doc.documentElement.setAttribute("xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance')
    doc.documentElement.setAttribute("xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd")

    xml_append(doc, "id", self.id)
    xml_append(doc, "availability", self.availability)
    elt = xml_append(doc, "availabilitythresholds")
    # FIXME: Put the thresholds into the CS
    xml_append(doc, "threshold", value=15, elt=elt, level="available")
    xml_append(doc, "threshold", value=10, elt=elt, level="affected")
    xml_append(doc, "threshold", value=5, elt=elt, level="degraded")
    xml_append(doc, "availabilityinfo", "Free="+str(self.free)+" Total="+str(self.total))
    xml_append(doc, "availabilitydesc", "FreeSpace less than 4TB implies 0%-99% ;\
  FreeSpace greater than  4TB is always  100%")
    xml_append(doc, "refreshperiod", "PT27M")
    xml_append(doc, "validityduration", self.validity)
    elt = xml_append(doc, "data")
    elt2 = xml_append(doc, "grp", name="Space occupancy", elt=elt)
    xml_append(doc, "numericvalue", value=str(self.total-self.free), elt=elt2, name="Consumed")
    xml_append(doc, "numericvalue", value=str(self.total), elt=elt2, name="Capacity")
    xml_append(doc, "numericvalue", value=str(self.free), elt=elt, name="Free space")
    xml_append(doc, "numericvalue", value=str(self.total-self.free), elt=elt, name="Occupied space")
    xml_append(doc, "numericvalue", value=str(self.total), elt=elt, name="Total space")
    xml_append(doc, "textvalue", "Storage space for the specific space token", elt=elt)
    xml_append(doc, "timestamp", self.timestamp)

    try:
      xmlfile = open(self.xmlpath + self.id + ".xml", "w")
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def dashboard(self):
    try:
      dbfile = open(self.xmlpath + self.id + "_space_monitor", "w")
      dbfile.write(self.id + ' ' + str(self.total) + ' ' + str(self.guaranteed) + ' ' + str(self.free) + '\n')
    finally:
      dbfile.close()

class DIRACTest(object):
  def __init__(self, xmlpath):
    from DIRAC import gConfig
    self.setup = gConfig.getValue('DIRAC/Setup')
    self.xmlpath = xmlpath

    # Run xml_gw
    self.xml_gw()

    # For each service, run XML...
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
      import urlparse
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


class SLSAgent(AgentModule):

  def execute(self):

    # SpaceTokenOccupancyTest #########################

    site_of_localSE = Utils.dict_invert(CS.getTypedDictRootedAt(root="", relpath="/Resources/SiteLocalSEMapping"))
    storageElts = CS.getTypedDictRootedAt(root="", relpath="/Resources/StorageElements")

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

    for k in res:
      for st in res[k]:
        # FIXME: Add xmlpath to CS
        SpaceTokenOccupancyTest(k, st, res[k][st],
                                xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/storage_space/")

    #################################################

    return S_OK()
