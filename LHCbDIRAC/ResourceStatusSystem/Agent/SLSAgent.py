from DIRAC import gLogger, gConfig, S_OK

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.DISET.RPCClient                             import RPCClient

from LHCbDIRAC.ResourceStatusSystem.Utilities                   import CS, Utils
from LHCbDIRAC.ResourceStatusSystem.Utilities.Utils             import xml_append

# For replicas test

from DIRAC.Core.Utilities.File                    import makeGuid

# For caching to DB
from LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

import xml.dom, xml.sax
import time
import urlparse, urllib
import sys, re, os, subprocess

__RCSID__ = "$Id$"

AGENT_NAME = 'ResourceStatus/SLSAgent'

impl = xml.dom.getDOMImplementation()
xml_re = re.compile('>\n\s+([^<>\s].*?)\n\s+</', re.DOTALL)

#### Helper functions to send a warning mail to a site (for space-token test)

def get_pledged_value_for_token(st):
  val = gConfig.getValue("/Resources/StorageElements/"+st+"/PledgedSpace")
  return (val if val != None else 0)

def contact_mail_of_site(site):
  infos = CS.getTypedDictRootedAt(root="", relpath="/Resources/Sites/LCG")
  try:
    return infos[site]['Mail']
  except KeyError:
    gLogger.warn("Unable to get contact mail for site %s" % site)
    return ""

def send_mail_to_site(site, pledged, token, total):
  from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
  nc = NotificationClient()
  subject = "Site %s provide insufficient space for space-token %s" % (site, token)
  body =  """
Hi ! Our RSS monitoring systems informs us that your site %s has
pledged %d TB to the space-token %s, but in reality, only %d TB of
space is available. Thanks to solve the problem if possible.

""" % (site, pledged, token, total)
  address = contact_mail_of_site(site)
  if address != "":
    res = nc.sendMail(address, subject, body)
    if res['OK'] == False:
      gLogger.warn("Unable to send mail to site %s: %s" % (site, res['Message']))

class SpaceTokenOccupancyTest(object):
  def __init__(self, xmlpath):
    self.xmlpath      = xmlpath
    self.SEs          = self.generate_SE_dict()
    self.rmDB         = ResourceManagementDB()

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

  def generate_xml_and_dashboard(self, se, st, url,fake=False):
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
      else:
        gLogger.info("StorageSpace: lcg_util.lcg_stmd('%s', '%s', True, 0) = (%d, %s)" % (st, url, answer[0], answer[1]))

    else:
      gLogger.warn("SpaceTokenOccupancyTest runs in fake mode, values are not real ones.")

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
    xml_append(doc, "threshold", value_=15, elt_=elt, level="available")
    xml_append(doc, "threshold", value_=10, elt_=elt, level="affected")
    xml_append(doc, "threshold", value_=5, elt_=elt, level="degraded")
    xml_append(doc, "availabilityinfo", "Free="+str(free)+" Total="+str(total))
    xml_append(doc, "availabilitydesc", "FreeSpace less than 4TB implies 0%-99% ;\
  FreeSpace greater than  4TB is always  100%")
    xml_append(doc, "refreshperiod", "PT27M")
    xml_append(doc, "validityduration", validity)
    elt = xml_append(doc, "data")
    elt2 = xml_append(doc, "grp", name="Space occupancy", elt_=elt)
    xml_append(doc, "numericvalue", value_=str(total-free), elt_=elt2, name="Consumed")
    xml_append(doc, "numericvalue", value_=str(total), elt_=elt2, name="Capacity")
    xml_append(doc, "numericvalue", value_=str(free), elt_=elt, name="Free space")
    xml_append(doc, "numericvalue", value_=str(total-free), elt_=elt, name="Occupied space")
    xml_append(doc, "numericvalue", value_=str(total), elt_=elt, name="Total space")
    xml_append(doc, "textvalue", "Storage space for the specific space token", elt_=elt)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    self.rmDB.updateSLSStorage(se, st, availability, "PT27M", validity, total, guaranteed, free)
    gLogger.info("SpaceTokenOccupancyTest: %s-%s done." % (se, st))

    xmlfile = open(self.xmlpath + id_ + ".xml", "w")
    try:
      uglyXml = doc.toprettyxml(indent="  ", encoding="utf-8")
      prettyXml = xml_re.sub('>\g<1></', uglyXml)
      xmlfile.write(prettyXml)
    finally:
      xmlfile.close()

    # Send notifications
    pledged = get_pledged_value_for_token(st)
    if not fake and total < pledged:
      send_mail_to_site(se, pledged, st, total)


    # Dashboard
    dbfile = open(self.xmlpath + id_ + "_space_monitor", "w")
    try:
      dbfile.write(id_ + ' ' + str(total) + ' ' + str(guaranteed) + ' ' + str(free) + '\n')
    finally:
      dbfile.close()


class DIRACTest(object):
  def __init__(self, xmlpath):
    self.setup     = CS.getValue('DIRAC/Setup')
    self.setupDict = CS.getTypedDictRootedAt(root="/DIRAC/Setups", relpath=self.setup)
    self.xmlpath   = xmlpath
    self.rmDB      = ResourceManagementDB()

    self.xml_gw()
    self.run_xml_sensors()
    self.run_t1_xml_sensors()

  def run_xml_sensors(self):
    # For each service of each system, run xml_sensors...
    systems = CS.getTypedDictRootedAt(root="", relpath="/Systems")
    discovered_services = []
    for sys in systems:
      try:
        services = systems[sys][self.setupDict[sys]]['Services']
        for k in services:
          discovered_services.append((sys, k))
      except KeyError:
        try:
          gLogger.warn("DIRACTest: No /Systems/%s/%s/Services in CS." % (sys, self.setupDict[sys]))
        except KeyError:
          gLogger.warn("DIRACTest: No /Systems/%s in CS." % sys)

    gLogger.info("DIRACTest: discovered %d services" % len(discovered_services))

    for (sys, srv) in discovered_services:
      self.xml_sensor(sys, srv)

  def run_t1_xml_sensors(self):
    # For each T0/T1 VO-BOXes, run xml_t1_sensors...
    request_management_urls = Utils.list_(CS.getValue("/Systems/RequestManagement/Development/URLs/allURLS"))
    configuration_urls      = Utils.list_(CS.getValue("/DIRAC/Configuration/Servers"))
    gLogger.info("DIRACTest: discovered %d request management url(s) and %d configuration url(s)"
                 % (len(request_management_urls),len(configuration_urls)))
    for url in request_management_urls + configuration_urls:
      self.xml_t1_sensor(url)

  # XML GENERATORS

  def xml_gw(self):
    try:
      sites = gConfig.getSections('/Resources/Sites/LCG')['Value']
    except KeyError:
      gLogger.error("SLSAgent, DIRACTest: Unable to query CS")
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
      xml_append(doc, "numericvalue", res['service uptime'], elt_=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res['host uptime'], elt_=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")
      xml_append(doc, "numericvalue", res['load'].split()[0], elt_=elt,
                 name="Load", desc="Instantaneous load")
      xml_append(doc, "notes", "Service " + res['service url'] + " completely up and running")

      # Fill database
      self.rmDB.updateSLSServices(system, service, 100,
                                  res['service uptime'], res['host uptime'], res['load'].split()[0])
      gLogger.info("%s/%s successfully pinged" % (system, service))

    else:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes", res['Message'])
      self.rmDB.updateSLSServices(system, service, 0, Utils.SQLValues.null, Utils.SQLValues.null, Utils.SQLValues.null)
      gLogger.info("%s/%s does not respond to ping" % (system, service))

    xmlfile = open(self.xmlpath + system + "_" + service + ".xml", "w")
    try:
      uglyXml = doc.toprettyxml(indent="  ", encoding="utf-8")
      prettyXml = xml_re.sub('>\g<1></', uglyXml)
      xmlfile.write(prettyXml)
    finally:
      xmlfile.close()



  def xml_t1_sensor(self, url):
    parsed = urlparse.urlparse(url)
    if sys.version_info >= (2,6):
      system, service = parsed.path.strip("/").split("/")
      site = parsed.netloc.split(":")[0]
    else:
      site, system, service = parsed[2].strip("/").split("/")
      site = site.split(":")[0]

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
      xml_append(doc, "numericvalue", res['service uptime'], elt_=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res['host uptime'], elt_=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")

      self.rmDB.updateSLST1Services(site, service, 100, res['service uptime'], res['host uptime'])

      if system == "RequestManagement":
        for k,v in res2["Value"].items():
          xml_append(doc, "numericvalue", v["Assigned"], elt_=elt,
                     name=k + " - Assigned", desc="Number of Assigned " + k + "requests")
          xml_append(doc, "numericvalue", v["Waiting"], elt_=elt,
                     name=k + " - Waiting", desc="Number of Waiting " + k + "requests")
          xml_append(doc, "numericvalue", v["Done"], elt_=elt,
                     name=k + " - Done", desc="Number of Done " + k + "requests")

      gLogger.info("%s/%s successfully pinged" % (site, service))

    else:
      xml_append(doc, "availability", 0)
      xml_append(doc, "notes", res['Message'])
      self.rmDB.updateSLST1Services(site, service, 0, Utils.SQLValues.null, Utils.SQLValues.null)

      gLogger.info("%s/%s does not respond to ping" % (site, service))

    xmlfile = open(self.xmlpath + site + "_" + service + ".xml", "w")
    try:
      uglyXml = doc.toprettyxml(indent="  ", encoding="utf-8")
      prettyXml = xml_re.sub('>\g<1></', uglyXml)
      xmlfile.write(prettyXml)
    finally:
      xmlfile.close()

class LOGSETest(object):
  def __init__(self, xmlpath):
    self.xmlpath = xmlpath
    self.entities = "volhcb15" # FIXME: Get from CS
    self.lemon_url = "http://lemon-gateway.cern.ch/lemon-xml/xml_gateway.php" # FIXME: Get from CS
    self.rmDB      = ResourceManagementDB()

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
    xml_append(doc, "numericvalue", percent, elt_=elt, name="LogSE data partition used")
    xml_append(doc, "numericvalue", space, elt_=elt, name="Total space on data partition")

    self.rmDB.updateSLSLogSE("partition", "PT12H", (100 if percent < 90 else (5 if percent < 99 else 0)),
                             percent, space)
    gLogger.info("LogSE partition test done")

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

    self.rmDB.updateSLSLogSE("gridftp", "PT2H", int(round(float(data['data'][0])*100)),
                             Utils.SQLValues.null, Utils.SQLValues.null)
    gLogger.info("LogSE gridftp test done")


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

    self.rmDB.updateSLSLogSE("cert", "PT24H", int(round(float(data['data'][0])*100)),
                             Utils.SQLValues.null, Utils.SQLValues.null)
    gLogger.info("LogSE cert test done")

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

    self.rmDB.updateSLSLogSE("httpd", "PT2H", int(round(float(data['data'][0])*100)),
                             Utils.SQLValues.null, Utils.SQLValues.null)
    gLogger.info("LogSE httpd test done")

    xmlfile = open(self.xmlpath + filename, "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def getxml(self, **kw):
    params = urllib.urlencode(kw)
    xml_ = urllib.urlopen(self.lemon_url, params)
    return xml_

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
        self.data.append({ 'node'   : self.node,
                           'metric' : self.metric,
                           'ts'     : self.ts,
                           'data'   : self.cur_list })

      elif name == "d":
        self.inside_d = False

    def characters(self, content):
      if self.inside_d:
        self.cur_list.append(content)

class LFCReplicaTest(object):
  def __init__(self, path, timeout, fake=False):
    self.path       = path
    self.timeout    = timeout
    self.cfg        = CS.getTypedDictRootedAt(
      root="", relpath="/Resources/FileCatalogs/LcgFileCatalogCombined")
    self.ro_mirrors = []

    if not fake: # If not fake, run it!
      import lfc
      from DIRAC.Resources.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
      self.master_lfc = LcgFileCatalogClient(self.cfg['LcgGfalInfosys'], self.cfg['MasterHost'])
      self.run_test()
    else:
      gLogger.warn("LFCReplicaTest runs in fake mode, nothing is done")

  def run_test(self):
  # Load the list of mirrors
    for site in self.cfg:
      if type(self.cfg[site]) == dict:
        self.ro_mirrors.append(self.cfg[site]['ReadOnly'])

    # For all the mirrors, do the unit test:
    for mirror in self.ro_mirrors:
      lfn =  '/lhcb/test/lfc-replication/%s/testFile.%s' % (mirror,time.time())
      if not self.register_dummy(lfn):
        gLogger.error("Error: "+lfn+" is already in the master or can't be registered \
there...check your voms role is prodution \n")
        continue

      # Try to open a session
      if lfc.lfc_startsess(mirror, "DIRAC_test"): # rc != 0 means error
        continue

      # Measure time to create replica and write XML file
      time_to_create = self.time_to_create_rep(lfn)
      fd = open(self.path + mirror + ".timing", "w")
      try:
        fd.write("%s" % time_to_create)
      finally:
        fd.close()

      # Measure time to find a replica
      if time_to_create == self.timeout:
        time_to_find = self.timeout
      else:
        time_to_find = self.time_to_find_rep(lfn)

      lfc.lfc_endsess()

      # Measure time to delete a replica
      removed = self.remove_replica(lfn)
      if removed:
        # Try to open a session
        if lfc.lfc_startsess(mirror, "DIRAC_test"): # rc != 0 means error
          continue
        time_to_remove = self.time_to_remove_rep(lfn)
        lfc.lfc_endsess()
        gLogger.always('%s %s %s %s' % (mirror, time_to_create, time_to_find, time_to_remove))

  @staticmethod
  def pfn_of_token(SE):
    cfg = CS.getTypedDictRootedAt(
      root="",
      relpath="/Resources/StorageElements/" + SE + "/AccessProtocol.1")
    return "srm://" + cfg['Host'] + cfg['Path']

  def register_dummy(self, lfn, size=0, SE="CERN-USER", guid=makeGuid(), chksum=""):
    pfn = self.pfn_of_token(SE) + lfn
#    res = self.master_lfc.addFile(lfn, pfn, size, SE, guid, chksum)
    res = self.master_lfc.addFile(lfn)

    if not res['OK']:
      gLogger.info("register_dummy: %s" % res['Message'])
    return res['OK'] and res['Value']['Successful'].has_key(lfn)

  def get_replica(self, lfn):
    reps = {}
    rc, replica_objs = lfc.lfc_getreplica("/grid" + lfn, "", "")
    if rc:
      gLogger.error(lfc.sstrerror(lfc.cvar.serrno))
    else:
      for r in replica_objs:
        SE = r.host
        pfn = r.sfn.strip()
      reps[SE] = pfn
    return reps

  def remove_replica(self, lfn, SE="CERN-USER"):
    pfn = self.pfn_of_token(SE) + lfn
    res = self.master_lfc.removeReplica((lfn, pfn, SE))
    if res['OK'] == False:
      gLogger.info("remove_replica: %s" % res['Message'])
    return res['OK'] and res['Value']['Successful'].has_key(lfn)

  def remove_file(self, lfn):
    res = self.master_lfc.removeFile(lfn)
    return res['OK'] and res['Value']['Successful'].has_key(lfn)

  def time_to_find_rep(self, lfn):
    start_time = time.time()
    while True:
      reps = self.get_replica(lfn)
      if reps.has_key('CERN-USER'):
        return time.time() - start_time
      else:
        if (time.time() - start_time < self.timeout) : time.sleep(0.1)
        else                                         : return self.timeout

  def time_to_create_rep(self, lfn):
    start_time = time.time()
    while True:
      if lfc.lfc_access("/grid" + lfn, 0) == 0:
        return time.time() - start_time
      else:
        if (time.time() - start_time < self.timeout) : time.sleep(0.1)
        else                                         : return self.timeout

  def time_to_remove_rep(self, lfn):
    start_time = time.time()
    while not lfc.lfc_access("/grid" + lfn, 0): # rc = 0 if accessible
      if (time.time() - start_time < self.timeout) : time.sleep(0.1)
      else                                         : return self.timeout
    return time.time() - start_time

class CondDBTest(object):
  def __init__(self, xmlpath, workDirectory):
    self.xmlpath = xmlpath
    # Get ConDB infos
    self.CDB_infos = CS.getTypedDictRootedAt(root="", relpath="/Resources/CondDB")

    # Go to work directory
    oldcwd = os.getcwd()
    os.chdir(workDirectory)

    # Generate options file
    options = """from Gaudi.Configuration import *
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
    options_file = open("options.py", "w")
    try:
      options_file.write(options)
    finally:
      options_file.close()

    # Generate shell snipplet that will run the test
    env_script = """#!/bin/bash
. /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.sh
. SetupProject.sh LHCb v31r5 --use-grid
gaudirun.py options.py > result.log
"""
    env_file = open("run_condDB_test.sh", "w")
    try:
      env_file.write(env_script)
    finally:
      env_file.close()

    # For each CondDB, run the test and generate XML file
    for site in self.CDB_infos:
      self.run_test(site)

    # Go back to previous directory
    os.chdir(oldcwd)
    ### END OF TEST

  def run_test(self, site):
    # Generate the dblookup.xml and authentication.xml files needed by gaudirun.py
    self.generate_lookup_file(site)
    self.generate_authentication_file(site)

    # Run the shell snipplet that will output to result.log
    ret = subprocess.call(["bash", "run_condDB_test.sh"])

    if ret == 0:
      try:
        res = open("result.log", "r")
        res_string = res.read()
      finally:
        res.close()

        regExp = re.compile("ToolSvc.Sequenc...\s+INFO\s+LoadDDDB\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+\|\s+(\d+\.\d+)\s+(\d+\.\d)\s+\|\s+(\d)\s+\|\s+(\d+\.\d+)")
        reRes = regExp.search(res_string)
        loadTime = float(reRes.group(6))
        availability = 100

    else:
      loadTime = 0
      availability = 0

    # Generate XML file
    self.generate_xml(site, loadTime, availability)

  def generate_lookup_file(self, site):
    doc = impl.createDocument(None, "servicelist", None)
    elt = xml_append(doc, "logicalservice", name="CondDB")
    xml_append(doc, "service", elt_=elt, name=self.CDB_infos[site]['Connection'] + "/lhcb_conddb",
               accessMode="readonly", authentication="password")
    elt2 = xml_append(doc, "logicalservice", name="CondDBOnline")
    xml_append(doc, "service", elt_=elt2, name=self.CDB_infos[site]['Connection'] + "/lhcb_online_conddb",
               accessMode="readonly", authentication="password")
    xmlfile = open("dblookup.xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def generate_authentication_file(self, site):
    doc = impl.createDocument(None, "connectionlist", None)
    elt = xml_append(doc, "connection", name=self.CDB_infos[site]['Connection'] + "/lhcb_conddb")
    xml_append(doc, "parameter", elt_=elt, name="user", value=self.CDB_infos[site]["Username"])
    xml_append(doc, "parameter", elt_=elt, name="password", value=self.CDB_infos[site]["Password"])
    elt2 = xml_append(doc, "role", name="reader")
    xml_append(doc, "parameter", elt_=elt2, name="user", value=self.CDB_infos[site]["Username"])
    xml_append(doc, "parameter", elt_=elt2, name="password", value=self.CDB_infos[site]["Password"])

    elt = xml_append(doc, "connection", name=self.CDB_infos[site]['Connection'] + "/lhcb_online_conddb")
    xml_append(doc, "parameter", elt_=elt, name="user", value=self.CDB_infos[site]["Username"])
    xml_append(doc, "parameter", elt_=elt, name="password", value=self.CDB_infos[site]["Password"])
    elt2 = xml_append(doc, "role", name="reader")
    xml_append(doc, "parameter", elt_=elt2, name="user", value=self.CDB_infos[site]["Username"])
    xml_append(doc, "parameter", elt_=elt2, name="password", value=self.CDB_infos[site]["Password"])


    xmlfile = open("authentication.xml", "w")
    try:
      xmlfile.write(doc.toprettyxml(indent="  ", encoding="utf-8"))
    finally:
      xmlfile.close()

  def generate_xml(self, site, time_, availability):
    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    xml_append(doc, "id", site + "CondDB")
    xml_append(doc, "availability", availability)
    elt = xml_append(doc, "availabilitythresholds")
    xml_append(doc, "threshold", 5, elt_=elt, level="degraded")
    xml_append(doc, "threshold", 10, elt_=elt, level="affected")
    xml_append(doc, "threshold", 15, elt_=elt, level="available")
    xml_append(doc, "refreshperiod", "PT27M")
    xml_append(doc, "validityduration", 'PT13H')
    elt2 = xml_append(doc, "data")
    xml_append(doc, "numericvalue", str(time_), elt_=elt2, name="Time to access CondDB")
    xml_append(doc, "textvalue", "ConditionDB access timex", elt_=elt2)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))

    xmlfile = open(self.xmlpath + site + ".xml", "w")
    try:
      uglyXml = doc.toprettyxml(indent="  ", encoding="utf-8")
      prettyXml = xml_re.sub('>\g<1></', uglyXml)
      xmlfile.write(prettyXml)
    finally:
      xmlfile.close()

class SLSAgent(AgentModule):

  def execute(self):

    # FIXME: Get parameters from CS
    # SpaceTokenOccupancyTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/storage_space/")
    # DIRACTest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/dirac_services/")
    #    LOGSETest(xmlpath="/afs/cern.ch/user/v/vibernar/www/sls/log_se/")
    CondDBTest("/afs/cern.ch/user/v/vibernar/www/sls/condDB/", self.am_getWorkDirectory())
    #    LFCReplicaTest(path="/afs/cern.ch/project/gd/www/eis/docs/lfc/", timeout=60)
    return S_OK()
