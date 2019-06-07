###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" LHCbDIRAC.ResourceStatusSystem.Agent.SLSAgent

    This agent creates XML files with SE space left, that will be picked up by a cron job that will add to meter.cern.ch

    What's collected here will enter in https://meter.cern.ch/public/_plugin/kibana/#/dashboard/temp/meter::lhcb
    by using this cronjob:

# Puppet Name: Send SLS Info
10 * * * * /opt/dirac/webRoot/www/send_sls_info.csh

[dirac@lbvobox108 pro]$ more /opt/dirac/webRoot/www/send_sls_info.csh
#!/bin/csh
echo "CURL"
foreach i (`ls /opt/dirac/webRoot/www/sls/dirac_services/*`)
  echo $i
  /usr/bin/curl -F file=@${i} xsls.cern.ch
end
foreach i (`ls /opt/dirac/webRoot/www/sls/log_se/*`)
  echo $i
  /usr/bin/curl -F file=@$i xsls.cern.ch
end
foreach i (`ls /opt/dirac/webRoot/www/sls/storage_space/*`)
  echo $i
  /usr/bin/curl -F file=@$i xsls.cern.ch
end
exit

"""

# TODO: SLSAgent is not anymore the right name
# TODO: use elasticseach module to spit in meter.

__RCSID__ = "$Id$"

import time
import xml.dom
import xml.sax
import urlparse

from DIRAC import gConfig, gLogger, S_OK, rootPath
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers

AGENT_NAME = 'ResourceStatus/SLSAgent'


def xml_append(doc, tag, value_=None, elt_=None, **kw):
  '''
    #TODO: see below
    I will be so happy getting rid of this !
  '''
  new_elt = doc.createElement(tag)
  for k in kw:
    new_elt.setAttribute(k, str(kw[k]))
  if value_ is not None:
    textnode = doc.createTextNode(str(value_))
    new_elt.appendChild(textnode)
  if elt_ is not None:
    return elt_.appendChild(new_elt)
  return doc.documentElement.appendChild(new_elt)


def gen_xml_stub():
  impl = xml.dom.getDOMImplementation()
  doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                            "serviceupdate",
                            None)
  doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
  doc.documentElement.setAttribute("xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance')
  doc.documentElement.setAttribute("xsi:schemaLocation",
                                   "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd")
  return doc


class TestBase(object):
  def __init__(self, agent):
    self.agent = agent

  def getAgentOption(self, name, defaultValue=None):
    return self.agent.am_getOption(name, defaultValue)

  def getTestOption(self, name, defaultValue=None):
    return self.agent.am_getOption(self.__class__.__name__ + "/" + name, defaultValue)

  getAgentValue = getAgentOption
  getTestValue = getTestOption


class SpaceTokenOccupancyTest(TestBase):

  def __init__(self, am):
    super(SpaceTokenOccupancyTest, self).__init__(am)
    self.xmlPath = rootPath + "/" + self.getAgentOption("webRoot") + self.getTestOption("dir")

    self.rmClient = ResourceManagementClient()
    mkDir(self.xmlPath)

    self.generate_xml_and_dashboard()

  def generate_xml_and_dashboard(self):

    res = self.rmClient.selectSpaceTokenOccupancyCache()
    if not res['OK']:
      gLogger.error(res['Message'])
      return

    itemDicts = [dict(zip(res['Columns'], item)) for item in res['Value']]
    for itemDict in itemDicts:
      self.generate_xml(itemDict)

  def generate_xml(self, itemDict):
    """ itemDict is like

      {'Endpoint': 'httpg://tbit00.nipne.ro:8446/srm/managerv2',
       'Free': 113252649.213,
       'Guaranteed': 0.0,
       'LastCheckTime': datetime.datetime(2018, 11, 8, 10, 49, 14),
       'Token': 'NIPNE-07_MC-DST',
       'Total': 274877906.944}
    """

    endpoint = itemDict['Endpoint']

    site = ''

    for se in DMSHelpers().getStorageElements():
      # Ugly, ugly, ugly..
      if ('-' not in se) or ('_' in se):
        continue

      res = CSHelpers.getStorageElementEndpoint(se)
      if not res['OK']:
        continue

      if endpoint == res['Value']:
        # HACK !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        if 'RAL-HEP' in se:
          site = 'RAL-HEP'
        else:
          site = se.split('-', 1)[0]
        break

    if not site:
      gLogger.error('Unable to find site for %s' % endpoint)
      return

    token = itemDict['Token']

    # ['Endpoint', 'LastCheckTime', 'Guaranteed', 'Free', 'Token', 'Total']
    total = itemDict['Total']
    # guaranteed   = itemDict[ 'Guaranteed' ]
    free = itemDict['Free']
    availability = "available" if free > 4 else ("degraded" if total != 0 else "unavailable")

    doc = gen_xml_stub()
    xml_append(doc, "id", site + "_" + token)
    xml_append(doc, "status", availability)
    xml_append(doc, "contact", "lhcb-geoc@cern.ch")
    xml_append(doc, "availabilityinfo", "Free=" + str(free) + " Total=" + str(total))
    xml_append(doc, "availabilitydesc", self.getTestValue("availabilitydesc"))
    elt = xml_append(doc, "data")
    xml_append(doc, "numericvalue", value_=str(free), elt_=elt, name="Free space")
    xml_append(doc, "numericvalue", value_=str(total - free), elt_=elt, name="Occupied space")
    xml_append(doc, "numericvalue", value_=str(total), elt_=elt, name="Total space")
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S"))

    with open(self.xmlPath + site + "_" + token + ".xml", "w") as xmlfile:
      xmlfile.write(doc.toxml())

    gLogger.info("SpaceTokenOccupancyTest: %s/%s done." % (site, token))
    return S_OK()


class DIRACTest(TestBase):
  def __init__(self, am):
    super(DIRACTest, self).__init__(am)
    self.xmlPath = rootPath + "/" + self.getAgentValue("webRoot") + self.getTestValue("dir")
    from DIRAC.Interfaces.API.Dirac import Dirac
    self.dirac = Dirac()

    mkDir(self.xmlPath)

    self.run_t1_xml_sensors()

  def run_t1_xml_sensors(self):
    # For each T0/T1 VO-BOXes, run xml_t1_sensors...
    request_management_urls = gConfig.getValue("/Systems/RequestManagement/Production/URLs/ReqProxyURLs", [])
    configuration_urls = gConfig.getServersList()
    framework_urls = gConfig.getValue("/DIRAC/Framework/SystemAdministrator", [])

    gLogger.info("DIRACTest: discovered %d request management url(s), %d configuration url(s) and %d framework url(s)"
                 % (len(request_management_urls), len(configuration_urls), len(framework_urls)))
    for url in request_management_urls + configuration_urls + framework_urls:
      try:
        self.xml_t1_sensor(url)
      except BaseException:
        gLogger.warn('DIRACTest.t1_xml_sensors crashed on %s' % url)

  def xml_t1_sensor(self, url):
    parsed = urlparse.urlparse(url)
    system, _service = parsed[2].strip("/").split("/")
    site = parsed[1].split(":")[0]

    pinger = RPCClient(url)
    res = pinger.ping()

    doc = gen_xml_stub()
    xml_append(doc, "id", site + "_" + system)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S"))
    xml_append(doc, "contact", "lhcb-geoc@cern.ch")

    if res['OK']:
      res = res['Value']

      xml_append(doc, "status", "available")

      elt = xml_append(doc, "data")
      xml_append(doc, "numericvalue", res.get('service uptime', 0), elt_=elt,
                 name="Service Uptime", desc="Seconds since last restart of service")
      xml_append(doc, "numericvalue", res.get('host uptime', 0), elt_=elt,
                 name="Host Uptime", desc="Seconds since last restart of machine")

      gLogger.info("%s/%s successfully pinged" % (site, system))

    else:
      xml_append(doc, "status", "unavailable")
      gLogger.info("%s/%s does not respond to ping" % (site, system))

    with open(self.xmlPath + site + "_" + system + ".xml", "w") as xmlfile:
      xmlfile.write(doc.toxml())


class SLSAgent(AgentModule):
  def initialize(self):

    self.am_setOption('shifterProxy', 'DataManager')
    return S_OK()

  def execute(self):

    SpaceTokenOccupancyTest(self)
    DIRACTest(self)

    return S_OK()
