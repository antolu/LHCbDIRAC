''' LHCbDIRAC.ResourceStatusSystem.Agent.SLSAgent

    This agent creates XML files with SE space left, that will be picked up by a cron job that will add to meter.cern.ch
      so maybe SLSAgent is not anymore the right name

'''

# TODO: decide what to do about this agent (maybe really remove it).
# Alternatively: use elasticseach module to spit in meter.

__RCSID__ = "$Id$"

import time
import xml.dom
import xml.sax

from DIRAC import gLogger, S_OK, rootPath
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers

AGENT_NAME = 'ResourceStatus/SLSAgent'

# Taken from utilities


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

# Helper functions to send a warning mail to a site (for space-token test)

# def contact_mail_of_site( site ):
#  opHelper = Operations()
#  return opHelper.getValue( "Shares/Disk/" + site + "/Mail" )

# def send_mail_to_site( site, token, pledged, total ):
#  from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
#  nc = NotificationClient()
#  subject = "%s provide insufficient space for space-token %s" % ( site, token )
#  body = """
# Hi ! Our RSS monitoring systems informs us that %s has
# pledged %f TB to the space-token %s, but in reality, only %f TB of
# space is available. Thanks to solve the problem if possible.
#
#""" % ( site, pledged, token, total )
#  address = contact_mail_of_site( site )
#  if address:
#    res = nc.sendMail( address, subject, body )
#    if res['OK'] == False:
#      gLogger.warn( "Unable to send mail to %s: %s" % ( address, res['Message'] ) )
#    else:
#      gLogger.info( "Sent mail to %s OK!" % address )


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

    endpoint = itemDict['Endpoint']

    site = ''

    ses = CSHelpers.getStorageElements()
    if not ses['OK']:
      gLogger.error(ses['Message'])

    for se in ses['Value']:
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
    #guaranteed   = itemDict[ 'Guaranteed' ]
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

    xmlfile = open(self.xmlPath + site + "_" + token + ".xml", "w")
    try:
      xmlfile.write(doc.toxml())
    finally:
      xmlfile.close()

    gLogger.info("SpaceTokenOccupancyTest: %s/%s done." % (site, token))
    return S_OK()


class SLSAgent(AgentModule):
  def initialize(self):

    self.am_setOption('shifterProxy', 'DataManager')
    return S_OK()

  def execute(self):

    # Future me, forgive me for this. TO BE Fixed.
    try:
      SpaceTokenOccupancyTest(self)
    except BaseException as e:
      gLogger.warn('SpaceTokenOccupancyTest crashed with %s' % e)

    return S_OK()
