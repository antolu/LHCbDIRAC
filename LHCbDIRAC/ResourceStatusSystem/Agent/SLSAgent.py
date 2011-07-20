from DIRAC import gLogger, S_OK, S_ERROR

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.ResourceStatusSystem.Utilities import CS, Utils

import xml.dom

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SLSAgent'

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
  def __init__(self, site, space_token, url):
    self.site = site
    self.space_token = space_token
    self.url = url
    self.availability = 0
    self.free = 0
    self.total = 0
    self.timestamp = ""
    self.validity = ""

  def xml(self):
    impl = xml.dom.getDOMImplementation()
    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                               "serviceupdate",
                               None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    doc.documentElement.setAttribute("xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance')
    doc.documentElement.setAttribute("xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd")

    xml_append(doc, "id", self.site)
    xml_append(doc, "availability", self.availability)
    elt = xml_append(doc, "availabilitythresholds")
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


    return doc


class SLSAgent(AgentModule):

  def execute(self):

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
        st_test = SpaceTokenOccupancyTest(k, st, res[k][st])
        print st_test.xml().toprettyxml(indent="  ", encoding="utf-8")

    # for k in res:
    #   print k
    # print len(res), res

    return S_OK()
