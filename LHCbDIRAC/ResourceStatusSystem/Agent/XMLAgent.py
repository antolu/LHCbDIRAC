"""
This agent is in charge of creating SLS Update XML files from various
sources of data (mostly ResourceManagement service). It should run on
a VOBOX equipped with a web server.
"""

__RCSID__ = "$Id: $"
AGENT_NAME = 'ResourceStatus/XMLAgent'

import xml.etree.ElementTree as ET

from DIRAC import S_OK, rootPath
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                           import Utils

rmClient = ResourceManagementClient()

def gen_xml(id_, availability, timestamp, *l, **kw):
  e =  ET.Element("serviceupdate",
                  {"xmlns": "http://sls.cern.ch/SLS/XML/update",
                   "xmlns:xsi": 'http://www.w3.org/2001/XMLSchema-instance',
                   "xsi:schemaLocation": "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd"
                   })
  ET.SubElement(e, "id").text = id_
  ET.SubElement(e, "availability").text = str(availability)
  ET.SubElement(e, "timestamp").text = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
  # Appending all positional arguments:
  for pa in l: e.append(pa)
  # Adding all text arguments:
  for k in kw: ET.SubElement(e, k).text=kw[k]
  return e

def gen_threshold_elt(available=15, affected=10, degraded=5):
  e = ET.Element("availabilitythresholds")
  ET.SubElement(e, "threshold", {"level":"available"}).text=available
  ET.SubElement(e, "threshold", {"level":"affected"}).text=affected
  ET.SubElement(e, "threshold", {"level":"degraded"}).text=degraded
  return e

def gen_sto(id_, availability, timestamp, total, free, refresh, validity):
  def gen_sto_data_group(total, free):
    occupied = total - free
    e = ET.Element("data")
    group = ET.Element("grp", {"name": "Space occupancy"})
    ET.SubElement(group, "numericvalue", {"name": "Consumed"}).text=occupied
    ET.SubElement(group, "numericvalue", {"name": "Capacity"}).text=total
    e.append(group)
    ET.SubElement(e, "numericvalue", {"name": "Free space"}).text=free
    ET.SubElement(e, "numericvalue", {"name": "Occupied space"}).text=occupied
    ET.SubElement(e, "numericvalue", {"name": "Total space"}).text=total
    ET.SubElement(e, "textvalue").text="Storage space for the specific space token"
    return e

  th_group = gen_threshold_elt()
  data_group = gen_sto_data_group(total, free)
  return gen_xml(id_, availability, timestamp, th_group, data_group,
                 availabilityinfo=("Free=%d, Total=%d" % (free, total)),
                 availabilitydesc="FreeSpace less than 4TB implies 0%-99% ; FreeSpace greater than 4TB is always 100%",
                 refreshperiod=refresh,
                 validityduration=validity)

def gen_dirac_services(id_, availability, timestamp, serviceu, hostu, load, message):
  notes = ET.Element("notes")
  notes.text = message
  e = ET.Element("data")
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the service",
                                    "name": "Service Uptime"}).text=str(serviceu)
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the machine",
                                    "name": "Host Uptime"}).text=str(hostu)
  if load: ET.SubElement(e, "numericvalue", {"desc": "Instant load",
                                              "name": "Load"}).text=str(load)

  return (gen_xml(id_, availability, timestamp, notes, e) if availability > 0
          else gen_xml(id_, availability, timestamp, notes))

class XMLAgent(AgentModule):
  def execute(self):
    # Generate St Occupancy XML files
    self.log.info("Generating SpaceToken Occupancy XML files")
    lines = Utils.unpack(rmClient.getSLSStorage())
    for l in lines:
      print gen_sto(l[0] + "_" + l[1], l[3], l[2], l[6], l[8], l[4], l[5])

    # Generate DIRAC Services XML files
    self.log.info("Generating DIRAC Services XML files")
    lines = Utils.unpack(rmClient.getSLSService())
    for l in lines:
      ET.ElementTree(
        gen_dirac_services(l[0] + "_" + l[1], l[3], l[2], l[4], l[5], l[6], l[7])).write(
        rootPath + "/webRoot/www/sls/dirac_services/" + l[0] + "_" + l[1] + ".xml")

    # Generate VOBOX Services XML files
    self.log.info("Generating VOBOX Services XML files")
    lines = Utils.unpack(rmClient.getSLST1Service())
    for l in lines:
      ET.ElementTree(
        gen_dirac_services(l[0] + "_" + l[1], l[3], l[2], l[4], l[5], None, l[6])).write(
        rootPath + "/webRoot/www/sls/dirac_services/" + l[0] + "_" + l[1] + ".xml")

    return S_OK()
