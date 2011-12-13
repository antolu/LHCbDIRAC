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
from DIRAC.ResourceStatusSystem.Utilities                           import Utils
from DIRAC.Core.Base.DB import DB

rmDB = DB("ResourceManagementDB", "ResourceStatus/ResourceManagementDB", 10)

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

def gen_sto(site, token, availability, timestamp, refresh, validity, total, _guaranteed, free):
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
  return gen_xml(site + "_" + token, availability, timestamp, th_group, data_group,
                 availabilityinfo=("Free=%d, Total=%d" % (free, total)),
                 availabilitydesc="FreeSpace less than 4TB implies 0%-99% ; FreeSpace greater than 4TB is always 100%",
                 refreshperiod=refresh,
                 validityduration=validity)

def gen_dirac_services(system, service, availability, timestamp, host, serviceu, hostu, load, message):
  webpage = ET.Element("webpage")
  webpage.text = "http://lemonweb.cern.ch/lemon-web/info.php?entity=" + (host if host else "unknown")
  notes = ET.Element("notes")
  notes.text = message
  e = ET.Element("data")
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the service",
                                    "name": "Service Uptime"}).text=str(serviceu)
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the machine",
                                    "name": "Host Uptime"}).text=str(hostu)
  ET.SubElement(e, "numericvalue", {"desc": "Instant load",
                                    "name": "Load"}).text=str(load)

  return (gen_xml(system + "_" + service, availability, timestamp, notes, e) if availability > 0
          else gen_xml(system + "_" + service, availability, timestamp, notes))

def gen_dirac_t1_services(url, system, availability, timestamp, _version, serviceu, hostu, message):
  notes = ET.Element("notes")
  notes.text = message
  e = ET.Element("data")
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the service",
                                    "name": "Service Uptime"}).text=str(serviceu)
  ET.SubElement(e, "numericvalue", {"desc": "Seconds since last restart of the machine",
                                    "name": "Host Uptime"}).text=str(hostu)
  if system == "RequestManagement":
    lines = Utils.unpack(rmDB._query("SELECT Name,Assigned,Waiting,Done FROM SLSRMStats where Site=\"%s\"" % url))
    for l in lines:
      ET.SubElement(e, "numericvalue", {"desc": "Number of assigned " + l[0] + " requests",
                                        "name": l[0] + " - Assigned"}).text=str(l[1])
      ET.SubElement(e, "numericvalue", {"desc": "Number of waiting " + l[0] + " requests",
                                        "name": l[0] + " - Waiting"}).text=str(l[2])
      ET.SubElement(e, "numericvalue", {"desc": "Number of completed " + l[0] + " requests",
                                        "name": l[0] + " - Completed"}).text=str(l[3])

  return (gen_xml(url + "_" + system, availability, timestamp, notes, e) if availability > 0
          else gen_xml(url + "_" + system, availability, timestamp, notes))

def gen_logse(name, availability, timestamp, validityduration, used = None, total = None):
  if name == "partition":
    e = ET.Element("data")
    ET.SubElement(e, "numericvalue", {"name": "LogSE data partition used"}).text=str(used)
    ET.SubElement(e, "numericvalue", {"name": "Total space on data partition"}).text=str(total)
  return (gen_xml("log_se_" + name, availability, timestamp,
                  e, validityduration=validityduration) if name == "partition"
          else gen_xml("log_se_" + name, availability, timestamp, validityduration=validityduration))

def gen_conddb(name, availability, timestamp, accesstime):
  e = ET.Element("data")
  ET.SubElement(e, "numericvalue", {"name": "CondDB access time"}).text=str(accesstime)
  return (gen_xml(name + "_CondDB", availability, timestamp, e) if availability > 0
          else gen_xml(name + "_CondDB", availability, timestamp))

class XMLAgent(AgentModule):
  def execute(self):
    # Generate St Occupancy XML files
    self.log.info("Generating SpaceToken Occupancy XML files")
    lines = Utils.unpack(rmDB._query("SELECT * FROM SLSStorage"))
    for l in lines:
      ET.ElementTree(gen_sto(*l)).write(
        rootPath + "/webRoot/www/sls/storage_space/" + l[0] + "_" + l[1] + ".xml")

    # Generate DIRAC Services XML files
    self.log.info("Generating DIRAC Services XML files")
    lines = Utils.unpack(rmDB._query("SELECT * FROM SLSService"))
    for l in lines:
      ET.ElementTree(gen_dirac_services(*l)).write(
        rootPath + "/webRoot/www/sls/dirac_services/" + l[0] + "_" + l[1] + ".xml")

    # Generate VOBOX Services XML files
    self.log.info("Generating VOBOX Services XML files")
    lines = Utils.unpack(rmDB._query("SELECT * FROM SLST1Service"))
    for l in lines:
      ET.ElementTree(gen_dirac_t1_services(*l)).write(
        rootPath + "/webRoot/www/sls/dirac_services/" + l[0] + "_" + l[1] + ".xml")

    # Generate LogSE XML files
    self.log.info("Generating LogSE XML files")
    lines = Utils.unpack(rmDB._query("SELECT * FROM SLSLogSE"))
    for l in lines:
      ET.ElementTree(gen_logse(*l)).write(
        rootPath + "/webRoot/www/sls/log_se/log_se_" + l[0] + ".xml")

    self.log.info("Generating CondDB XML files")
    lines = Utils.unpack(rmDB._query("SELECT * FROM SLSCondDB"))
    for l in lines:
      ET.ElementTree(gen_conddb(*l)).write(
        rootPath + "/webRoot/www/sls/condDB/" + l[0] + ".xml")

    return S_OK()
