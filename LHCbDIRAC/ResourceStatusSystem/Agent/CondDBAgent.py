from DIRAC import gLogger, gConfig, S_OK, rootPath

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule                            import AgentModule

from LHCbDIRAC.Core.Utilities import ProductionEnvironment
from LHCbDIRAC.ResourceStatusSystem.Utilities               import CS, Utils
from LHCbDIRAC.ResourceStatusSystem.Utilities.Utils         import xml_append
from LHCbDIRAC.ResourceStatusSystem.Agent.SLSAgent          import TestBase

# For caching to DB
from LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

import xml.dom, xml.sax
import time
import re, os, subprocess, pwd

__RCSID__ = "$Id: $"
AGENT_NAME = "ResourceStatus/CondDBAgent"

impl = xml.dom.getDOMImplementation()

class CondDBTest(TestBase):
  def __init__(self, am):
    super(CondDBTest, self).__init__(am)

    # Get ConDB infos
    self.CDB_infos = CS.getTypedDictRootedAt(root="", relpath="/Resources/CondDB")
    self.xmlPath      = rootPath + "/" + self.getAgentValue("webRoot") + self.getTestValue("dir")

    try:
      os.makedirs(self.xmlPath)
    except OSError:
      pass # The dir exist already, or cannot be created: do nothing


    # Go to work directory
    oldcwd = os.getcwd()
    os.chdir(am.am_getWorkDirectory())

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

    # For each CondDB, run the test and generate XML file
    for site in self.CDB_infos:
      gLogger.info("Starting SLS CondDB test for site %s" % site)
      self.run_test(site)

    # Go back to previous directory
    os.chdir(oldcwd)
    ### END OF TEST

  def run_test(self, site):
    # Generate the dblookup.xml and authentication.xml files needed by gaudirun.py
    self.generate_lookup_file(site)
    self.generate_authentication_file(site)

    if not os.environ.has_key("USER"):
      # Workaround: on some VOBOXes, the dirac process runs without a USER env variable.
      os.environ["USER"] = pwd.getpwuid(os.getuid())[0]

    env = Utils.unpack(ProductionEnvironment.getProjectEnvironment('x86_64-slc5-gcc43-opt', "LHCb"))
    f = open("result.log", "w")
    try:
      ret = subprocess.call(["gaudirun.py", "options.py"], env=env,stdout=f,stderr=subprocess.STDOUT)
    finally:
      f.close()

    if ret == 0:
      res = open("result.log", "r")
      try:
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
      xmlfile.write(doc.toxml())
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
      xmlfile.write(doc.toxml())
    finally:
      xmlfile.close()

  def generate_xml(self, site, time_, availability):
    doc = impl.createDocument("http://sls.cern.ch/SLS/XML/update",
                              "serviceupdate",
                              None)
    doc.documentElement.setAttribute("xmlns", "http://sls.cern.ch/SLS/XML/update")
    doc.documentElement.setAttribute("xmlns:xsi", 'http://www.w3.org/2001/XMLSchema-instance')
    doc.documentElement.setAttribute("xsi:schemaLocation",
                                     "http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd")

    xml_append(doc, "id", site + "_" + "CondDB")
    xml_append(doc, "availability", availability)
    elt = xml_append(doc, "availabilitythresholds")
    xml_append(doc, "threshold", self.getTestValue("Thresholds/degraded"), elt_=elt, level="degraded")
    xml_append(doc, "threshold", self.getTestValue("Thresholds/affected"), elt_=elt, level="affected")
    xml_append(doc, "threshold", self.getTestValue("Thresholds/available"), elt_=elt, level="available")
    xml_append(doc, "refreshperiod", self.getTestValue("refreshperiod"))
    xml_append(doc, "validityduration", self.getTestValue("validityduration"))
    elt2 = xml_append(doc, "data")
    xml_append(doc, "numericvalue", str(time_), elt_=elt2, name="Time to access CondDB")
    xml_append(doc, "textvalue", "ConditionDB access timex", elt_=elt2)
    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S"))

    xmlfile = open(self.xmlPath + site + "_" + "CondDB.xml", "w")
    try:
      xmlfile.write(doc.toxml())
    finally:
      xmlfile.close()

class CondDBAgent(AgentModule):
  def execute(self):
    CondDBTest(self)
    return S_OK()
