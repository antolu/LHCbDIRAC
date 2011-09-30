from DIRAC import gLogger, gConfig, S_OK, rootPath

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Base.AgentModule import AgentModule
import time, xml.dom.minidom, re, os

__RCSID__ = "$Id: $"
AGENT_NAME = 'ResourceStatus/NagiosTopologyAgent'

#
# This agent loops over the Dirac CS and extracts the necessary
# information to create a "topology map" which is used by the IT
# provided Nagios system to test Grid sites. The topology information
# defines the services to be tested.
#
# 2010-XX-YY : Roberto Santinelli : first version
# 2011-07-21 : Stefan Roiser      : skeleton re-used, introducing xml writer, generalizing CE handling
# 2011-08-11 : Vincent Bernardoff : Transformed the script into a Dirac agent.
#

class NagiosTopologyAgent(AgentModule):
  def initialize(self):
    self.xmlPath      = rootPath + "/" + self.am_getOption("webRoot")

    try:
      os.makedirs(self.xmlPath)
    except OSError:
      pass # The dirs exist already, or cannot be created: do nothing

    return S_OK()

  def xml_append(self, doc, base, elem, cdata=None, **attrs):
    new_elem = doc.createElement(elem)
    for attr in attrs : new_elem.setAttribute(attr, attrs[attr])
    if cdata : new_elem.appendChild(doc.createTextNode(cdata))
    return base.appendChild(new_elem)

  def execute(self):
    # instantiate xml doc
    xml_impl = xml.dom.minidom.getDOMImplementation()
    xml_doc = xml_impl.createDocument(None, 'root', None)
    xml_root = xml_doc.documentElement

    # xml header info
    self.xml_append(xml_doc, xml_root, 'title', 'LHCb Topology Information for ATP')
    self.xml_append(xml_doc, xml_root, 'description', 'List of LHCb site names for monitoring and mapping to the SAM/WLCG site names')
    self.xml_append(xml_doc, xml_root, 'feed_responsible', dn='/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=roiser/CN=564059/CN=Stefan Roiser', name='Stefan Roiser')
    self.xml_append(xml_doc, xml_root, 'last_update', time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))
    self.xml_append(xml_doc, xml_root, 'vo', 'lhcb')

    # loop over sites
    for site in gConfig.getSections('/Resources/Sites/LCG')['Value'] :

      # Site config
      site_opts = gConfig.getOptionsDict('/Resources/Sites/LCG/%s'%site)['Value']
      site_name = site_opts.get('Name')
      site_tier = site_opts.get('MoUTierLevel')
      if not site_tier : site_tier = 'UNDEFINED'
      has_grid_elem = False
      xml_site = self.xml_append(xml_doc, xml_root, 'atp_site', name=site_name)

      # CE info
      if gConfig.getSections('/Resources/Sites/LCG/%s/CEs'%site)['OK']:
        for site_ce_name in gConfig.getSections('/Resources/Sites/LCG/%s/CEs'%site)['Value'] :
          has_grid_elem = True
          site_ce_opts = gConfig.getOptionsDict('/Resources/Sites/LCG/%s/CEs/%s'%(site,site_ce_name))['Value']
          site_ce_type = site_ce_opts.get('CEType')
          if site_ce_type == 'LCG' : site_ce_type = 'CE'
          elif site_ce_type == 'CREAM' : site_ce_type = 'CREAM-CE'
          elif not site_ce_type : site_ce_type = 'UNDEFINED'
          self.xml_append(xml_doc, xml_site, 'service', hostname=site_ce_name, flavour=site_ce_type)

      # SE info
      if site_opts.has_key('SE') and site_tier in ['0', '1']:
        has_grid_elem=True
        site_se_opts = gConfig.getOptionsDict('/Resources/StorageElements/%s-RAW/AccessProtocol.1'%site.split('.')[1])['Value']
        site_se_name = site_se_opts.get('Host')
        site_se_type = site_se_opts.get('ProtocolName')
        if site_se_type == 'SRM2' : site_se_type = 'SRMv2'
        elif not site_se_type : site_se_type = 'UNDEFINED'
        self.xml_append(xml_doc, xml_site, 'service', hostname=site_se_name, flavour=site_se_type)

      # FileCatalog info
      if site in gConfig.getSections('/Resources/FileCatalogs/LcgFileCatalogCombined')['Value'] :
        has_grid_elem = True
        site_fc_opts = gConfig.getOptionsDict('/Resources/FileCatalogs/LcgFileCatalogCombined/%s'%site)['Value']
        if site_fc_opts.has_key('ReadWrite') : self.xml_append(xml_doc, xml_site, 'service', hostname=site_fc_opts.get('ReadWrite'), flavour='Central-LFC')
        if site_fc_opts.has_key('ReadOnly') : self.xml_append(xml_doc, xml_site, 'service', hostname=site_fc_opts.get('ReadOnly'), flavour='Local-LFC')

      # Site info will be put if we found at least one CE, SE or LFC element
      if has_grid_elem :
        self.xml_append(xml_doc, xml_site, 'group', name=site, type='LHCb_Site')
        self.xml_append(xml_doc, xml_site, 'group', name='Tier'+site_tier, type='LHCb_Tier')
      else :
        gLogger.warn("Site %s, (WLCG Name: %s) has no CE, SE or LFC, thus will not be put into the xml" % ( site, site_name ))
        xml_root.removeChild(xml_site)

    # produce the xml

    uglyXml = xml_doc.toprettyxml(indent='  ', encoding='utf-8')
    text_re = re.compile('>\n\s+([^<>\s].*?)\n\s+</', re.DOTALL)
    prettyXml = text_re.sub('>\g<1></', uglyXml)

    fname = self.xmlPath + "lhcb_topology.xml"
    xmlf = open(fname,'w')
    xmlf.write(prettyXml)
    xmlf.close()

    return S_OK()
