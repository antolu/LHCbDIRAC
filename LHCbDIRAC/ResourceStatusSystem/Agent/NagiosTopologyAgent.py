""" LHCbDIRAC.ResourceStatusSystem.Agent.NagiosTopologyAgent

   NagiosTopologyAgent.__bases__:
     DIRAC.Core.Base.AgentModule.AgentModule
   xml_append

"""

import os
import time
import xml.dom.minidom
import urllib
import json
from DIRAC import S_OK, rootPath, gLogger, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement


__RCSID__ = "$Id$"
AGENT_NAME = 'ResourceStatus/NagiosTopologyAgent'


class NagiosTopologyAgent(AgentModule):
  """
  This agent loops over the Dirac CS and extracts the necessary
  information to create a "topology map" which is used by the IT
  provided Nagios system to test Grid sites. The topology information
  defines the services to be tested.

  NagiosTopologyAgent, writes the xml topology consumed by Nagios to run
  the tests.
  """

  def __init__(self, *args, **kwargs):

    AgentModule.__init__(self, *args, **kwargs)

    self.xmlPath = None

    self.dryRun = False

  def initialize(self):
    """ Initialize the agent.
    """

    self.xmlPath = rootPath + '/' + self.am_getOption('webRoot')

    try:
      os.makedirs(self.xmlPath)
    except OSError:
      pass  # The dirs exist already, or cannot be created: do nothing

    return S_OK()

  def execute(self):
    """ Let's generate the xml file with the topology.
    """

    # instantiate xml doc
    xml_impl = xml.dom.minidom.getDOMImplementation()
    xml_doc = xml_impl.createDocument(None, 'root', None)
    xml_root = xml_doc.documentElement

    # xml header info
    self.__writeHeaderInfo(xml_doc, xml_root)

    # loop over sites

##########################################################################
# Newest code to include VAC and VCYCLE

    urljson = 'https://wlcg-rebus.cern.ch/apps/topology/all/json'
    response = urllib.urlopen(urljson)
    wlcg = json.loads(response.read())

    ret = gConfig.getSections('Resources/Sites')
    if not ret['OK']:
      gLogger.error(ret['Message'])
      return ret

    gridTypes = ret['Value']

    all_sites = []
    organized_list_of_sites = []

    for grid in gridTypes:
      sites = gConfig.getSections('Resources/Sites/%s' % grid)
      all_sites = all_sites + sites['Value']

    for site in all_sites:
      grid, real_site_name, country = site.split(".")
      same_site = [s for s in all_sites if (
          "." + real_site_name + "." + country) in s]
      organized_list_of_sites = organized_list_of_sites + [same_site]

    for sites in organized_list_of_sites:
      site_parameters = self.__site_parameters(sites, wlcg)

      if site_parameters:

        xml_site = xml_append(xml_doc, xml_root, 'atp_site',
                              name=site_parameters['WlcgName'])
        has_grid_elem = False

        dirac_name = site_parameters['DiracName']
        site_tier = site_parameters['Tier']
        site_subtier = site_parameters['Sub-Tier']

        for grid in site_parameters['Grid']:
          site = site_parameters['Grid'][grid]['SiteName']
          ces = site_parameters['Grid'][grid]['CEs']
          # CE info
          if ces:
            res = self.__writeCEInfo(
                xml_doc, grid, xml_site, site, ces)
            # Update has_grid_elem
            has_grid_elem = res or has_grid_elem

        # SE info
        if site_parameters['SE'] and (site_tier in ['0', '1', '2'] or site_subtier in ['T2-D']):
          #res = self.__writeSEInfo( xml_doc, xml_site, dirac_name )
          res = self.__writeSEInfo(
              xml_doc, xml_site, dirac_name, site_tier, site_subtier)
          # Update has_grid_elem
          has_grid_elem = res or has_grid_elem

        if has_grid_elem:
          xml_append(xml_doc, xml_site, 'group',
                     name='Tier ' + site_tier, type='LHCb_Tier')
          xml_append(xml_doc, xml_site, 'group',
                     name=dirac_name, type='LHCb_Site')
          xml_append(xml_doc, xml_site, 'group',
                     name=dirac_name, type='All Sites')
          xml_append(xml_doc, xml_site, 'group',
                     name=dirac_name, type='WLCG_%s' % (site_parameters['FederationAccountingName']))  # ==> Federation
          xml_append(xml_doc, xml_site, 'group',
                     name=dirac_name, type='WLCG_%s' % (site_parameters['Country']))  # ==> Country
          try:
            if site_subtier == 'T2-D':
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 0/1/2D')
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 2D')
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 2')    # Mod for T2-D also have T2 tag

            elif int(site_tier) == 2:
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 2')

            # site_tier can be only 1 or 0, (see site_tier def above to
            # convince yourself.)
            else:
              # If site_type is None, then we go to the exception.
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 0/1/2D')
              xml_append(xml_doc, xml_site, 'group',
                         name=dirac_name, type='Tier 0/1')

          except ValueError:  # Site tier is None, do nothing
            pass

        else:
          _msg = "Site %s, (WLCG Name: %s) has no CE, SE or LFC, thus will not be put into the xml"
          _msg = _msg % (site, site_name)
          self.log.warn(_msg)
          xml_root.removeChild(xml_site)

    self.dryRun = self.am_getOption('DryRun', self.dryRun)
    if self.dryRun:
      self.log.info("Dry Run: XML file will not be created, just printed")
      print xml_doc.toxml()

    else:
      # produce the xml
      with open(self.xmlPath + "lhcb_topology.xml", 'w') as xmlf:
        xmlf.write(xml_doc.toxml())

      self.log.info("XML file created Successfully")

    return S_OK()

## Private methods #######################################################

  @staticmethod
  def __site_parameters(sites, wlcg):
    """
      Function that returns the sites parameters.
      Keys:
      'WlcgName', 'Coordinates', 'Description', 'Mail', 'DiracName', 'Tier', 'Sub-Tier', 
      'SE', 'Country':, 'Federation', 'FederationAccountingName', 'Infrastructure', 
      'Institute Name', 'Grid' 

      If the site is not listed in WLCG or have no MoU Tier Level the function will return False
    """

    grid_dict = {}
    grid, real_site_name, country = sites[0].split(".")
    site_opts = gConfig.getOptionsDict(
        'Resources/Sites/%s/%s' % (grid, sites[0]))
    site_opts = site_opts.get('Value')
    site_name = site_opts.get('Name')
    site_tier = site_opts.get('MoUTierLevel', 'None')
    if site_tier != 'None':
      wlcg_params = [s for s in wlcg if site_name in s.get('Site')]
      # wlcg_params = wlcg_params[0] if wlcg_params else {}
      if not wlcg_params:
        return False
      if len(sites) > 1:
        for i in sites:
          grid = i.split(".")[0]
          CE = gConfig.getSections('Resources/Sites/%s/%s/CEs' % (grid, i))
          CE = CE['Value'] if CE['OK'] else None
          grid_dict.update({grid: {'SiteName': i, 'CEs': CE}})
      else:
        CE = gConfig.getSections(
            'Resources/Sites/%s/%s/CEs' % (grid, sites[0]))
        CE = CE['Value'] if CE['OK'] else None
        grid_dict.update({grid: {'SiteName': sites[0], 'CEs': CE}})

      site_subtier = site_opts.get('SubTier', None)
      ses = site_opts.get('SE', None)

      site_params = {'WlcgName': site_opts.get('Name'), 'Coordinates': site_opts.get('Coordinates'),
                     'Description': site_opts.get('Description'), 'Mail': site_opts.get('Mail'),
                     'DiracName': ('LCG.' + real_site_name + "." + country),
                     'Tier': site_tier, 'Sub-Tier': site_subtier, 'SE': ses,
                     'Country': wlcg_params.get('Country'), 'Federation': wlcg_params.get('Federation'),
                     'FederationAccountingName': wlcg_params.get('FederationAccountingName'),
                     'Infrastructure': wlcg_params.get('Infrastructure'),
                     'Institute Name': wlcg_params.get('Institute Name'), 'Grid': grid_dict
                     }

      return site_params
    else:
      return False

  @staticmethod
  def __writeHeaderInfo(xml_doc, xml_root):
    """
      Writes XML document header.
    """

    xml_append(xml_doc, xml_root, 'title', 'LHCb Topology Information for ATP')
    xml_append(xml_doc, xml_root, 'description',
               'List of LHCb site names for monitoring and mapping to the SAM/WLCG site names')
    xml_append(xml_doc, xml_root, 'feed_responsible',
               dn='/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=roiser/CN=564059/CN=Stefan Roiser',
               name='Stefan Roiser')
    xml_append(xml_doc, xml_root, 'last_update',
               time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    xml_append(xml_doc, xml_root, 'vo', 'lhcb')

  @staticmethod
  def __writeCEInfo(xml_doc, grid, xml_site, site, ces):
    """ Writes CE information in the XML Document
    """

    has_grid_elem = False

    for site_ce_name in ces:

      has_grid_elem = True

      site_ce_opts = gConfig.getOptionsDict(
          'Resources/Sites/%s/%s/CEs/%s' % (grid, site, site_ce_name))
      if not site_ce_opts['OK']:
        gLogger.error(site_ce_opts['Message'])
        continue
      site_ce_opts = site_ce_opts['Value']

      site_ce_type = site_ce_opts.get('CEType')
      mappingCEType = {'LCG': 'CE', 'CREAM': 'CREAM-CE',
                       'ARC': 'ARC-CE', 'HTCondorCE': 'org.opensciencegrid.htcondorce',
                       'Vac': 'uk.ac.gridpp.vac', 'Cloud': 'CLOUD', 'Boinc': 'BOINC',
                       'Vcycle': 'uk.ac.gridpp.vcycle'}

      xml_append(xml_doc, xml_site, 'service', hostname=site_ce_name,
                 flavour=mappingCEType.get(site_ce_type, 'UNDEFINED'))

    return has_grid_elem

  @staticmethod
  def __writeSEInfo(xml_doc, xml_site, site, site_tier, site_subtier):
    """ Writes SE information in the XML Document
    """
    def __write_SE_XML(site_se_opts):
      """
      Sub-function just to populate the XML with the SE values
      """
      site_se_name = site_se_opts.get('Host')
      site_se_flavour = site_se_opts.get('Protocol')
      site_se_path = site_se_opts.get('Path', 'UNDEFINED')
      site_se_endpoint = site_se_opts.get('URLBase')
      mappingSEFlavour = {'srm': 'SRMv2',
                          'root': 'XROOTD', 'http': 'HTTPS'}

      xml_append(xml_doc, xml_site, 'service',
                 endpoint=site_se_endpoint,
                 flavour=mappingSEFlavour.get(site_se_flavour, 'UNDEFINED'),
                 hostname=site_se_name,
                 path=site_se_path)

    has_grid_elem = True

    real_site_name = site.split(".")[1]
    dmsHelper = DMSHelpers()

    if int(site_tier) in (0, 1):
      dst = dmsHelper.getSEInGroupAtSite('Tier1-DST', real_site_name)
      raw = dmsHelper.getSEInGroupAtSite('Tier1-RAW', real_site_name)
      if not raw['OK']:
        gLogger.error(raw['Message'])
        return False
      raw = raw['Value']
      se_RAW = StorageElement(raw)
      se_plugins_RAW = se_RAW.getPlugins()

    if site_subtier == 'T2-D':
      dst = dmsHelper.getSEInGroupAtSite('Tier2D-DST', real_site_name)

    if not dst['OK']:
      gLogger.error(dst['Message'])
      return False

    dst = dst['Value']
    se_DST = StorageElement(dst)
    se_plugins_DST = se_DST.getPlugins()
    if not se_plugins_DST['OK']:
      gLogger.error(se_plugins_DST['Message'])
      return False

    for protocol in se_plugins_DST['Value']:
      site_se_opts_DST = se_DST.getStorageParameters(protocol)
      if not site_se_opts_DST['OK']:
        gLogger.error(site_se_opts_DST['Message'])
        return False
      site_se_opts_DST = site_se_opts_DST['Value']
      __write_SE_XML(site_se_opts_DST)

      if site_tier in (0, 1):
        if protocol in se_plugins_RAW['Value']:
          site_se_opts_RAW = se_RAW.getStorageParameters(protocol)
          if not site_se_opts_RAW['OK']:
            gLogger.error(site_se_opts_RAW['Message'])
            return has_grid_elem
          site_se_opts_RAW = site_se_opts_RAW['Value']
          # This tests if the DST and RAW StorageElements have the same endpoint.
          # If so it only uses the one already added.
          if site_se_opts_RAW['Host'] != site_se_opts_DST['Host']:
            __write_SE_XML(site_se_opts_RAW)

    return has_grid_elem

##########################################################################


def xml_append(doc, base, elem, cdata=None, **attrs):
  """
    Given a Document, we append to it an element.
  """

  new_elem = doc.createElement(elem)
  for attr in attrs:
    new_elem.setAttribute(attr, attrs[attr])
  if cdata:
    new_elem.appendChild(doc.createTextNode(cdata))

  return base.appendChild(new_elem)

#.........................................................................
# EOF
