""" LHCbDIRAC.ResourceStatusSystem.Agent.NagiosTopologyAgent

   NagiosTopologyAgent.__bases__:
     DIRAC.Core.Base.AgentModule.AgentModule
   xml_append

"""

import os
import time
import xml.dom.minidom

from DIRAC                                               import S_OK, rootPath, gLogger, gConfig
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.DataManagementSystem.Utilities.DMSHelpers     import DMSHelpers  
from DIRAC.Resources.Storage.StorageElement              import StorageElement


__RCSID__ = "$Id$"
AGENT_NAME = 'ResourceStatus/NagiosTopologyAgent'

class NagiosTopologyAgent( AgentModule ):
  """
  This agent loops over the Dirac CS and extracts the necessary
  information to create a "topology map" which is used by the IT
  provided Nagios system to test Grid sites. The topology information
  defines the services to be tested.

  NagiosTopologyAgent, writes the xml topology consumed by Nagios to run
  the tests.
  """

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )

    self.xmlPath = None

  def initialize( self ):
    """ Initialize the agent.
    """

    self.xmlPath = rootPath + '/' + self.am_getOption( 'webRoot' )

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass  # The dirs exist already, or cannot be created: do nothing

    return S_OK()

  def execute( self ):
    """ Let's generate the xml file with the topology.
    """

    self.dryRun = False

    # instantiate xml doc
    xml_impl = xml.dom.minidom.getDOMImplementation()
    xml_doc = xml_impl.createDocument( None, 'root', None )
    xml_root = xml_doc.documentElement

    # xml header info
    self.__writeHeaderInfo( xml_doc, xml_root )

    # loop over sites

    middlewareTypes = []
    ret = gConfig.getSections('Resources/Sites') 
    if not ret[ 'OK' ] : 
      gLogger.error( ret[ 'Message' ] )
      return ret
    
    middlewareTypes = ret['Value']
    for middleware in middlewareTypes :

      sites = gConfig.getSections( 'Resources/Sites/%s' % middleware )
      if not sites[ 'OK' ]:
        gLogger.error( sites[ 'Message' ] )
        return sites

      for site in sites[ 'Value' ]:

        # Site config
        site_opts = gConfig.getOptionsDict( 'Resources/Sites/%s/%s' % ( middleware, site ) )
        if not site_opts[ 'OK' ]:
          gLogger.error( site_opts[ 'Message' ] )
          return site_opts
        site_opts = site_opts[ 'Value' ]

        site_name = site_opts.get( 'Name' )
        site_tier = site_opts.get( 'MoUTierLevel', 'None' )
        # we are only interested in sites with a MoUTierLevel, i.e. WLCG sites, for the WLCG topology map
        if site_tier != 'None' :
          site_subtier = site_opts.get( 'SubTier', 'None' )
          has_grid_elem = False
          xml_site = xml_append( xml_doc, xml_root, 'atp_site', name = site_name )

          # CE info
          ces = gConfig.getSections( 'Resources/Sites/%s/%s/CEs' % ( middleware, site ) )
          if ces[ 'OK' ]:
            res = self.__writeCEInfo( xml_doc, middleware, xml_site, site, ces[ 'Value' ] )
            # Update has_grid_elem
            has_grid_elem = res or has_grid_elem

          # SE info
          if site_opts.has_key( 'SE' ) and  ( site_tier in [ '0', '1', '2' ] or site_subtier in ['T2-D'] ):
            res = self.__writeSEInfo( xml_doc, xml_site, site )
            # Update has_grid_elem
            has_grid_elem = res or has_grid_elem

          # Site info will be put if we found at least one CE, SE or LFC element
          if has_grid_elem:
            xml_append( xml_doc, xml_site, 'group', name = 'Tier ' + site_tier, type = 'LHCb_Tier' )
            xml_append( xml_doc, xml_site, 'group', name = site, type = 'LHCb_Site' )
            xml_append( xml_doc, xml_site, 'group', name = site, type = 'All Sites' )
            try:
              if site_subtier == 'T2-D':
                xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1/2D' )
                xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 2D' )

              elif int( site_tier ) == 2:
                xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 2' )

              else:  # site_tier can be only 1 or 0, (see site_tier def above to convince yourself.)
                # If site_type is None, then we go to the exception.
                xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1/2D' )
                xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1' )

            except ValueError:  # Site tier is None, do nothing
              pass

          else :
            _msg = "Site %s, (WLCG Name: %s) has no CE, SE or LFC, thus will not be put into the xml"
            _msg = _msg % ( site, site_name )
            self.log.warn( _msg )
            xml_root.removeChild( xml_site )

    self.dryRun = self.am_getOption( 'DryRun', self.dryRun )
    if self.dryRun:
      self.log.info( "Dry Run: XML file will not be created, just printed" )
      print xml_doc.toxml()

    else:
      # produce the xml
      with open( self.xmlPath + "lhcb_topology.xml", 'w' ) as xmlf:
        xmlf.write( xml_doc.toxml() )

      self.log.info( "XML file created Successfully" )

    return S_OK()

  ## Private methods ###########################################################

  @staticmethod
  def __writeHeaderInfo( xml_doc, xml_root ):
    """
      Writes XML document header.
    """

    xml_append( xml_doc, xml_root, 'title', 'LHCb Topology Information for ATP' )
    xml_append( xml_doc, xml_root, 'description',
                     'List of LHCb site names for monitoring and mapping to the SAM/WLCG site names' )
    xml_append( xml_doc, xml_root, 'feed_responsible',
                     dn = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=roiser/CN=564059/CN=Stefan Roiser',
                     name = 'Stefan Roiser' )
    xml_append( xml_doc, xml_root, 'last_update',
                     time.strftime( '%Y-%m-%dT%H:%M:%SZ', time.gmtime() ) )
    xml_append( xml_doc, xml_root, 'vo', 'lhcb' )

  @staticmethod
  def __writeCEInfo( xml_doc, middleware, xml_site, site, ces ):
    """ Writes CE information in the XML Document
    """

    has_grid_elem = False


    for site_ce_name in ces:

      has_grid_elem = True

      site_ce_opts = gConfig.getOptionsDict( 'Resources/Sites/%s/%s/CEs/%s' % ( middleware, site, site_ce_name ) )
      if not site_ce_opts['OK']:
        gLogger.error( site_ce_opts['Message'] )
        continue
      site_ce_opts = site_ce_opts['Value']

      site_ce_type = site_ce_opts.get( 'CEType' )
      mappingCEType = { 'LCG':'CE', 'CREAM':'CREAM-CE', 
                       'ARC':'ARC-CE', 'HTCondorCE':'HTCONDOR-CE', 
                       'Vac':'VAC', 'Cloud':'CLOUD', 'Boinc':'BOINC' }

      xml_append( xml_doc, xml_site, 'service', hostname = site_ce_name,
                       flavour = mappingCEType.get( site_ce_type, 'UNDEFINED' ) )

    return has_grid_elem

  @staticmethod
  def __writeSEInfo( xml_doc, xml_site, site ):
    """ Writes SE information in the XML Document
    """
    def __write_SE_XML(site_se_opts):
      """
      Sub-function just to populate the XML with the SE values
      """  
      site_se_name = site_se_opts.get( 'Host' )
      site_se_flavour = site_se_opts.get( 'Protocol' )
      site_se_path = site_se_opts.get( 'Path', 'UNDEFINED' )
      mappingSEFlavour = { 'srm' : 'SRMv2', 
                          'root' : 'XROOTD', 'http' : 'HTTPS' } 
   
      xml_append( xml_doc, xml_site, 'service', 
                  hostname = site_se_name,
                  flavour = mappingSEFlavour.get( site_se_flavour, 'UNDEFINED' ),
                  path = site_se_path )

    has_grid_elem = True

    real_site_name = site.split( "." )[ 1 ]
    dmsHelper = DMSHelpers()
    
    t1 = dmsHelper.getSEInGroupAtSite( 'Tier1-DST', real_site_name )
    t2 = dmsHelper.getSEInGroupAtSite( 'Tier2D-DST', real_site_name )
    if not (t1['OK'] or t2['OK']):
      
      gLogger.error( t1.get( 'Message' ) or t2.get( 'Message' ) )
      return False
    
    storage_element_name_DST = t1['Value'] or t2['Value']
    if storage_element_name_DST:
      se_DST = StorageElement(storage_element_name_DST)
 
      storage_element_name_RAW = dmsHelper.getSEInGroupAtSite( 'Tier1-RAW', real_site_name )
      if not storage_element_name_RAW['OK']:
        gLogger.error( storage_element_name_RAW['Message'] )
        return False
      storage_element_name_RAW = storage_element_name_RAW['Value']
      se_RAW = None if not storage_element_name_RAW else StorageElement(storage_element_name_RAW)
  
      # Use case - Storage Element exists but it's removed from Resources/StorageElementGroups 
      if not (se_DST or se_RAW):
        gLogger.error( 'Storage Element for site ' + site + 
                       ' was found in Resources/Sites but not in the Storage Element Groups' )
        return False 

      se_plugins = se_DST.getPlugins()   
      if not se_plugins['OK']:
        gLogger.error( se_plugins['Message'] )
        return False
      
      for protocol in se_plugins['Value']:
        site_se_opts_DST = se_DST.getStorageParameters( protocol )
        if not site_se_opts_DST['OK']:
          gLogger.error( site_se_opts_DST[ 'Message' ] )
          return False
        site_se_opts_DST = site_se_opts_DST['Value']
        __write_SE_XML( site_se_opts_DST )

        
        site_se_opts_RAW = None if not se_RAW else se_RAW.getStorageParameters( protocol )
        if not site_se_opts_RAW:
          gLogger.error( 'No RAW Storage Element found for ' + site )
          continue
        else:
          if not site_se_opts_RAW['OK']:
            gLogger.error( site_se_opts_RAW['Message'] )
            return has_grid_elem
          site_se_opts_RAW = site_se_opts_RAW[ 'Value' ]                
          # This tests if the DST and RAW StorageElements have the same endpoint. 
          # If so it only uses the one already added.
          if site_se_opts_RAW[ 'Host' ] != site_se_opts_DST[ 'Host' ]:
            __write_SE_XML( site_se_opts_RAW )
  
      
    return has_grid_elem

################################################################################

def xml_append( doc, base, elem, cdata = None, **attrs ):
  """
    Given a Document, we append to it an element.
  """
  
  new_elem = doc.createElement( elem )
  for attr in attrs:
    new_elem.setAttribute( attr, attrs[ attr ] )
  if cdata:
    new_elem.appendChild( doc.createTextNode( cdata ) )

  return base.appendChild( new_elem )

#...............................................................................
#EOF
