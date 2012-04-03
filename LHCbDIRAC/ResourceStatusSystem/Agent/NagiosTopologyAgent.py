# $HeadURL: $
''' NagiosTopologyAgent:  

  This agent loops over the Dirac CS and extracts the necessary
  information to create a "topology map" which is used by the IT
  provided Nagios system to test Grid sites. The topology information
  defines the services to be tested.
     
'''

import os
import time
import xml.dom.minidom

from DIRAC                                import gConfig, S_OK, rootPath
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import Utils

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/NagiosTopologyAgent'

class NagiosTopologyAgent( AgentModule ):
  '''
  NagiosTopologyAgent, writes the xml topology consumed by Nagios to run
  the tests.
  '''
  # Too many public methods
  # pylint: disable-msg=R0904  
  
  def initialize( self ):
    '''
    Initialize the agent.
    '''
    
    # Attribute defined outside __init__  
    # pylint: disable-msg=W0201
    
    self.xmlPath      = rootPath + '/' + self.am_getOption( 'webRoot' )

    try:
      os.makedirs( self.xmlPath )
    except OSError:
      pass # The dirs exist already, or cannot be created: do nothing

    return S_OK()

  def execute( self ):
    '''
    Let's generate the xml file with the topology.
    '''
    
    # instantiate xml doc
    xml_impl = xml.dom.minidom.getDOMImplementation()
    xml_doc  = xml_impl.createDocument( None, 'root', None )
    xml_root = xml_doc.documentElement

    # xml header info
    self.__writeHeaderInfo( xml_doc, xml_root )
#    self.xml_append( xml_doc, xml_root, 'title', 'LHCb Topology Information for ATP')
#    self.xml_append( xml_doc, xml_root, 'description', 
#                     'List of LHCb site names for monitoring and mapping to the SAM/WLCG site names' )
#    self.xml_append( xml_doc, xml_root, 'feed_responsible', 
#                     dn = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=roiser/CN=564059/CN=Stefan Roiser', 
#                     name = 'Stefan Roiser' )
#    self.xml_append( xml_doc, xml_root, 'last_update', 
#                     time.strftime( '%Y-%m-%dT%H:%M:%SZ', time.gmtime() ) )
#    self.xml_append( xml_doc, xml_root, 'vo', 'lhcb' )

    # loop over sites
    for site in Utils.unpack( gConfig.getSections( '/Resources/Sites/LCG' ) ):

      # Site config
      site_opts     = Utils.unpack( gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site ) )
      site_name     = site_opts.get( 'Name' )
      site_tier     = site_opts.get( 'MoUTierLevel', 'None' )
      has_grid_elem = False
      xml_site      = self.xml_append( xml_doc, xml_root, 'atp_site', name = site_name )

      # CE info
      ces = gConfig.getSections( '/Resources/Sites/LCG/%s/CEs' % site )
      if ces[ 'OK' ]:
        res = self.__writeCEInfo( xml_doc, xml_site, site, ces[ 'Value' ] )
        # Update has_grid_elem
        has_grid_elem = res or has_grid_elemen
         
#      if gConfig.getSections( '/Resources/Sites/LCG/%s/CEs' % site )[ 'OK' ]:
#        for site_ce_name in gConfig.getSections( '/Resources/Sites/LCG/%s/CEs' % site )[ 'Value' ] :
#          has_grid_elem = True
#          site_ce_opts  = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s/CEs/%s' % ( site,site_ce_name ) )[ 'Value' ]
#          site_ce_type  = site_ce_opts.get( 'CEType' )
#          if site_ce_type == 'LCG': 
#            site_ce_type = 'CE'
#          elif site_ce_type == 'CREAM': 
#            site_ce_type = 'CREAM-CE'
#          elif not site_ce_type: 
#            site_ce_type = 'UNDEFINED'
#          self.xml_append( xml_doc, xml_site, 'service', hostname = site_ce_name, 
#                           flavour = site_ce_type )

      # SE info
      if site_opts.has_key( 'SE' ) and site_tier in [ '0', '1' ]:
        res = self.__writeSEInfo( xml_doc, xml_site, site )
        # Update has_grid_elem
        has_grid_elem = res or has_grid_elemen
        
#        has_grid_elem  = True
#        real_site_name = site.split( "." )[ 1 ] if site.split( "." )[ 1 ] != "NIKHEF" else "SARA"
#        site_se_opts   = gConfig.getOptionsDict( '/Resources/StorageElements/%s-RAW/AccessProtocol.1' % real_site_name )[ 'Value' ]
#        site_se_name   = site_se_opts.get( 'Host' )
#        site_se_type   = site_se_opts.get( 'ProtocolName' )
#        if site_se_type == 'SRM2': 
#          site_se_type = 'SRMv2'
#        elif not site_se_type: 
#          site_se_type = 'UNDEFINED'
#        self.xml_append( xml_doc, xml_site, 'service', hostname = site_se_name, 
#                         flavour = site_se_type )

      # FileCatalog info
      sites = gConfig.getSections( '/Resources/FileCatalogs/LcgFileCatalogCombined' )
      if sites[ 'OK' ] and site in sites[ 'Value' ]:
        res = self.__writeFileCatalogInfo( xml_doc, xml_site, site )
        # Update has_grid_elem
        has_grid_elem = res or has_grid_elemen      
      
#      if site in gConfig.getSections( '/Resources/FileCatalogs/LcgFileCatalogCombined' )[ 'Value' ]:
#        has_grid_elem = True
#        site_fc_opts  = gConfig.getOptionsDict( '/Resources/FileCatalogs/LcgFileCatalogCombined/%s' % site )[ 'Value' ]
#        if site_fc_opts.has_key( 'ReadWrite' ): 
#          self.xml_append( xml_doc, xml_site, 'service', 
#                           hostname = site_fc_opts.get( 'ReadWrite' ), 
#                           flavour = 'Central-LFC' )
#        if site_fc_opts.has_key( 'ReadOnly' ): 
#          self.xml_append( xml_doc, xml_site, 'service', 
#                           hostname = site_fc_opts.get( 'ReadOnly' ), 
#                           flavour = 'Local-LFC' )

      # Site info will be put if we found at least one CE, SE or LFC element
      if has_grid_elem:
        self.xml_append( xml_doc, xml_site, 'group', name = 'Tier ' + site_tier, 
                         type = 'LHCb_Tier' )
        self.xml_append( xml_doc, xml_site, 'group', name = site, type = 'LHCb_Site' )
        self.xml_append( xml_doc, xml_site, 'group', name = site, type = 'All Sites' )
        try:
          if int( site_tier ) == 2:
            self.xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1/2' )

          else: # site_tier can be only 1 or 0, (see site_tier def above to convince yourself.)
            # If site_type is None, then we go to the exception.
            self.xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1/2' )
            self.xml_append( xml_doc, xml_site, 'group', name = site, type = 'Tier 0/1' )

        except ValueError: # Site tier is None, do nothing
          pass

      else :
        self.log.warn( "Site %s, (WLCG Name: %s) has no CE, SE or LFC, thus will not be put into the xml" % ( site, site_name ) )
        xml_root.removeChild( xml_site )

    # produce the xml
    xmlf = open( self.xmlPath + "lhcb_topology.xml", 'w' )
    try:
      xmlf.write( xml_doc.toxml() )
    finally:
      xmlf.close()

    return S_OK()

################################################################################

  def xml_append( self, doc, base, elem, cdata = None, **attrs ):
    '''
      Given a Document, we append to it an element.
    '''
    
    new_elem = doc.createElement( elem )
    for attr in attrs: 
      new_elem.setAttribute( attr, attrs[ attr ] )
    if cdata: 
      new_elem.appendChild( doc.createTextNode( cdata ) )
      
    return base.appendChild( new_elem )

  def __writeHeaderInfo( self, xml_doc, xml_root ):
    '''
      Writes XML document header.
    '''

    self.xml_append( xml_doc, xml_root, 'title', 'LHCb Topology Information for ATP')
    self.xml_append( xml_doc, xml_root, 'description', 
                     'List of LHCb site names for monitoring and mapping to the SAM/WLCG site names' )
    self.xml_append( xml_doc, xml_root, 'feed_responsible', 
                     dn = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=roiser/CN=564059/CN=Stefan Roiser', 
                     name = 'Stefan Roiser' )
    self.xml_append( xml_doc, xml_root, 'last_update', 
                     time.strftime( '%Y-%m-%dT%H:%M:%SZ', time.gmtime() ) )
    self.xml_append( xml_doc, xml_root, 'vo', 'lhcb' )
    
  def __writeCEInfo( self, xml_doc, xml_site, site, ces ):
    '''
      Writes CE information in the XML Document
    '''
    
    has_grid_elem = False
    
    #for site_ce_name in gConfig.getSections( '/Resources/Sites/LCG/%s/CEs' % site )[ 'Value' ]:
    for site_ce_name in ces:
      
      has_grid_elem = True
      
      site_ce_opts = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s/CEs/%s' % ( site, site_ce_name ) )
      if not site_ce_opts[ 'OK' ]:
        gLogger.error( site_ce_opts[ 'Message' ] )
        continue
      site_ce_opts = site_ce_opts[ 'Value' ]
      
      site_ce_type  = site_ce_opts.get( 'CEType' )
      
      if site_ce_type == 'LCG': 
        site_ce_type = 'CE'
      elif site_ce_type == 'CREAM': 
        site_ce_type = 'CREAM-CE'
      elif not site_ce_type: 
        site_ce_type = 'UNDEFINED'
    
      self.xml_append( xml_doc, xml_site, 'service', hostname = site_ce_name, 
                       flavour = site_ce_type )    
        
    return has_grid_elem  

  def __writeSEInfo( self, xml_doc, xml_site, site ):
    '''
      Writes SE information in the XML Document
    '''    
    has_grid_elem  = True
    
    splittedSite = site.split( "." )[ 1 ]
    if splittedSite != 'NIKHEF':
      real_site_name = splittedSite
    else:
      real_site_name = 'SARA'  
    
    #real_site_name = site.split( "." )[ 1 ] if site.split( "." )[ 1 ] != "NIKHEF" else "SARA"
    site_se_opts = gConfig.getOptionsDict( '/Resources/StorageElements/%s-RAW/AccessProtocol.1' % real_site_name )
    
    if not site_se_opts[ 'OK' ]:
      gLogger.error( site_se_opts[ 'Message' ] )
      return False
    site_se_opts = site_se_opts[ 'Value' ]
    
    site_se_name = site_se_opts.get( 'Host' )
    site_se_type = site_se_opts.get( 'ProtocolName' )
    
    if site_se_type == 'SRM2': 
      site_se_type = 'SRMv2'
    elif not site_se_type: 
      site_se_type = 'UNDEFINED'
    
    self.xml_append( xml_doc, xml_site, 'service', hostname = site_se_name, 
                     flavour = site_se_type )    

    return has_grid_elem

  def __writeFileCatalogInfo( self, xml_doc, xml_site, site ):
    '''
      Writes FileCatalog information in the XML Document
    '''

    has_grid_elem = True
    
    site_fc_opts  = gConfig.getOptionsDict( '/Resources/FileCatalogs/LcgFileCatalogCombined/%s' % site )
    if not site_fc_opts[ 'OK' ]:
      gLogger.error( site_fc_opts[ 'Message' ] )
      return False
    site_fc_opts = site_fc_opts[ 'Value' ]
    
    if site_fc_opts.has_key( 'ReadWrite' ): 
      self.xml_append( xml_doc, xml_site, 'service', 
                       hostname = site_fc_opts.get( 'ReadWrite' ), 
                       flavour = 'Central-LFC' )
    
    if site_fc_opts.has_key( 'ReadOnly' ): 
      self.xml_append( xml_doc, xml_site, 'service', 
                       hostname = site_fc_opts.get( 'ReadOnly' ), 
                       flavour = 'Local-LFC' )
  
    return has_grid_elem  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF