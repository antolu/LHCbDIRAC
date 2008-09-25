# $Id: SrmSpaceTokenAgent.py,v 1.3 2008/09/25 16:36:32 acasajus Exp $

__author__ = 'Greig A Cowan'
__date__ = 'September 2008'
__version__ = 0.1

'''
Queries BDII to pick out information about SRM2.2 space token descriptions.
'''

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.Agent import Agent
from DIRAC.AccountingSystem.Client.Types.SRMSpaceTokenDeployment import SRMSpaceTokenDeployment
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.Core.Utilities import Time

import sys, os
import re
import ldap

AGENT_NAME = "LHCb/SrmSpaceTokenAgent"

class SrmSpaceTokenAgent(Agent):

  def __init__( self ):
    Agent.__init__( self, AGENT_NAME )

  def initialize( self ):
    result = Agent.initialize( self )
    if not result[ 'OK' ]:
      return result
    bdiiServerLocation = gConfig.getValue( "%s/BDIIServer" % self.section, 'lcg-bdii.cern.ch:2170' )
    self.bdiiServerPort = 2170
    bdiiSplit = bdiiServerLocation.split( ":" )
    self.bdiiServerHostname = bdiiSplit[0]
    if len( bdiiSplit ) > 1:
      try:
        self.bdiiServerPort = int( bdiiSplit[1] )
      except:
        pass
    return S_OK()

  #This method is the one being executed to do the real task
  def execute( self ):
    who = ''
    cred = ''
    gLogger.info( "Connecting to BDII server at %s:%s" % ( self.bdiiServerHostname,
                                                           self.bdiiServerPort ) )
    try:
      l = ldap.open( self.bdiiServerHostname, self.bdiiServerPort )
      l.simple_bind_s( who, cred)
      gLogger.info( 'Successfully bound to server\nSearching...' )
    except ldap.LDAPError, error_message:
      gLogger.error( 'Could not connect', ' to %s:%s : %s' % ( self.bdiiServerHostname,
                                                               self.bdiiServerPort,
                                                               str( error_message ) ) )
      return S_ERROR

    #sites = get_sites( l, site)
    # For this LHCb application, I'll hardcode the sites of interest
    sites = ['RAL-LCG2', 'FZK-LCG2', 'SARA-MATRIX',
             'CERN-PROD', 'UKI-SCOTGRID-ECDF', 'IN2P3-CC',
             'GRIF-LAL', 'csTCDie', 'INFN-T1', 'pic']
    sites = gConfig.getValue( "%s/Sites" % self.section, sites )
    now = Time.dateTime()
    for site in sites:
      #name = site[0][1]['GlueSiteUniqueID'][0]
      name = site
      ses = self.get_SEs( l, name)
      for se in ses:
        host = se[0][1]['GlueSEUniqueID'][0]
        gLogger.verbose( " Host: %s" % host )
        sas = self.get_SAs( l, host)
        tokens = self.get_VOInfo( l, host)
        for sa in sas:
          localID = sa[0][1]['GlueSALocalID'][0]
          for token in tokens:
            for i in token[0][1]['GlueChunkKey']:
              if i == 'GlueSALocalID='+localID and token[0][1]['GlueVOInfoName'][0].split(':')[0] == 'lhcb':
                acRecord = SRMSpaceTokenDeployment()
                acRecord.setValueByKey( "Site", name )
                acRecord.setValueByKey( "Hostname", host )
                acRecord.setValueByKey( "SpaceTokenDesc", token[0][1]['GlueVOInfoTag'][0] )
                saDict = sa[0][1]
                for acKey, glueKey in ( ( 'AvailableSpace', 'GlueSAStateAvailableSpace' ),
                                        ( 'UsedSpace', 'GlueSAStateUsedSpace' ),
                                        ( 'TotalOnline', 'GlueSATotalOnlineSize' ),
                                        ( 'UsedOnline', 'GlueSAUsedOnlineSize' ),
                                        ( 'FreeOnline', 'GlueSAFreeOnlineSize' ),
                                        ( 'ReservedOnline', 'GlueSAReservedOnlineSize' ),
                                        ( 'TotalNearline', 'GlueSATotalNearlineSize' ),
                                        ( 'UsedNearline', 'GlueSAUsedNearlineSize' ),
                                        ( 'FreeNearline', 'GlueSAFreeNearlineSize' ),
                                        ( 'ReservedNearline', 'GlueSAReservedNearlineSize' )
                                      ):
                  if glueKey in saDict:
                    acRecord.setValueByKey( acKey, int( saDict[ glueKey ][0] ) )
                  else:
                    #HACK: Some entries seem to have some glue keys missing
                    acRecord.setValueByKey( acKey, 0 )
                acRecord.setStartTime( now )
                acRecord.setEndTime( now )
                result = gDataStoreClient.addRegister( acRecord )
                if not result[ 'OK' ]:
                  return result

    return gDataStoreClient.commit()

  def get_sites( self, l, site):
    base = 'o=grid'
    scope = ldap.SCOPE_SUBTREE
    filter = 'GlueSiteUniqueID=%s' % site
    retrieve_attributes = ['GlueSiteUniqueID']
    return self.search( l, base, scope, filter, retrieve_attributes)

  def get_SEs( self, l, site):
    base = 'o=grid'
    scope = ldap.SCOPE_SUBTREE
    # It seems that some sites (all dCache?) are not publishing GlueForeignKey in this way
    filter = '(&(GlueSEUniqueID=*)(GlueForeignKey=GlueSiteUniqueID=%s))' % site
    retrieve_attributes = ['GlueSEUniqueID',
                           'GlueSEImplementationName',
                           'GlueSEImplementationVersion',
                           'GlueSEArchitecture',
                           'GlueSESizeFree',
                           'GlueSESizeTotal'
                           'GlueSEUsedOnlineSize',
                           'GlueSEUsedNearlineSize',
                           'GlueSETotalOnlineSize',
                           'GlueSchemaVersionMajor',
                           'GlueSchemaVersionMinor']

    return self.search( l, base, scope, filter, retrieve_attributes)

  def get_SAs( self, l, se):
    base = 'o=grid'
    scope = ldap.SCOPE_SUBTREE
    filter = '(&(GlueSALocalID=*)(GlueChunkKey=GlueSEUniqueID=%s))' % se
    retrieve_attributes = ['GlueSALocalID',
                           'GlueSAPath',
                           'GlueSAStateAvailableSpace',
                           'GlueSAStateUsedSpace',
                           'GlueSAAccessControlBaseRule',
                           'GlueSARetentionPolicy',
                           'GlueSAAccessLatency',
                           'GlueSATotalOnlineSize',
                           'GlueSAUsedOnlineSize',
                           'GlueSAFreeOnlineSize',
                           'GlueSAReservedOnlineSize',
                           'GlueSATotalNearlineSize',
                           'GlueSAUsedNearlineSize',
                           'GlueSAFreeNearlineSize',
                           'GlueSAReservedNearlineSize',
                           'GlueChunkKey']

    return self.search( l, base, scope, filter, retrieve_attributes)

  def get_VOInfo( self, l, se):
    base = 'o=grid'
    scope = ldap.SCOPE_SUBTREE
    filter = '(&(GlueVOInfoLocalID=*)\
    (GlueChunkKey=GlueSEUniqueID=%s)\
    (!(GlueVOInfoTag=UNAVAILABLE)))'\
    % ( se)

    retrieve_attributes = ['GlueVOInfoName',
                           'GlueVOInfoTag',
                           'GlueChunkKey']

    return self.search( l, base, scope, filter, retrieve_attributes)

  def search( self, l, base, scope, filter, retrieve_attributes):
    result_set = []
    timeout = 0
    try:
        result_id = l.search(base, scope, filter, retrieve_attributes)
        while True:
            result_type, result_data = l.result(result_id, timeout)
            if (result_data == []):
                break
            else:
                if result_type == ldap.RES_SEARCH_ENTRY:
                    result_set.append(result_data)
    except ldap.LDAPError, error_message:
        gLogger.warn( "Can't search for", "%s %s %s %s" % ( base, scope, filter, str( error_message ) ) )
    return result_set

