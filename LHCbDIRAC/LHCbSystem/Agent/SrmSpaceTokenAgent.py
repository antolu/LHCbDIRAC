# $Id: SrmSpaceTokenAgent.py,v 1.1 2008/09/22 15:35:10 gcowan Exp $

__author__ = 'Greig A Cowan'
__date__ = 'September 2008'
__version__ = 0.1

'''
Queries BDII to pick out information about SRM2.2 space token descriptions.
'''

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Accounting.Types.SRMSpaceTokenDeployment import SRMSpaceTokenDeployment
from DIRAC.Core.Accounting.Client.DataStoreClient import gDataStoreClient

import sys, os
import re
import ldap

AGENT_NAME = "SrmSpaceTokenAgent"

class SrmSpaceTokenAgent(Agent):
  def __init__( self ):
    Agent.__init__( self, AGENT_NAME )

  def initialize( self ):
    result = Agent.initialize( self )
    if not result[ 'OK' ]:
      return result
    #HERE COMES YOUR SERIOUS INITIALIZATION THINGS
    result = self.initWorld()
    if not result[ 'OK' ]:
      return result
    worldName = result[ 'Value' ]
    self.registerWorld( worldName )
    return S_OK()

  #This method is the one being executed to do the real task
  def execute( self ):
    server = 'lcg-bdii.cern.ch:2170'
    who = ''
    cred = ''

    try:
        l = ldap.open( server)
        l.simple_bind_s( who, cred)
        print 'Successfully bound to server.'
        print 'Searching..\n'
    except ldap.LDAPError, error_message:
        print 'Could not connect. %s ' % error_message

    #sites = get_sites( l, site)
    # For this LHCb application, I'll hardcode the sites of interest
    sites = ['RAL-LCG2', 'FZK-LCG2', 'SARA-MATRIX',
             'CERN-PROD', 'UKI-SCOTGRID-ECDF', 'IN2P3-CC',
             'GRIF-LAL', 'csTCDie', 'INFN-T1', 'pic']
    for site in sites:
      #name = site[0][1]['GlueSiteUniqueID'][0]
      name = site
      ses = get_SEs( l, name)
      for se in ses:
        host = se[0][1]['GlueSEUniqueID'][0]
        print host
        sas = get_SAs( l, host)
        tokens = get_VOInfo( l, host)
        for sa in sas:
          localID = sa[0][1]['GlueSALocalID'][0]
          for token in tokens:
            for i in token[0][1]['GlueChunkKey']:
              if i == 'GlueSALocalID='+localID and token[0][1]['GlueVOInfoName'][0].split(':')[0] == 'lhcb':
                acRecord = SRMSpaceTokenDeployment()
                acRecord.setValue( "site", name )
                acRecord.setValue( "uniqueid", host )
                acRecord.setValue( "vo", token[0][1]['GlueVOInfoName'][0].split(':')[0] )
                acRecord.setValue( "voinfotag", token[0][1]['GlueVOInfoTag'][0] )
                acRecord.setValue( "availablespace", sa[0][1]['GlueSAStateAvailableSpace'][0] )
                acRecord.setValue( "usedspace", sa[0][1]['GlueSAStateUsedSpace'][0] )
                acRecord.setValue( "totalonline", sa[0][1]['GlueSATotalOnlineSize'][0] )
                acRecord.setValue( "usedonline", sa[0][1]['GlueSAUsedOnlineSize'][0] )
                acRecord.setValue( "freeonline", sa[0][1]['GlueSAFreeOnlineSize'][0] )
                acRecord.setValue( "reservedonline", sa[0][1]['GlueSAReservedOnlineSize'][0] )
                acRecord.setValue( "totalnearline", sa[0][1]['GlueSATotalNearlineSize'][0] )
                acRecord.setValue( "usednearline", sa[0][1]['GlueSAUsedNearlineSize'][0] )
                acRecord.setValue( "freenearline", sa[0][1]['GlueSAFreeNearlineSize'][0])
                acRecord.setValue( "reservednearline", sa[0][1]['GlueSAReservedNearlineSize'][0] )

                result = gDataStoreClient.addRegister( acRecord )
                if not result[ 'OK' ]:
                  return result

    return repClient.commit()

  def get_sites( self, l, site):
    base = 'o=grid'
    scope = ldap.SCOPE_SUBTREE
    filter = 'GlueSiteUniqueID=%s' % site
    retrieve_attributes = ['GlueSiteUniqueID']
    return search( l, base, scope, filter, retrieve_attributes)

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

    return search( l, base, scope, filter, retrieve_attributes)

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

    return search( l, base, scope, filter, retrieve_attributes)

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

    return search( l, base, scope, filter, retrieve_attributes)

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
        print error_message
    return result_set

