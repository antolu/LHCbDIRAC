#######################################################################
# $Id: UsersAndGroups.py,v 1.3 2008/07/03 07:10:59 rgracian Exp $
# File :   UsersAndGroups.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: UsersAndGroups.py,v 1.3 2008/07/03 07:10:59 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"
"""
  Update Users and Groups from VOMS on CS
"""
from DIRAC.Core.Base.Agent                    import Agent
from DIRAC.ConfigurationSystem.Client.CSAPI   import CSAPI
from DIRAC                                    import S_OK, S_ERROR

from DIRAC                                    import systemCall
from DIRAC                                    import List

AGENT_NAME = "LHCb/UsersAndGroups"

VOMS_SERVER = 'voms.cern.ch'

excludedRoles = []

class UsersAndGroups(Agent):

  def __init__( self ):

    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)
    self.vomsServer = VOMS_SERVER

  def initialize(self):
    result = Agent.initialize(self)
    
    return result

  def execute(self):

    ret = systemCall( 0, 'voms-proxy-init')
    if not ret['OK']:
      self.log.error('Could not create Proxy',ret['Message'])
      return ret

    csapi = CSAPI()
    currentUsers = csapi.listUsers()

    ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-roles'],)
    if not ret['OK']:
      self.log.error('Can not get Role List', ret['Message'])
      return ret

    roles = {}

    for item in List.fromChar(ret['Value'][1],'\n'):
      role = List.fromChar(item,'=')[1]
      if not role in excludedRoles:
        roles[role] = []

    ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-users'],)
    if not ret['OK']:
      self.log.error('Can not get User List', ret['Message'])
      return ret

    users = {}
    newUsers = []
    obsoleteUsers = []
    duplicateUsers = []
    multiDNUsers   = []
    for item in List.fromChar(ret['Value'][1],'\n'):
      dn,ca = List.fromChar(item,',')
      ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-user-attributes',dn,ca])
      if not ret['OK']:
        self.log.error('Can not not get User Alias',dn)
        continue
      # the output has the format nickname=<nickname>
      user = List.fromChar(ret['Value'][1],'=')[1]
      if user in users:
        if dn != users[user]['DN']:
          self.log.error('User Duplicated in VOMS:','%s = %s' % ( user, users[user]) )
          self.log.error('User Duplicated in VOMS:','%s = %s' % ( user, { 'DN':dn, 'CA':ca }) )
          multiDNUsers.append( user )
        duplicateUsers.append( user )
        continue
      users[user] = { 'DN':dn, 'CA':ca }

      if not user in currentUsers:
        newUsers.append(user)

      ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-user-roles', dn, ca] )
      if not ret['OK']:
        self.log.error('Can not not get User Roles', user)
        continue

      users[user]['Groups'] = []
      for newItem in List.fromChar(ret['Value'][1],'\n'):
        role = List.fromChar(newItem,'=')[1]
        if not role in roles:
          self.log.error( 'User Role not valid:','%s = %s' % ( user, newItem ) )
          continue
        roles[role].append(user)
        users[user]['Groups'].append( role )

    for user in currentUsers:
      if user not in users:
        self.log.error('Obsolete User', user )
      obsoleteUsers.append(user)

    self.log.info( 'New Users found', newUsers )
    self.log.info( 'Duplicated Users found', duplicateUsers )
    self.log.info( 'Obsolete Users found', obsoleteUsers )
    ret = csapi.deleteUsers( obsoleteUsers )    

    for role in roles:
      print role, roles[role]

    ret = systemCall( 0, 'voms-proxy-destroy' )

    self.log.info('Helo')
    return S_OK()
