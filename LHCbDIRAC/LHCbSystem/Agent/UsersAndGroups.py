#######################################################################
# $Id: UsersAndGroups.py,v 1.4 2008/07/03 10:17:38 rgracian Exp $
# File :   UsersAndGroups.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: UsersAndGroups.py,v 1.4 2008/07/03 10:17:38 rgracian Exp $"
__VERSION__ = "$Revision: 1.4 $"
"""
  Update Users and Groups from VOMS on CS
"""
from DIRAC.Core.Base.Agent                    import Agent
from DIRAC.ConfigurationSystem.Client.CSAPI   import CSAPI
from DIRAC                                    import S_OK, S_ERROR, gConfig

from DIRAC                                    import systemCall
from DIRAC                                    import List

AGENT_NAME = "LHCb/UsersAndGroups"

VO_NAME     = 'lhcb'
VOMS_SERVER = 'voms.cern.ch'

class UsersAndGroups(Agent):

  def __init__( self ):

    """ Standard constructor
    """
    self.pollingTime = 3600*6
    Agent.__init__(self,AGENT_NAME)
    self.vomsServer = VOMS_SERVER

  def initialize(self):
    result = Agent.initialize(self)

    return result

  def execute(self):

    mappingSection = 'Security/VOMSMapping'
    ret = gConfig.getOptionsDict( mappingSection )
    if not ret['OK']:
      self.log.error('No VOMS to DIRAC Group Mapping Available')
      return ret

    vomsMapping = ret['Value']

    ret = systemCall( 0, 'voms-proxy-init')
    if not ret['OK']:
      self.log.error('Could not create Proxy',ret['Message'])
      return ret

    csapi = CSAPI()
    ret = csapi.listUsers()
    if not ret['OK']:
      self.log.error( 'Could not retrieve current list of Users' )
      return ret
    currentUsers = ret['Value']
    print currentUsers
    ret = csapi.describeUsers( currentUsers )
    if not ret['OK']:
      self.log.error( 'Could not retrieve current list of Users' )
      return ret

    currentUsers = ret['Value']

    ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-roles'],)
    if not ret['OK']:
      self.log.error('Can not get Role List', ret['Message'])
      return ret

    roles = {}

    for item in List.fromChar(ret['Value'][1],'\n'):
      role = '/%s/%s' % ( VO_NAME, item )
      for group in vomsMapping:
        if role == vomsMapping[group]:
          roles[role] = {'Group':group,'Users':[]}

    ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-users'],)
    if not ret['OK']:
      self.log.error('Can not get User List', ret['Message'])
      return ret

    users = {}
    newUsers = []
    oldUsers = []
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
      else:
        oldUsers.append(user)


      ret = systemCall(0,['voms-admin','--vo','lhcb','--host',self.vomsServer,'list-user-roles', dn, ca] )
      if not ret['OK']:
        self.log.error('Can not not get User Roles', user)
        continue

      users[user]['Groups'] = []
      for newItem in List.fromChar(ret['Value'][1],'\n'):
        role = newItem
        if not role in roles:
          self.log.error( 'User Role not valid:','%s = %s' % ( user, newItem ) )
          continue
        roles[role]['Users'].append(user)
        users[user]['Groups'].append( roles[role]['Group'] )

    for user in oldUsers:
      if 'diracAdmin' in currentUsers[user]['Groups']:
        users[user]['Groups'].append('diracAdmin')
      users[user]['Groups'].sort()
      currentUsers[user]['Groups'].sort()
      print 'Update User', user, users[user]['Groups'] == currentUsers[user]['Groups'], users[user]['Groups'], currentUsers[user]['Groups']
      ret = csapi.modifyUser( user, users[user] )
      if not ret['OK']:
        self.log.error( 'Fail to modifyUser User:', '(%s) %s' % ( user, users[user] ) )

    for user in newUsers:
      users[user]['Groups'].sort()
      print 'New User', user, users[user]['Groups']
      ret = csapi.addUser( user, users[user])
      if not ret['OK']:
        self.log.error( 'Fail to add User:', '(%s) %s' % ( user, users[user] ) )

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