#######################################################################
# $Id: UsersAndGroups.py,v 1.36 2009/10/23 10:08:48 acasajus Exp $
# File :   UsersAndGroups.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: UsersAndGroups.py,v 1.36 2009/10/23 10:08:48 acasajus Exp $"
__VERSION__ = "$Revision: 1.36 $"
"""
  Update Users and Groups from VOMS on CS
"""
import os
from DIRAC.Core.Base.AgentModule              import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI   import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC                                    import S_OK, S_ERROR, gConfig, Source

from DIRAC                                    import systemCall, shellCall
from DIRAC                                    import List

VO_NAME     = 'lhcb'
VOMS_SERVER = 'voms.cern.ch'
GRIDENV     = ''

class UsersAndGroups(AgentModule):

  def initialize(self):
    self.vomsServer = VOMS_SERVER
    self.am_setOption( "PollingTime", 3600*6 ) # Every 6 hours
    self.gridEnv = GRIDENV
    return S_OK()
  
  def getMailForUser( self, userName ):
    #
    ret = shellCall( 0, "finger -m %s | grep Name: | sed 's/^.*Name: //g' | sed 's/ [A-Z]\. / /g' | sed 's/ /\./g'" % userName )
    if not ret[ 'OK' ]:
      self.log.error( "Could not get mail for user", "%s: %s" % ( userName, ret[ 'Message' ] ) )
      return ""
    exitStatus, stdout, stderr = ret[ 'Value' ]
    if exitStatus != 0:
      self.log.error( "Could not get mail for user", userName )
      return ""
    stdout = stdout.strip()
    if not stdout:
      return ""
    mailUser =  List.fromChar( stdout, "\n" )[0]
    if not mailUser:
      return ""
    return "%s@cern.ch" % mailUser

  def execute(self):

    self.log.info( 'Starting Agent loop')
    self.gridEnv = self.am_getOption( 'GridEnv', self.gridEnv )

    mappingSection = 'Security/VOMSMapping'
    ret = gConfig.getOptionsDict( mappingSection )
    if not ret['OK']:
      self.log.fatal('No VOMS to DIRAC Group Mapping Available')
      return ret

    vomsMapping = ret['Value']
    proxyFile = "%s/proxy" % self.am_getOption( 'ControlDirectory' )

    systemCall(0, ( 'voms-proxy-destroy' ) )

    ret = systemCall( 0, ( 'voms-proxy-init', '-out', proxyFile ) )
    if not ret['OK']:
      self.log.fatal('Could not create Proxy',ret['Message'])
      return ret
    if ret['Value'][0]:
      self.log.fatal('Could not create Proxy',ret['Value'][2])
      return ret


    os.environ[ 'X509_USER_PROXY' ] = proxyFile

    csapi = CSAPI()
    ret = csapi.listUsers()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current list of Users' )
      return ret
    currentUsers = ret['Value']

    ret = csapi.describeUsers( currentUsers )
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current User description' )
      return ret

    currentUsers = ret['Value']

    vomsAdminTuple = ('voms-admin','--vo','lhcb','--host',self.vomsServer)

    ret = self._gridCommand( 0, proxyFile, vomsAdminTuple + ('list-roles',) )
    if not ret['OK']:
      self.log.fatal('Can not get Role List', ret['Message'])
      return ret
    if ret['Value'][0]:
      self.log.fatal('Can not get Role List', ret['Value'][2])
      return ret

    roles = {}
    vomsRoles = []

    for item in List.fromChar(ret['Value'][1],'\n'):
      role = '/%s/%s' % ( VO_NAME, item )
      vomsRoles.append(role)
      for group in vomsMapping:
        if role == vomsMapping[group]:
          roles[role] = {'Group':group,'Users':[]}
    self.log.info( "DIRAC valid roles are:\n\t", "\n\t ".join( roles.keys() )  )

    ret = self._gridCommand(0, proxyFile, vomsAdminTuple + ('list-users',) )
    if not ret['OK']:
      self.log.fatal('Can not get User List', ret['Message'])
      return ret
    if ret['Value'][0]:
      self.log.fatal('Can not get User List', ret['Value'][2])
      return ret

    os.environ['LFC_HOST'] ='lfc-lhcb.cern.ch'
    lfcUsers = []
    newLFCUsers = {}
    retlfc = systemCall(0, ('lfc-listusrmap',) )
    if not retlfc['OK']:
      self.log.fatal('Can not get LFC User List', retlfc['Message'])
      return retlfc
    if retlfc['Value'][0]:
      self.log.fatal('Can not get LFC User List', retlfc['Value'][2])
      return retlfc
    else:
      for item in List.fromChar(retlfc['Value'][1],'\n'):
          dn = item.split(' ',1)[1]
          lfcUsers.append(dn)

    users = {}
    newUsers = []
    oldUsers = []
    obsoleteUsers = []
    duplicateUsers = []
    multiDNUsers   = []
    for item in List.fromChar(ret['Value'][1],'\n'):
      dn,ca = List.fromChar(item,',')
      ret = self._gridCommand(0, proxyFile, vomsAdminTuple + ('--nousercert', 'list-user-attributes', dn, ca))
      if not ret['OK']:
        self.log.error('Can not get User Alias',dn)
        continue
      if ret['Value'][0]:
        self.log.error('Can not get User Alias', ret['Value'][2])
        continue
      # the output has the format nickname=<nickname>
      if ret['Value'][0] <> 0:
        self.log.fatal( 'Error executing voms-admin command:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
        return S_ERROR( 'Error executing voms-admin command' )
      user = List.fromChar(ret['Value'][1],'=')[1]
      if user == 'None':
        self.log.error('Wrong nickname for:', '(%s) "%s"' % (user, dn) )
        continue
      if user in users:
        if dn != users[user]['DN']:
          self.log.error('User Duplicated in VOMS:','%s = %s' % ( user, users[user]) )
          self.log.error('User Duplicated in VOMS:','%s = %s' % ( user, { 'DN':dn, 'CA':ca }) )
          multiDNUsers.append( user )
          users[user]['DN'] += ', %s' % dn
          users[user]['CA'] += ', %s' % ca
        duplicateUsers.append( user )
        continue
      userMail = self.getMailForUser( user )
      if not userMail:
        userMail = '%s@cern.ch' % user
      self.log.info( "Mail for user %s is %s" % ( user, userMail ) )
      users[user] = { 'DN': dn, 'CA': ca , 'email': userMail }

      if not user in currentUsers:
        newUsers.append(user)
      else:
        oldUsers.append(user)

      # In some cases list-user-roles fails this will prevent user to get out of default groups
      users[user]['Groups'] = ['lhcb', 'private_pilot', 'user']
      ret = self._gridCommand(0, proxyFile, vomsAdminTuple + ('--nousercert', 'list-user-roles', dn, ca) )
      if not ret['OK']:
        self.log.error('Can not get User Roles', user)
        self.log.error('Error executing list-user-roles:', ret['Message'] )
        continue
      if ret['Value'][0]:
        self.log.error('Can not get User Roles', ret['Value'][2])
        self.log.error('Error executing list-user-roles:', '\n'.join( ret['Value']) )
        continue

      for newItem in List.fromChar(ret['Value'][1],'\n'):
        role = newItem
        if role not in vomsRoles:
          self.log.error( 'User Role not valid:','%s = %s' % ( user, newItem ) )
        if not role in roles:
          continue
        roles[role]['Users'].append(user)
        users[user]['Groups'].append( roles[role]['Group'] )
      self.log.verbose( "Groups for user %s are %s" % ( user, ", ".join( users[user]['Groups'] ) ) )

    ret = csapi.downloadCSData()
    if not ret['OK']:
      self.log.fatal('Can not update from CS', ret['Message'])
      return ret

    for user in oldUsers:
      for group in currentUsers[user]['Groups']:
        if group not in vomsMapping and group not in users[user]['Groups']:
          users[user]['Groups'].append(group)
          self.log.info( 'Keeping user %s in group %s' % (user, group ) )
      users[user]['Groups'].sort()
      currentUsers[user]['Groups'].sort()
      ret = csapi.modifyUser( user, users[user] )
      if not ret['OK']:
        self.log.error( 'Fail to modifyUser User:', '(%s) %s' % ( user, users[user] ) )
      userDN =  False
      for dn in lfcUsers:
        if dn in users[user]['DN'] :
          userDN = True
          break

      if not userDN:
        self.log.info('========= DN %s need to be registered in LFC for user %s' % (users[user]['DN'], user))
        newLFCUsers[user] = users[user]['DN']

    for user in multiDNUsers:
      userDN =  False
      for dn in lfcUsers:
        if dn in users[user]['DN'] :
          userDN = True
          break

      if not userDN:
        self.log.info('========= DN %s need to be registered in LFC for user %s' % (users[user]['DN'], user))
        newLFCUsers[user] = users[user]['DN']

    for user in duplicateUsers:
      userDN =  False
      for dn in lfcUsers:
        if dn in users[user]['DN'] :
          userDN = True
          break

      if not userDN:
        self.log.info('========= DN %s need to be registered in LFC for user %s' % (users[user]['DN'], user))
        newLFCUsers[user] = users[user]['DN']

    for user in newUsers:
      users[user]['Groups'].sort()
      ret = csapi.addUser( user, users[user])
      if not ret['OK']:
        self.log.error( 'Fail to add User:', '(%s) %s' % ( user, users[user] ) )

      userDN =  False
      for dn in lfcUsers:
        if dn == users[user]['DN'] :
          userDN = True
          break

      if not userDN:
        self.log.info('========= DN %s need to be registered in LFC for user %s' % (users[user]['DN'], user))
        newLFCUsers[user] = users[user]['DN']

    for user in currentUsers:
      if user not in users:
        self.log.error('Obsolete User', user )
        obsoleteUsers.append(user)

    address = self.am_getOption( 'mailTo', 'lhcb-vo-admin@cern.ch' )
    fromAddress = self.am_getOption( 'mailFrom', 'Joel.Closier@cern.ch' )
    if newLFCUsers:
      subject = 'New LFC Users found'
      self.log.info( subject, newLFCUsers )
      body = 'Command to add new entries into LFC: \n'
      body += 'login to volhcb11 and run : \n'
      body += 'source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.csh \n\n'
      for lfcuser in newLFCUsers.keys():
          for lfc_dn in newLFCUsers[lfcuser].split(','):
             body += 'add_DN_LFC --userDN="'+lfc_dn.strip()+'" --nickname='+lfcuser+'\n'

      NotificationClient().sendMail( address,'UsersAndGroupsAgent: ' + subject, body, fromAddress )

    if newUsers:
      subject = 'New Users found'
      self.log.info( subject, newUsers )
      body = ', '.join( newUsers )
      NotificationClient().sendMail( address,'UsersAndGroupsAgent: ' + subject, body, fromAddress )

    if duplicateUsers:
      subject = 'Duplicated Users found'
      self.log.info( subject, duplicateUsers )
      body = ', '.join( duplicateUsers )
      NotificationClient().sendMail( address,'UsersAndGroupsAgent: ' + subject, body, fromAddress )

    if multiDNUsers:
      subject = 'Users with multiple DN found'
      self.log.info( subject, multiDNUsers )
      body = ', '.join( multiDNUsers )
      NotificationClient().sendMail( address,'UsersAndGroupsAgent: ' + subject, body, fromAddress )

    if obsoleteUsers:
      subject = 'Obsolete Users found'
      self.log.info( subject, obsoleteUsers )
      body = ', '.join( obsoleteUsers )
      NotificationClient().sendMail( address,'UsersAndGroupsAgent: ' + subject, body, fromAddress )

    ret = csapi.deleteUsers( obsoleteUsers )

    ret = systemCall( 0, ( 'voms-proxy-destroy', '-file', proxyFile ) )

#    return S_OK()
    return csapi.commitChanges()

  def _gridCommand(self, timeout, proxyFile, cmd):
    """
     Execute cmd tuple after sourcing GridEnv
    """
    gridEnv = dict(os.environ)
    if self.gridEnv:
      self.log.verbose( 'Sourcing GridEnv script:', self.gridEnv )
      ret = Source( 10, [self.gridEnv] )
      if not ret['OK']:
        self.log.error( 'Failed sourcing GridEnv:', ret['Message'] )
        return S_ERROR( 'Failed sourcing GridEnv' )
      if ret['stdout']: self.log.verbose( ret['stdout'] )
      if ret['stderr']: self.log.warn( ret['stderr'] )
      gridEnv = ret['outputEnv']

    gridEnv[ 'X509_USER_PROXY' ] = proxyFile
    self.log.verbose( 'Executing', ' '.join(cmd) )
    return systemCall( timeout, cmd, env = gridEnv )