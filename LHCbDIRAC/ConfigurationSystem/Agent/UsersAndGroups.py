'''
  Update Users and Groups from VOMS on CS
'''

import os
from time import time
from DIRAC                                           import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.Core.Security.VOMSService                 import VOMSService
from DIRAC.Core.Security                             import Locations, X509Chain
from DIRAC.Core.Utilities                            import List
from DIRAC.ConfigurationSystem.Client.CSAPI          import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from LHCbDIRAC.DataManagementSystem.Utilities.FCUtilities import chown
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

__RCSID__ = "$Id$"

class UsersAndGroups( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """
    Initialisation of the Users and Groups agent
    """
    AgentModule.__init__( self, *args, **kwargs )
    self.vomsSrv = None
    self.proxyLocation = ".volatileId"
    self.__adminMsgs = {}
    self.lfcDNs = None
    self.lfcBANDNs = None

  def initialize( self ):
    ''' Initialize method
    '''

    self.am_setOption( "PollingTime", 3600 * 6 ) # Every 6 hours
    self.vomsSrv = VOMSService()
    self.proxyLocation = os.path.join( self.am_getWorkDirectory() )
    return S_OK()

  def __generateProxy( self ):
    ''' Generate proxy, returns False if it fails.
    '''

    self.log.info( "Generating proxy..." )
    certLoc = Locations.getHostCertificateAndKeyLocation()
    if not certLoc:
      self.log.error( "Can not find certificate!" )
      return False

    chain = X509Chain.X509Chain()
    result = chain.loadChainFromFile( certLoc[0] )
    if not result[ 'OK' ]:
      self.log.error( "Can not load certificate file", "%s : %s" % ( certLoc[0], result[ 'Message' ] ) )
      return False

    result = chain.loadKeyFromFile( certLoc[1] )
    if not result[ 'OK' ]:
      self.log.error( "Can not load key file", "%s : %s" % ( certLoc[1], result[ 'Message' ] ) )
      return False

    result = chain.generateProxyToFile( self.proxyLocation, 3600 )
    if not result[ 'OK' ]:
      self.log.error( "Could not generate proxy file", result[ 'Message' ] )
      return False

    self.log.info( "Proxy generated" )
    return True

  def addEntryDFC( self, userDFC ):
    '''
    check and create if necessary an entry in the DFC
    '''
    from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    dfc = FileCatalogClient()
    recursive = False
    exists = False
    initial = userDFC[0]
    baseDir = os.path.join( '/lhcb', 'user', initial, userDFC )
    subDirectories = []
    if dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
      self.log.always( 'Directory already existing', baseDir )
      exists = True
      res = dfc.listDirectory( baseDir )
      if res['OK']:
        success = res['Value']['Successful'][baseDir]
        subDirectories = success['SubDirs']
        if success.get( 'SubDirs' ) or success.get( 'Files' ):
          self.log.always( 'Directory is not empty:', ' %d files / %d subdirectories' % ( len( success['Files'] ), len( success['SubDirs'] ) ) )
        elif recursive:
          self.log.always( "Empty directory, recursive is useless..." )
          recursive = False
    if not exists:
      self.log.always( 'Creating directory', baseDir )
      res = dfc.createDirectory( baseDir )
      if not res['OK']:
        self.log.fatal( 'Error creating directory', res['Message'] )
        return S_ERROR( 2 )
    else:
      self.log.always( 'Change%s ownership of directory' % ' recursively' if recursive else '', baseDir )
    res = chown( baseDir, userDFC, group = 'lhcb_user', mode = 0755, recursive = False, fcClient = dfc )
    if not res['OK']:
      self.log.fatal( 'Error changing directory owner', res['Message'] )
      return S_ERROR( 2 )
    startTime = time()
    if recursive and subDirectories:
      res = chown( subDirectories, userDFC, group = 'lhcb_user', mode = 0755, recursive = True, ndirs = 1, fcClient = dfc )
      if not res['OK']:
        self.log.fatal( 'Error changing directory owner', res['Message'] )
        return S_ERROR( 2 )
      self.log.always( 'Successfully changed owner in %d directories in %.1f seconds' % ( res['Value'], time() - startTime ) )
    else:
      self.log.always( 'Successfully changed owner in directory %s in %.1f seconds' % ( baseDir, time() - startTime ) )

  def checkLFCRegisteredUsers( self, usersData ):
    ''' Registers and re-registers users in the LFC
    '''

    self.log.info( "Checking DFC registered users" )

    usersToBeRegistered      = {}
    found                    = False

    for user in usersData:
      for userDN in usersData[ user ][ 'DN' ]:
        if not found:
          if userDN not in self.lfcDNs:
            self.log.info( 'DN "%s" need to be registered in LFC for user %s' % ( userDN, user ) )
            if user not in usersToBeRegistered:
              usersToBeRegistered[ user ] = []
            usersToBeRegistered[ user ].append( userDN )

    if usersToBeRegistered:
      result = self.changeLFCRegisteredUsers( usersToBeRegistered, 'add' )
      if not result[ 'OK' ]:
        self.log.error( 'Problem to add a new user : %s' % result[ 'Message' ] )

    return S_OK()


  def changeLFCRegisteredUsers( self, registerUsers, action ):
    '''
    add user to DFC
    '''
    self.log.info( "Changing DFC registered users" )

    if action == "add":
      subject = 'New DFC Users found'
      self.log.info( subject, ", ".join( registerUsers ) )
      from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
      dfc = FileCatalogClient()
      for lfcuser in registerUsers:
        for lfc_dn in registerUsers[lfcuser]:
          print lfc_dn
          self.addEntryDFC( lfcuser )

    return S_OK()


  def execute( self ):
    ''' Main method: execute
    '''

    result = self.__syncCSWithVOMS()

    mailMsg = ""
    if self.__adminMsgs[ 'Errors' ]:
      mailMsg += "\nErrors list:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Errors' ] )

    if self.__adminMsgs[ 'Info' ]:
      mailMsg += "\nRun result:\n  %s" % "\n  ".join( self.__adminMsgs[ 'Info' ] )

    NotificationClient().sendMail( self.am_getOption( 'MailTo', 'lhcb-vo-admin@cern.ch' ),
                                   "UsersAndGroupsAgent run log", mailMsg,
                                   self.am_getOption( 'mailFrom', 'Joel.Closier@cern.ch' ) )
    return result

  def __syncCSWithVOMS( self ):
    self.__adminMsgs = { 'Errors' : [], 'Info' : [] }

    #Get DIRAC VOMS Mapping
    self.address = self.am_getOption( 'MailTo', 'lhcb-vo-admin@cern.ch' )
    self.fromAddress = self.am_getOption( 'mailFrom', 'Joel.Closier@cern.ch' )
    self.log.info( "Getting DIRAC VOMS mapping" )
    from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    dfc = FileCatalogClient()
    mappingSection = '/Registry/VOMS/Mapping'
    ret = gConfig.getOptionsDict( mappingSection )
    if not ret['OK']:
      self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
      return ret
    vomsMapping = ret['Value']
    self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

    #Get VOMS VO name
    self.log.info( "Getting VOMS VO name" )
    result = self.vomsSrv.admGetVOName()
    if not result['OK']:
      self.log.fatal( 'Could not retrieve VOMS VO name' )
      return S_ERROR( 'Could not retrieve VOMS VO name' )

    voNameInVOMS = result[ 'Value' ]
    self.log.info( "VOMS VO Name is %s" % voNameInVOMS )

    #Get VOMS roles
    self.log.info( "Getting the list of registered roles in VOMS" )
    result = self.vomsSrv.admListRoles()
    if not result['OK']:
      self.log.fatal( 'Could not retrieve registered roles in VOMS' )
      return S_ERROR( 'Could not retrieve registered roles in VOMS' )

    rolesInVOMS = result[ 'Value' ]
    self.log.info( "There are %s registered roles in VOMS" % len( rolesInVOMS ) )

    #Map VOMS roles
    vomsRoles = {}
    for role in rolesInVOMS:
      role = "%s/%s" % ( voNameInVOMS, role )
      groupsForRole = []
      for group in vomsMapping:
        if vomsMapping[ group ] == role:
          groupsForRole.append( group )
      if groupsForRole:
        vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
    self.log.info( "DIRAC valid VOMS roles are:\n\t", "\n\t ".join( vomsRoles.keys() ) )

    #Get DIRAC users
    self.log.info( "Getting the list of registered users in DIRAC" )
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
    self.__adminMsgs[ 'Info' ].append( "There are %s registered users in DIRAC" % len( currentUsers ) )
    self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )

    #Get VOMS user entries
    self.log.info( "Getting the list of registered user entries in VOMS" )
    result = self.vomsSrv.admListMembers()
    if not result['OK']:
      self.log.fatal( 'Could not retrieve registered user entries in VOMS' )
    usersInVOMS = result[ 'Value' ]
    self.__adminMsgs[ 'Info' ].append( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )
    self.log.info( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )

    usersTmpInVOMS = []
    for checkUser in usersInVOMS:
      result = self.vomsSrv.admListCertificates( checkUser['DN'], checkUser['CA'] )
      if not result['OK']:
        self.__adminMsgs[ 'Errors' ].append( "Could not retrieve info for DN %s" % checkUser[ 'DN' ] )
        self.log.error( "Could not get nickname for DN %s" % checkUser[ 'DN' ] )
        return result
      for userDN in result['Value']:
        userNewDN = {}
        if userDN['subject'] != checkUser['DN']:
          userNewDN['DN'] = userDN['subject']
          userNewDN['CA'] = userDN['issuer']
          userNewDN['mail'] = checkUser['mail']
          usersTmpInVOMS.append( userNewDN )

    usersInVOMS += usersTmpInVOMS
    #Consolidate users by nickname
    usersData = {}
    newUserNames = []
    knownUserNames = []
    obsoleteUserNames = {}
    self.log.info( "Retrieving usernames..." )
    usersInVOMS.sort()
    for iUPos in xrange( len( usersInVOMS ) ):
      user = usersInVOMS[ iUPos ]
      result = self.vomsSrv.attGetUserNickname( user[ 'DN' ], user[ 'CA' ] )
      if not result[ 'OK' ]:
        self.__adminMsgs[ 'Errors' ].append( "Could not retrieve nickname for DN %s" % user[ 'DN' ] )
        self.log.error( "Could not get nickname for DN %s" % user[ 'DN' ] )
        continue
      else:
        userName = result[ 'Value' ]
      if not userName:
        self.log.error( "Empty nickname for DN %s" % user[ 'DN' ] )
        self.__adminMsgs[ 'Errors' ].append( "Empty nickname for DN %s" % user[ 'DN' ] )
        continue
      self.log.info( " (%02d %%) Found username %s" % ( ( iUPos * 100 / len( usersInVOMS ) ), userName ) )
      if userName not in usersData:
        usersData[ userName ] = { 'DN': [], 'CA': [], 'Email': [], 'Groups' : [] }
      for key in ( 'DN', 'CA', 'mail' ):
        value = user[ key ]
        if value:
          if key == "mail":
            List.appendUnique( usersData[ userName ][ 'Email' ], value )
          else:
            usersData[ userName ][ key ].append( value )
      if userName not in currentUsers:
        List.appendUnique( newUserNames, userName )
      else:
        List.appendUnique( knownUserNames, userName )
    self.log.info( "Finished retrieving usernames" )

    for user in currentUsers:
      if user not in usersData:
        self.log.info( 'User %s is no longer valid' % user )
        if user not in obsoleteUserNames:
          obsoleteUserNames[user] = []
        obsoleteUserNames[user].append( currentUsers[user]['DN'] )

    if newUserNames:
      self.log.info( "There are %s new users" % len( newUserNames ) )
    else:
      self.log.info( "There are no new users" )

    #Get the list of users for each group
    result = csapi.listGroups()
    if not result[ 'OK' ]:
      self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
      return result
    staticGroups = result[ 'Value' ]
    self.log.info( "Mapping users in VOMS to groups" )
    for vomsRole in vomsRoles:
      self.log.info( "  Getting users for role %s" % vomsRole )
      groupsForRole = vomsRoles[ vomsRole ][ 'Groups' ]
      vomsMap = vomsRole.split( "Role=" )
      vomsGroup = "Role=".join( vomsMap[:-1] )
      for g in groupsForRole:
        if g in staticGroups:
          staticGroups.pop( staticGroups.index( g ) )
      if vomsGroup[-1] == "/":
        vomsGroup = vomsGroup[:-1]
      vomsRole = "Role=%s" % vomsMap[-1]
      result = self.vomsSrv.admListUsersWithRole( vomsGroup, vomsRole )
      if not result[ 'OK' ]:
        errorMsg = "Could not get list of users for VOMS %s" % ( vomsMapping[ group ] )
        self.__adminMsgs[ 'Errors' ].append( errorMsg )
        self.log.error( errorMsg, result[ 'Message' ] )
        return result
      numUsersInGroup = 0
      for vomsUser in result[ 'Value' ]:
        for userName in usersData:
          if vomsUser[ 'DN' ] in usersData[ userName ][ 'DN' ]:
            numUsersInGroup += 1
            usersData[ userName ][ 'Groups' ].extend( groupsForRole )
      infoMsg = "There are %s users in group(s) %s for VOMS Role %s" % ( numUsersInGroup, ",".join( groupsForRole ), vomsRole )
      self.__adminMsgs[ 'Info' ].append( infoMsg )
      self.log.info( "  %s" % infoMsg )

    self.log.info( "Checking static groups" )
    for group in staticGroups:
      self.log.info( "  Checking static group %s" % group )
      numUsersInGroup = 0
      result = csapi.listUsers( group )
      if not result[ 'OK' ]:
        self.log.error( "Could not get the list of users in DIRAC group %s" % group , result[ 'Message' ] )
        return result
      for userName in result[ 'Value' ]:
        if userName in usersData:
          numUsersInGroup += 1
          usersData[ userName ][ 'Groups' ].append( group )
      infoMsg = "There are %s users in group %s" % ( numUsersInGroup, group )
      self.__adminMsgs[ 'Info' ].append( infoMsg )
      self.log.info( "  %s" % infoMsg )

    #Do the CS Sync
    self.log.info( "Updating CS..." )
    ret = csapi.downloadCSData()
    if not ret['OK']:
      self.log.fatal( 'Can not update from CS', ret['Message'] )
      return ret

    usersWithMoreThanOneDN = {}
    for user in usersData:
      csUserData = dict( usersData[ user ] )
      if len( csUserData[ 'DN' ] ) > 1:
        usersWithMoreThanOneDN[ user ] = csUserData[ 'DN' ]
      result = csapi.describeUsers( [ user ] )
      if result[ 'OK' ]:
        if result[ 'Value' ]:
          prevUser = result[ 'Value' ][ user ]
          prevDNs = List.fromChar( prevUser[ 'DN' ] )
          newDNs = csUserData[ 'DN' ]
          for DN in newDNs:
            if DN not in prevDNs:
              self.__adminMsgs[ 'Info' ].append( "User %s has new DN %s" % ( user, DN ) )
          for DN in prevDNs:
            if DN not in newDNs:
              self.__adminMsgs[ 'Info' ].append( "User %s has lost a DN %s" % ( user, DN ) )
        else:
          newDNs = csUserData[ 'DN' ]
          for DN in newDNs:
            self.__adminMsgs[ 'Info' ].append( "New user %s has new DN %s" % ( user, DN ) )
      for k in ( 'DN', 'CA', 'Email' ):
        csUserData[ k ] = ", ".join( csUserData[ k ] )
      result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
      if not result[ 'OK' ]:
        self.__adminMsgs[ 'Error' ].append( "Cannot modify user %s: %s" % ( user, result[ 'Message' ] ) )
        self.log.error( "Cannot modify user %s" % user )

    if newUserNames:
      subject = 'New DFC Users found'
      self.log.info( subject )
      from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
      dfc = FileCatalogClient()

      self.__adminMsgs[ 'Info' ].append( "\nNew users:" )
      for newUser in newUserNames:
        self.__adminMsgs[ 'Info' ].append( "  %s" % newUser )
        self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
        for DN in usersData[newUser][ 'DN' ]:
          self.__adminMsgs[ 'Info' ].append( "      DN = %s" % DN )
          self.__adminMsgs[ 'Info' ].append( "      CA = %s" % usersData[newUser]['CA'] )
        self.__adminMsgs[ 'Info' ].append( "    + EMail: %s" % usersData[newUser][ 'Email' ] )
        self.addEntryDFC( newUser )


    if usersWithMoreThanOneDN:
      self.__adminMsgs[ 'Info' ].append( "\nUsers with more than one DN:" )
      for uwmtod in sorted( usersWithMoreThanOneDN ):
        self.__adminMsgs[ 'Info' ].append( "  %s" % uwmtod )
        self.__adminMsgs[ 'Info' ].append( "    + DN list:" )
        for DN in usersWithMoreThanOneDN[uwmtod]:
          self.__adminMsgs[ 'Info' ].append( "      - %s" % DN )

    if obsoleteUserNames:
      self.__adminMsgs[ 'Info' ].append( "\nObsolete users:" )
      subject = 'Obsolete DFC Users found'
      body = 'Ban entries into DFC: \n'
      for obsoleteUser in obsoleteUserNames:
        self.log.info( subject, ", ".join( obsoleteUserNames ) )
        body += 'Ban user ' + obsoleteUser + '\n'
        self.__adminMsgs[ 'Info' ].append( "  %s" % obsoleteUser )
      self.log.info( "Banning %s users" % len( obsoleteUserNames ) )
      NotificationClient().sendMail( self.address, 'UsersAndGroupsAgent: %s' % subject, body, self.fromAddress )
      csapi.deleteUsers( obsoleteUserNames )

    result = csapi.commitChanges()
    if not result[ 'OK' ]:
      self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
      return result
    self.log.info( "Configuration committed" )

    return S_OK()

#...............................................................................
#EOF
