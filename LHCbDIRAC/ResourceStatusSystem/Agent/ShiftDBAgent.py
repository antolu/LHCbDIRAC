""" LHCbDIRAC.ResourceStatusSystem.Agent.ShiftDBAgent

   ShiftDBAgent.__bases__:
     DIRAC.Core.Base.AgentModule.AgentModule

"""

import datetime
import os
import suds
import urllib2

from DIRAC                                           import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.Interfaces.API.DiracAdmin                 import DiracAdmin

from LHCbDIRAC.ResourceStatusSystem.Agent.ShiftEmail import getBodyEmail

__RCSID__  = '$Id: ShiftDBAgent.py 83225 2015-06-09 07:57:12Z joel $'
AGENT_NAME = 'ResourceStatus/ShiftDBAgent'

class ShiftDBAgent( AgentModule ):
  """ This agent queries the LHCb ShiftDB and gets the emails of the Production
      shifter. Then, populates the eGroup lhcb-grid-operations-alarms
  """

  # ShiftDB url where to find shifter emails
  __lbshiftdburl = 'https://lbshiftdb.cern.ch/shiftdb_list_mails.php'
  # eGroup user
  __user = 'lbdirac'
  # eGroup password
  __passwd = 'belikeapanda'
  # soap wsdl to access eGroups
  __wsdl = 'https://foundservices.cern.ch/ws/egroups/v1/EgroupsWebService/EgroupsWebService.wsdl'

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )

    # Members initialization

    self.lbshiftdburl = self.__lbshiftdburl
    self.user         = self.__user
    self.passwd       = self.__passwd
    self.wsdl         = self.__wsdl

    self.roles = {}
    self.roleShifters = {}
    self.newShifters = {}

    self.diracAdmin   = None

  def initialize( self ):
    """
     Initialize
    """

    self.user         = self.am_getOption( 'user', self.user )
    self.lbshiftdburl = self.am_getOption( 'lbshiftdburl', self.lbshiftdburl )
    self.wsdl         = self.am_getOption( 'wsdl', self.wsdl )

    pwfile = os.path.join( self.am_getWorkDirectory(), '.passwd' )

    passwd = self.__getPass( pwfile )
    if not passwd[ 'OK' ]:
      return passwd
    self.passwd = passwd[ 'Value' ]

    # Moved down to avoid crash
    self.diracAdmin   = DiracAdmin()

    return S_OK()

  def execute( self ):
    """ Execution
    """

    self.roles        = {}
    self.roleShifters = {}
    self.newShifters  = {}

    self.log.info( 'Getting roles from CS' )
    roles = self.__getRolesFromCS()
    if not roles[ 'OK' ]:
      self.log.error( roles[ 'Message' ] )
      return roles
    self.log.info( 'found %s ' % ', '.join( roles[ 'Value' ] ) )

    self.log.info( 'Getting role emails' )

    for role, eGroup in roles[ 'Value' ].items():

      self.roles[ role ] = eGroup

      email = self.__getRoleEmail( role )
      if not email[ 'OK' ]:
        self.log.error( email[ 'Message' ] )
        # We do not return, we keep execution to clean old shifters
        email[ 'Value' ] = None

      email = email[ 'Value' ]
      self.roleShifters[ eGroup ] = ( email, role )

      self.log.info( '%s -> %s' % ( role, email ) )

    self.log.info( 'Setting role emails' )
    for eGroup, roleTuple in self.roleShifters.items():

      email, role = roleTuple

      setEmail = self.__setRoleEmail( eGroup, email, role )
      if not setEmail[ 'OK' ]:
        self.log.error( setEmail[ 'Message' ] )

    for newShifterRole, shifterEgroup in self.newShifters.items():

      self.log.info( 'Notifying role %s' % newShifterRole )
      res = self.__notifyNewShifter( newShifterRole, shifterEgroup )
      if not res[ 'OK' ]:
        self.log.error( res[ 'Message' ] )

    return S_OK()

  def __getRolesFromCS( self ):
    """ Gets from the CS the roles we want to add to an eGroup.
        Role1 : { eGroup: egroup-blah } in the CS

        returns S_OK( { role1: egroup1, .. } )
    """

    _section = self.am_getModuleParam( 'section' )
    roles    = gConfig.getSections( '%s/roles' % _section )

    if not roles[ 'OK' ]:
      return roles

    eGroups = {}

    for role in roles[ 'Value' ]:
      eGroup = gConfig.getValue( '%s/roles/%s/eGroup' % ( _section, role ) )
      if eGroup:
        eGroups[ role ] = eGroup
        self.log.debug( 'Found %s : %s ' % ( role, eGroup ) )

    return S_OK( eGroups )

  def __getRoleEmail( self, role ):
    """ Get role email from shiftDB
    """

    try:
      web = urllib2.urlopen( self.lbshiftdburl, timeout = 60 )
    except urllib2.URLError, e:
      return S_ERROR( 'Cannot open URL: %s, erorr %s' % ( self.lbshiftdburl, e ) )

    now = datetime.datetime.now().hour
    emaillist = []
    emailperson = []

    for line in web.readlines():

      if role in line:

        # There are three shifts per day, so we take into account what time is it
        # before sending the email.
        morning, afternoon, evening = line.split( '|' )[ 4 : 7 ]

        if morning != '':
          emaillist.append( morning )
        if afternoon != '':
          emaillist.append( afternoon )
        if evening != '':
          emaillist.append( evening )

        for person in emaillist:
          if ':' in person:
            emailperson.append( person.split( ':' )[ 1 ].strip() )
        self.log.info( emailperson )
        return S_OK( emailperson )

    return S_ERROR( 'Email not found' )


  def __setRoleEmail( self, eGroup, email, role ):
    """ Set email in eGroup
    """

    client = suds.client.Client( self.wsdl, username = self.user, password = self.passwd )

    try:
      wgroup = client.service.FindEgroupByName( eGroup )
    except suds.WebFault, wError:
      return S_ERROR( wError )

    members = []
    lastShifterEmail = []
    lastShifterList = {}

    if hasattr( wgroup, 'warnings' ):
      if wgroup.warnings != []:
        self.log.warn( wgroup.warnings )
    #  return S_ERROR(wgroup.warnings)
    elif wgroup.result.Members:
      for members in wgroup.result.Members:
        lastShifterEmail.append( members.Email )
        lastShifterList[members.Email] = members

    if email is None:
      self.log.warn( "None email. Keeping previous one till an update is found." )
      return S_OK()

#    if lastShifterEmail.strip().lower() == email.strip().lower():
#      self.log.info( "%s has not changed as shifter, no changes needed" % email )
#      return S_OK()

    for lastShifter in lastShifterEmail:
      if email.count( lastShifter ) == 0 :
        self.log.info( "%s is not anymore shifter, deleting ..." % lastShifter )
        try:
        # The last boolean flag is to overwrite
          client.service.RemoveEgroupMembers( eGroup, lastShifterList[lastShifter] )
        except suds.WebFault, wError:
          return S_ERROR( wError )

    # Adding a member means it will be the only one in the eGroup, as it is overwritten
    if email != []:
      res = self.__addMember( email, client, eGroup )
      if not res[ 'OK' ]:
        self.log.error( res[ 'Message' ] )
        return res

    self.log.info( "%s added successfully to the eGroup for role %s" % ( email, role ) )
    self.newShifters[ role ] = eGroup

    return S_OK()

  def __addMember( self, email, client, wgroup ):
    """
    Adds a new member to the group
    """

    # Clear e-Group before inserting anything
    # self.__deleteMembers( client, wgroup )

    self.log.info( 'Adding member %s to eGroup %s' % ( email, wgroup ) )

    members = []
    for personEmail in email:
      newmember = client.factory.create( 'ns0:MemberType' )
      newmember.Type = "External"
      newmember.Email = personEmail

      members.append( newmember )

    try:
      # The last boolean flag is to overwrite
      client.service.AddEgroupMembers( wgroup, True, members )
    except suds.WebFault, wError:
      return S_ERROR( wError )
    return S_OK()


  @staticmethod
  def __getPass( pwfile ):
    """
    Reads password from local file
    """

    try:
      pwf    = open( pwfile )
      passwd = pwf.read()[ :-1 ]
    except IOError:
      return S_ERROR( 'Error: can\'t find file or read data' )

    pwf.close()
    return S_OK( passwd )

  def __notifyNewShifter( self, role, eGroup ):
    """
    Sends an email to the shifter ( if any ) at the beginning of the shift period.
    """

    body = getBodyEmail( role )

    if body is None:
      self.log.info( 'No email body defined for %s role' % role )
      return S_OK()

    if role == 'Production':
      prodRole = self.roles[ 'Production' ]
      geocRole = self.roles[ 'Grid Expert' ]
      body = body % ( self.roleShifters[ prodRole ][0], self.roleShifters[ geocRole ][0] )

    # Hardcoded Joel's email to avoid dirac@mail.cern.ch be rejected by smtp server
    res = self.diracAdmin.sendMail( '%s@cern.ch' % eGroup, 'Shifter information',
                                    body, fromAddress = 'joel.closier@cern.ch' )
    return res

#...............................................................................
#EOF
