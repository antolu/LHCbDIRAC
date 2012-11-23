# $HeadURL: $
''' ShiftDBAgent

  This agent queries the LHCb ShiftDB and gets the emails of the Production
  shifter. Then, populates the eGroup lhcb-grid-operations-alarms
   
'''

import os
import suds
import urllib2

from datetime import datetime

from DIRAC                                           import S_OK, S_ERROR, gConfig  
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.Interfaces.API.DiracAdmin                 import DiracAdmin

from LHCbDIRAC.ResourceStatusSystem.Agent.ShiftEmail import prodBody

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/ShiftDBAgent'

class ShiftDBAgent( AgentModule ):
  '''
    ShiftDBAgent
  '''
  
  def __init__( self, agentName, baseAgentName = False, properties = dict() ):

    AgentModule.__init__( self, agentName, baseAgentName, properties )

    self.user         = 'lbdirac'
    self.lbshiftdburl = 'https://lbshiftdb.cern.ch/shiftdb_list_mails.php'
    self.wsdl         = 'https://cra-ws.cern.ch/cra-ws/CraEgroupsWebService.wsdl'   
    self.passwd       = None

    self.roles        = {}    
    self.roleShifters = {}
    self.newShifters  = {}
    
    self.diracAdmin   = DiracAdmin()
  
  def initialize( self ):
    '''
     Initialize
    '''
   
    passwd = self.__getPass()
    if not passwd[ 'OK' ]:
      return passwd
    self.passwd = passwd[ 'Value' ]  
   
    return S_OK()
 
  def execute( self ):
    '''
     Execution
    '''  
   
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
    '''
    Gets from the CS the roles we want to add to an eGroup.
    Role1 : { eGroup: egroup-blah } in the CS
   
    returns S_OK( { role1: egroup1, .. } )
    '''  
   
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
    '''
    Get role email from shiftDB
    '''
       
    try:  
      web = urllib2.urlopen( self.lbshiftdburl, timeout = 60 )
    except urllib2.URLError, e:  
      return S_ERROR( 'Cannot open URL: %s, erorr %s' % ( self.lbshiftdburl, e ) )

    now = datetime.now().hour

    for line in web.readlines():
     
      if role in line:
       
        # There are three shifts per day, so we take into account what time is it
        # before sending the email.
        morning, afternoon, evening = line.split( '|' )[ 4 : 7 ]
        
        email = afternoon
        if now > 22 or now < 6:
          email = evening
        elif now < 14:
          email = morning
       
        if ':' in email:   
          email = email.split( ':' )[ 1 ].strip()        
          return S_OK( email )

    return S_ERROR( 'Email not found' )    

  def __setRoleEmail( self, eGroup, email, role ):
    '''
    Set email in eGroup
    '''
   
    client = suds.client.Client( self.wsdl )

    try:
      wgroup = client.service.findEgroupByName( self.user, self.passwd, eGroup )
    except suds.WebFault, wError:
      return S_ERROR( wError )  

    members = []
    if wgroup.result.Members:
      members = wgroup.result.Members[ 0 ]
     
    if len( members ) == 0:
      self.log.info( 'eGroup is empty' )
          
    elif len( members ) > 1:
      self.log.info( 'More than one user in the eGroup' )
      
    else:
      if email is None:
        self.log.warn( 'Get email returned None, deleting previous ... %s' % members[ 0 ].Email )
        return self.__deleteMembers( client, wgroup )
      elif members[ 0 ].Email.strip() == email.strip():
        self.log.info( '%s has not changed as shifter, no changes needed' % email )      
        return S_OK()     
      else:
        self.log.info( '%s is not anymore shifter, deleting ...' % members[ 0 ].Email )

    # Adding a member means it will be the only one in the eGroup, as it is overwritten
    res = self.__addMember( email, client, wgroup )
    if not res[ 'OK' ]:
      self.log.error( res[ 'Message' ] )
      return res
   
    self.log.info( '%s added successfully to the eGroup for role %s' % ( email, role ) )
    self.newShifters[ role ] = eGroup
       
    return S_OK()
  
  def __deleteMembers( self, client, wgroup ):
    '''
    Creates a new MembersType type and pushes it
    '''
    
    self.log.info( 'Creating a new MembersType type' )
      
    emptyMembers = client.factory.create( 'ns1:MembersType' )  
      
    return self.__syncGroupMembers( client, wgroup, emptyMembers )
   
  def __addMember( self, email, client, wgroup ):
    '''
    Adds a new member to the group
    '''
   
    self.log.info( 'Adding member %s to eGroup' % email )
   
    members         = client.factory.create( 'ns1:MembersType' )
   
    newmember       = client.factory.create( 'ns1:MemberType' )
    newmember.ID    = email
    newmember.Type  = "External"
    newmember.Email = email

    members.Member.append( newmember )
   
    return self.__syncGroupMembers( client, wgroup, members )

  def __syncGroupMembers( self, client, wgroup, members ):
    '''
    Synchronizes new group members
    '''

    wgroupName = wgroup.result.Name

    try:
      # The last boolean flag is to overwrite
      client.service.addEgroupMembers( self.user, self.passwd, wgroupName, members, True )
    except suds.WebFault, wError:
      return S_ERROR( wError )  

    return S_OK()

  def __getPass( self ):
    '''
    Reads password from local file
    '''
   
    # Future me, forgive me for this
    pwfile = os.path.join( self.am_getWorkDirectory(), '.passwd' )
   
    try:
      pwf    = open( pwfile )
      passwd = pwf.read()[ :-1 ]
    except IOError:
      return S_ERROR( 'Error: can\'t find file or read data' )
 
    pwf.close()  
    return S_OK( passwd )  

  def __notifyNewShifter( self, role, eGroup ):
    '''
    Sends an email to the shifter ( if any ) at the beginning of the shift period.
    '''

    if role == 'Production':
      
      prodRole = self.roles[ 'Production' ]
      geocRole = self.roles[ 'Grid Expert' ]
      body = prodBody % ( self.roleShifters[ prodRole ][0], self.roleShifters[ geocRole ][0] )
   
      # Hardcoded Joel's email to avoid dirac@mail.cern.ch be rejected by smtp server 
      res = self.diracAdmin.sendMail( '%s@cern.ch' % eGroup, 'Shifter information', 
                                       body, fromAddress = 'joel@mail.cern.ch' )
      return res
    
    else:
      self.log.info( 'No email body defined for %s role' % role )
      return S_OK()    
 
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF