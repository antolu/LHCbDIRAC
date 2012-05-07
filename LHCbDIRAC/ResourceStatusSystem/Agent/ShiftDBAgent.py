# $HeadURL: $
''' ShiftDBAgent

  This agent queries the LHCb ShiftDB and gets the emails of the Production
  shifter. Then, populates the eGroup lhcb-grid-operations-alarms
    
'''

import os
import suds
import urllib2

from DIRAC                                import S_OK, S_ERROR, gConfig  
from DIRAC.Core.Base.AgentModule          import AgentModule
from DIRAC.Interfaces.API.DiracAdmin      import DiracAdmin

from LHCbDIRAC.ResourceStatusSystem.Agent import ShiftEmail

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/ShiftDBAgent'

class ShiftDBAgent( AgentModule ):
  '''
    ShiftDBAgent 
  '''

  # Too many public methods
  # pylint: disable-msg=R0904

  def initialize( self ):
    '''
     Initialize
    '''

    # Attribute defined outside __init__  
    # pylint: disable-msg=W0201
    
    # To be extended
    
    self.user         = 'lbdirac'
    self.lbshiftdburl = 'https://lbshiftdb.cern.ch/shiftdb_list_mails.php'
    self.wsdl         = 'https://cra-ws.cern.ch/cra-ws/CraEgroupsWebService?WSDL'
    # Future me, forgive me for this
    self.pwfile = os.path.join( self.am_getWorkDirectory(), '.passwd' )
    
    passwd = self.__getPass()
    if not passwd[ 'OK' ]:
      return passwd
    self.passwd = passwd[ 'Value' ] 
    
    self.roles        = {}     
    self.roleShifters = {}
    self.newShifters  = {}

    self.diracAdmin = DiracAdmin()
    
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
    
    self.roleShifters = {}
    
    for role, eGroup in roles[ 'Value' ].items():
    
      self.roles[ role ] = eGroup
    
      email = self.__getRoleEmail( role )
      if not email[ 'OK' ]:
        self.log.error( email[ 'Message' ] )
        # We do not return, we keep execution to clean old shifters
        email[ 'Value' ] = None
        
      email                = email[ 'Value' ]
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
 
    #role = 'Production'
       
    try:   
      web = urllib2.urlopen( self.lbshiftdburl, timeout = 60 )
    except urllib2.URLError, e:  
      return S_ERROR( 'Cannot open URL: %s, erorr %s' % ( self.lbshiftdburl, e ) )

    for line in web.readlines():
      
      if line.find( role ) != -1:
        
        linesplitted = line.split( '|' )
        
        if linesplitted[ 4 ].find( ':' ) != -1 :
          email = linesplitted[ 4 ].split( ':' )[ 1 ]
          
          if email.find( '@' ) != -1:
            
            email = email.strip()
            
            return S_OK( email )
          else:
            return S_ERROR( '%s in %s should be an email but seems not' % ( email, linesplitted[ 4 ] ) ) 

    return S_ERROR( 'Email not found' )    

  def __setRoleEmail( self, eGroup, email, role ):
    '''
    Set email in eGroup
    '''
    
    client = suds.client.Client( self.wsdl )
    #eGroup = 'lhcb-grid-shifter-oncall'

    try:
      wgroup = client.service.findEgroupByName( self.user, self.passwd, eGroup )
    except suds.WebFault, wError:
      return S_ERROR( wError )  

    members = []
    if wgroup.Members:
      members = wgroup.Members[ 0 ]

    if len( members ) > 1 :
      self.log.error( 'The eGroup has more than one member, deleting ...' )  

      for _i in range( len( members ) ):
        self.log.error( 'Deleting member %s' % members[ 0 ].Email )
        del members[ 0 ]
      
      self.__syncGroup( client, wgroup )  
      
    elif len( members ) == 0:
      self.log.info( 'eGroup is empty, adding member')     

    else :
      if email is None:
        self.log.info( 'Get email returned None, deleting previous ... %s' % members[ 0 ].Email )
        del members[ 0 ]
        return self.__syncGroup( client, wgroup )
#        return S_OK()
      elif members[ 0 ].Email.strip() == email.strip():
        self.log.info( '%s has not changed as shifter, no changes needed' % email )       
        return S_OK()
      else:
        self.log.info( '%s is not anymore shifter, deleting ...' % members[ 0 ].Email )
        del members[ 0 ]
        self.__syncGroup( client, wgroup )

    res = self.__addMember( email, client, wgroup )
    if not res[ 'OK' ]:
      self.log.error( res[ 'Message' ] )
      return res
    
    self.log.info( '%s added successfully to the eGroup for role %s' % ( email, role ) )
    self.newShifters[ role ] = eGroup
        
    return S_OK()
    
  def __addMember( self, email, client, wgroup ):
    '''
    Adds a new member to the group
    '''
    
    self.log.info( 'Adding member %s to eGroup' % email )
    
    newmember       = client.factory.create( 'ns0:MemberType' )
    newmember.ID    = email
    newmember.Type  = "External"
    newmember.Email = email

    if not wgroup.Members:
      wgroup.Members = client.factory.create( 'ns0:MembersType' )   

    wgroup.Members[ 0 ].append( newmember )
    
    return self.__syncGroup( client, wgroup )

  def __syncGroup( self, client, wgroup ):
    '''
    Synchronizes new group
    '''

    try:
      client.service.synchronizeEgroup( self.user, self.passwd, wgroup )
    except suds.WebFault, wError:
      return S_ERROR( wError )  

    return S_OK()

  def __getPass( self ):
    '''
    Reads password from local file
    '''
    
    try:
      pwf    = open( self.pwfile )
      passwd = pwf.read()[ :-1 ]
    except IOError:
      return S_ERROR( 'Error: can\'t find file or read data' )
  
    pwf.close()  
    return S_OK( passwd )  

  def __notifyNewShifter( self, role, eGroup ):
    '''
    Sends an email to the shifter ( if any ) at the beginning of the shift period.
    '''
  
    if not ShiftEmail.emailBody.has_key( role ):
      self.log.info( 'No email body defined for %s role' % role )
      return S_OK()
    
    body = ShiftEmail.emailBody[ role ]
    if role == 'Production':
      prodRole = self.roles[ 'Production' ]
      geocRole = self.roles[ 'Grid Expert' ]
      body = body % ( self.roleShifters[ prodRole ][0], self.roleShifters[ geocRole ][0] )
    
    res = self.diracAdmin.sendMail( '%s@cern.ch' % eGroup, 'Shifter information', body )
    return res     
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF