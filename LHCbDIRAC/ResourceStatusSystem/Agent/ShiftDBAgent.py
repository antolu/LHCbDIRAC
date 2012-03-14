# $HeadURL: $
''' ShiftDBAgent

  This agent queries the LHCb ShiftDB and gets the emails of the Production
  shifter. Then, populates the eGroup lhcb-grid-operations-alarms
    
'''

import os
import suds
import urllib2

from DIRAC                       import S_OK, S_ERROR  
from DIRAC.Core.Base.AgentModule import AgentModule

__RCSID__  = '$Id:  $'
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
    
    self.user     = 'lbdirac'
    
    # Future me, forgive me for this
    self.pwfile = os.path.join( self.am_getWorkDirectory(), '.passwd' )
    
    passwd = self.__getPass()
    if not passwd[ 'OK' ]:
      return passwd
    self.passwd = passwd[ 'Value' ] 
    
    return S_OK()
  
  def execute( self ):
    '''
     Execution
    '''  
    
    email = self.__getRoleEmail()
    if not email[ 'OK' ]:
      self.log.error( email[ 'Message' ] )
      return email
    
    email = email[ 'Value' ]
    
    setEmail = self.__setRoleEmail( email )
    if not setEmail[ 'OK' ]:
      self.log.error( setEmail[ 'Message' ] )
      return setEmail
    
#    self.log.info( '%s added successfully to the eGroup' % email )    
    
    return S_OK()
    
  def __getRoleEmail( self ):
    '''
    Get role email from shiftDB
    '''
 
    role = 'Production'
    lbshiftdburl = 'https://lbshiftdb.cern.ch/shiftdb_list_mails.php'
       
    web  = urllib2.urlopen( lbshiftdburl )

    for line in web.readlines():
      
      if line.find( role ) != -1:
        
        linesplitted = line.split( '|' )
        
        if linesplitted[ 4 ].find( ':' ) != -1 :
          email = linesplitted[ 4 ].split( ':' )[ 1 ]
          
          if email.find( '@' ) != -1:
            return S_OK( email )
          else:
            return S_ERROR( '%s in %s should be an email but seems not' % ( email, linesplitted[ 4 ] ) ) 

    return S_ERROR( 'Email not found' )  

  def __setRoleEmail( self, email ):
    '''
    Set email in eGroup
    '''
    
    wsdl   = 'https://cra-ws.cern.ch/cra-ws/CraEgroupsWebService?WSDL'
    client = suds.client.Client( wsdl )
    eGroup = 'lhcb-current-shifter'

    wgroup = client.service.findEgroupByName( self.user, self.passwd, eGroup )

    members = wgroup.Members[0]

    if len( members ) > 1 :
      self.log.error( 'The eGroup has more than one member, deleting ...' )  

      for _i in range( len( members ) ):
        self.log.error( 'Deleting member %s' % members[ 0 ].Email )
        del members[ 0 ]
      
    elif len( members ) == 0:
      self.log.info( 'eGroup is empty, adding member')     

    else :
      if members[ 0 ].Email.strip() == email.strip():
        self.log.info( '%s has not changed as shifter, no changes needed' % email )
        return S_OK()
      else:
        self.log.info( '%s is not anymore shifter, deleting ...' % members[ 0 ].Email )
        del members[ 0 ]

    res = self.__addMember( email, client, wgroup )
    if not res[ 'OK' ]:
      self.log.error( res[ 'Message' ] )
      return res
    
    return S_OK()
    
  def __addMember( self, email, client, wgroup ):
    '''
    Highly inestable function !!!
    '''
    
    self.log.info( 'Adding member %s to eGroup' % email )
    
    newmember       = client.factory.create( 'ns0:MemberType' )
    newmember.ID    = email
    newmember.Type  = "External"
    newmember.Email = email

    wgroup.Members[ 0 ].append( newmember )
    client.service.synchronizeEgroup( self.user, self.passwd, wgroup )

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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF