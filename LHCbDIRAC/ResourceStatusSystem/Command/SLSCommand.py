# $HeadURL: $
''' SLSCommand
  
  The Command class is a command class to properly interrogate the SLS.
  
'''

import re

from DIRAC                                      import S_OK
from DIRAC.Core.LCG                             import SLSClient
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities       import CSHelpers

__RCSID__ = "$Id$"

def slsid_of_service( granularity, name, type_ = None ):
  '''
    Return the SLS id of various services.
  '''
  
  if type_ == 'VO-BOX': 
    return name.split( '.' )[ 1 ] + '_VOBOX'
  elif type_ == 'VOMS': 
    return 'VOMS'
  elif ( granularity, type_ ) == ( 'StorageElement', None ): 
    st = CSHelpers.getSEToken( name )
    if not st['OK']:
      raise RuntimeError, st['Message']
    st = st['Value']
    return re.split( '[-_]', name )[ 0 ] + '_' + st
  elif type_ == 'CASTOR':
    try: 
      st = CSHelpers.getSEToken( name )
      if not st['OK']:
        raise RuntimeError, st['Message']
      st = st['Value']
      return 'CASTORLHCB_LHCB' + re.split( '[-_]', st )[1].upper()
    except IndexError: 
      return ''
  else: 
    return ''

class SLSStatusCommand( Command ):
  '''
    SLSStatusCommand
  '''
  
  def doCommand( self ):
    '''
    Return getAvailabilityStatus from SLS Client

    :attr:`args`:
     - args[0]: string: should be a granularity
     - args[1]: string: should be the (DIRAC) name of the granularity
     - args[2]: string: should be the granularity type (e.g. 'VO-BOX')
    '''

    super( SLSStatusCommand, self ).doCommand()
      
    res = SLSClient.getAvailabilityStatus( slsid_of_service( *self.args ) )

    if res[ 'OK' ]:
      res = S_OK( res[ 'Value' ][ 'Availability' ] )
        
    return { 'Result' : res }    

class SLSLinkCommand( Command ):
  '''
    SLSLinkCommand
  '''
  
  def doCommand( self ):
    '''
    Return getStatus from SLS Client

    :attr:`args`:
     - args[0]: string: should be a granularity
     - args[1]: string: should be the (DIRAC) name of the granularity
     - args[2]: string: should be the granularity type (e.g. 'VO-BOX')
    '''

    super( SLSLinkCommand, self ).doCommand()

    res = SLSClient.getAvailabilityStatus( slsid_of_service( *self.args ) )

    if res[ 'OK' ]:
      res = S_OK( res[ 'Value' ][ 'Weblink' ] )

    return { 'Result' : res }    

class SLSServiceInfoCommand( Command ):
  '''
    SLSServiceInfoCommand
  '''

  def doCommand( self ):
    '''
    Return getServiceInfo from SLS Client

    :attr:`args`:
     - args[0]: string: should be a ValidElement
     - args[1]: string: should be the (DIRAC) name of the ValidElement
    '''

    super( SLSServiceInfoCommand, self ).doCommand()
       
    if self.args[ 0 ] == 'StorageElement':
      slsName = slsid_of_service( self.args[ 0 ], self.args[ 1 ], 'CASTOR' )
    else:
      slsName = slsid_of_service( *self.args )

    res = SLSClient.getServiceInfo( slsName )

    return { 'Result' : res }    

#...............................................................................
#EOF
