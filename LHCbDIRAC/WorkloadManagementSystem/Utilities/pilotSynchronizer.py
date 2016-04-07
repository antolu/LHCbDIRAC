""" pilotSynchronizer

  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS, these are incorporated
  to the file.

"""

__RCSID__ = '$Id:  $'

import urllib

from DIRAC                                    import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.HTTPDISETConnection     import HTTPDISETConnection


class pilotSynchronizer( object ):
  '''
  Every time there is a successful write on the CS, pilotSynchronizer().sync() is
  executed. It updates the file with the values on the CS.

  '''

  def __init__( self ):
    '''
    '''
    self.pilotFileName = 'LHCb-pilot.json'
    # FIXME: pilotFileServer should contain the url of the web server where we will upload the LHCb-Pilot.json file
    self.pilotFileServer = '128.141.170.61'



  def sync( self, _eventName, _params ):
    '''
    Main synchronizer method.
    '''
    syncFile = self._syncFile()
    if not syncFile[ 'OK' ]:
      gLogger.error( syncFile[ 'Message' ] )
      return syncFile
    return S_OK()

  def _syncFile( self ):
    '''
      Compares CS with the file and does the necessary modifications.
    '''

    gLogger.info( '-- Synchronizing the file --' )
    pilotDict = {}
    setups = gConfig.getSections( 'DIRAC/Setups' )
    if not setups['OK']:
      gLogger.error( setups['Message'] )
      return setups
    setups['Value'].append( 'Defaults' )
    for setup in setups['Value']:
      options = gConfig.getOptionsDict( 'Operations/%s/Pilot' % setup )
      if not options['OK']:
        gLogger.error( options['Message'] )
        return options
      pilotDict[setup] = options['Value']
      commands = gConfig.getOptionsDict( 'Operations/%s/Pilot/Commands' % setup )
      if commands['OK']:
        pilotDict[setup]['Commands'] = commands['Value']
      else:
        gLogger.debug( "List of commands not found: %s" % commands['Message'] )
    result = self._upload( pilotDict )
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file" )
      return result
    return S_OK()

  def _upload ( self, pilotDict ):
    """ Method to upload the pilot file to the server.
    """

    params = urllib.urlencode( {'filename':'LHCb-pilot.json', 'data':pilotDict } )
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    con = HTTPDISETConnection( self.pilotFileServer, '8443' )
    con.request( "POST", "/DIRAC/upload", params, headers )
    resp = con.getresponse()
    if resp.status != 200:
      return S_ERROR( resp.status )
    return S_OK()
