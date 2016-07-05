""" pilotSynchronizer

  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS, these are incorporated
  to the file.

"""

import urllib

from DIRAC                                    import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.HTTPDISETConnection     import HTTPDISETConnection

__RCSID__ = '$Id:  $'


class pilotSynchronizer( object ):
  '''
  Every time there is a successful write on the CS, pilotSynchronizer().sync() is
  executed. It updates the file with the values on the CS.

  '''

  def __init__( self ):
    ''' c'tor

        Just setting defaults
    '''
    self.pilotFileName = 'LHCb-pilot.json'
    # FIXME: pilotFileServer should contain the url of the web server where we will upload the LHCb-Pilot.json file
    self.pilotFileServer = '128.141.170.61'



  def sync( self, _eventName, _params ):
    ''' Main synchronizer method.
    '''
    gLogger.notice( '-- Synchronizing the content of the pilot file %s with the content of the CS --' % self.pilotFileName  )

    pilotDict = self._syncFile()

    result = self._upload( pilotDict )
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file: %s" %result['Message'] )
      return result

    return S_OK()

  def _syncFile( self ):
    ''' Compares CS with the file and does the necessary modifications.
    '''

    gLogger.info( '-- Getting the content of the CS --' )
    pilotDict = {}
    setups = gConfig.getSections( '/Operations/' )
    if not setups['OK']:
      gLogger.error( setups['Message'] )
      return setups
    setups['Value'].remove( 'SoftwareDistribution' )
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

    gLogger.verbose( "Got %s"  %str(pilotDict) )

    return pilotDict

  def _upload ( self, pilotDict ):
    """ Method to upload the pilot file to the server.
    """
    gLogger.info( "Synchronizing the content of the pilot file" )
    params = urllib.urlencode( {'filename':self.pilotFileName, 'data':pilotDict } )
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    con = HTTPDISETConnection( self.pilotFileServer, '8443' )
    con.request( "POST", "/DIRAC/upload", params, headers )
    resp = con.getresponse()
    if resp.status != 200:
      return S_ERROR( resp.status )
    return S_OK()
