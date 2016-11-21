""" pilotSynchronizer

  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS, these are incorporated
  to the file.

"""

import json
import urllib 
import shutil
import os
from git import Repo


from DIRAC                                    import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.HTTPDISETConnection     import HTTPDISETConnection

__RCSID__ = '$Id:  $'


class pilotSynchronizer( object ):
  '''
  Every time there is a successful write on the CS, pilotSynchronizer().sync() is
  executed. It updates the file with the values on the CS.

  '''

  def __init__( self, pilotScriptsLocation ):
    ''' c'tor

        Just setting defaults
    '''
    # pilotFileName/Server contain the default filename and domain name of the web server where we will upload the pilot json file
    self.pilotFileName = 'pilot.json'
    self.pilotFileServer = 'lhcb-portal-dirac.cern.ch'
    self.pilotScriptsLocation = 'https://github.com/DIRACGrid/Pilot.git'
    self.pilotVOScriptsLocation = 'https://:@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git'

    self.pilotLocalRepo = pilotRepo
    self.pilotVOLocalRepo = pilotVORepo
    self.pilotVersion = ''
    self.pilotVOVersion =''
    self.pilotSetup='LHCb-Production'

  def sync( self, _eventName, _params ):
    ''' Main synchronizer method.
    '''
    gLogger.notice( '-- Synchronizing the content of the pilot file %s with the content of the CS --' % self.pilotFileName  )

    pilotDict = self._syncFile()

    gLogger.notice( '-- Synchronizing the pilot scripts %s with the content of the repository --' % self.pilotScriptsLocation )

    self._syncScripts()

    result = self._upload( pilotDict )
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file: %s" %result['Message'] )
      return result

    return S_OK()

  def _syncFile( self ):
    ''' Creates the pilot dictionary from the CS, ready for encoding as JSON
    '''

    gLogger.info( '-- Getting the content of the CS --' )
    pilotDict = { 'Setups' : {}, 'CEs' : {} }
    setups = gConfig.getSections( '/Operations/' )
    if not setups['OK']:
      gLogger.error( setups['Message'] )
      return setups

    try:
      setups['Value'].remove( 'SoftwareDistribution' )
    except:
      pass

    for setup in setups['Value']:
      options = gConfig.getOptionsDict( '/Operations/%s/Pilot' % setup )
      if not options['OK']:
        gLogger.error( options['Message'] )
        return options
      # We include everything that's in the Pilot section for this setup
      if setup == pilotSetup:
        self.pilotVOVersion = options['Value']['Version']
      pilotDict['Setups'][setup] = options['Value']
      ceTypesCommands = gConfig.getOptionsDict( '/Operations/%s/Pilot/Commands' % setup )
      if ceTypesCommands['OK']:
        # It's ok if the Pilot section doesn't list any Commands too
        pilotDict['Setups'][setup]['Commands'] = {}        
        for ceType in ceTypesCommands['Value']:
          pilotDict['Setups'][setup]['Commands'][ceType] = ceTypesCommands['Value'][ceType].split(', ')
      if 'CommandExtensions' in pilotDict['Setups'][setup]:
        pilotDict['Setups'][setup]['CommandExtensions'] = pilotDict['Setups'][setup]['CommandExtensions'].split(', ')

    sitesSection = gConfig.getSections( '/Resources/Sites/' )
    if not sitesSection['OK']:
      gLogger.error( sitesSection['Message'] )
      return sitesSection

    for grid in sitesSection['Value']:
      gridSection = gConfig.getSections( '/Resources/Sites/' + grid )
      if not gridSection['OK']:
        gLogger.error( gridSection['Message'] )
        return gridSection

      for site in gridSection['Value']:
        ceList = gConfig.getSections( '/Resources/Sites/' + grid + '/' + site + '/CEs/' )
        if not ceList['OK']:
          gLogger.error( ceList['Message'] )
          return ceListSection
        
        for ce in ceList['Value']:
          ceType = gConfig.getValue( '/Resources/Sites/' + grid + '/' + site + '/CEs/' + ce + '/CEType')

          if ceType is None:
            # Skip but log it
            gLogger.error( 'CE ' + ce + ' at ' + site + ' has no option CEType! - skipping' )
          else:
            pilotDict['CEs'][ce] = { 'Site' : site, 'GridCEType' : ceType }


    defaultSetup = gConfig.getValue( '/DIRAC/DefaultSetup' )
    if defaultSetup:
      pilotDict['DefaultSetup'] = defaultSetup

    gLogger.verbose( "Got %s"  %str(pilotDict) )
    return pilotDict

  def _syncScripts(self):
    """Clone the pilot scripts from the repository and upload them to the web server
    """
    if os.path.isdir( self.pilotLocalRepo ):
      shutil.rmtree( self.pilotLocalRepo )

    os.mkdir( self.pilotLocalRepo )
    repo = Repo.init( self.pilotLocalRepo )
    upstream = repo.create_remote( 'upstream', self.pilotScriptsLocation )
    upstream.fetch()
    upstream.pull( upstream.refs[0].remote_head )
    if repo.tags:
      releasescfg = open(os.path.join(self.pilotVOLocalRepo,'LHCbDIRAC/releases.cfg'))
      if self.pilotVOVersion in releasescfg.read():
        repo.git.checkout( repo.tags[self.pilotVersion], b = 'pilotScripts' )
    else:
      repo.git.checkout( 'master', b = 'pilotScripts' )
    if self.pilotVOScriptsLocation:
      os.mkdir( self.pilotVOLocalRepo )
      repo_VO = Repo.init( self.pilotVOLocalRepo )
      releases = repo_VO.create_remote( 'releases', self.pilotVOScriptsLocation )
      upstream.fetch()
      upstream.pull( upstream.refs[0].remote_head )
      repo.git.checkout( repo.tags[self.pilotVOVersion], b = 'pilotScripts' )
    


  def _upload ( self, pilotDict ):
    """ Method to upload the pilot file to the server.
    """

    gLogger.info( "Synchronizing the content of the pilot file" )
    params = urllib.urlencode( {'filename':self.pilotFileName, 'data':json.dumps( pilotDict ) } )
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    con = HTTPDISETConnection( self.pilotFileServer, '443' )
    con.request( "POST", "/DIRAC/upload", params, headers )
    resp = con.getresponse()
    if resp.status != 200:
      return S_ERROR( resp.status )
    return S_OK()
