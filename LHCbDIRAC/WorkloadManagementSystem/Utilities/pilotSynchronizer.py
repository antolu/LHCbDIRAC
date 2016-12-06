""" pilotSynchronizer

  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS, these are incorporated
  to the file.
  The module uploads to a web server the latest version of the pilot scripts.

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

  def __init__( self, paramDict ):
    ''' c'tor

        Just setting defaults
    '''
    self.pilotFileName = 'pilot.json'  # default filename of the pilot json file
    self.pilotFileServer = paramDict['pilotFileServer']  # domain name of the web server used to upload the pilot json file and the pilot scripts
    self.pilotRepo = paramDict['pilotRepo']  # repository of the pilot
    self.pilotVORepo = paramDict['pilotVORepo']  # repository of the VO that can contain a pilot extension
    self.pilotLocalRepo = 'pilotLocalRepo'  # local repository to be created
    self.pilotVOLocalRepo = 'pilotVOLocalRepo'  # local VO repository to be created
    self.pilotSetup = gConfig.getValue( '/DIRAC/Setup', '' )
    self.projectDir = paramDict['projectDir']
    self.pilotVOScriptPath = paramDict['pilotVOScriptPath']  # where the find the pilot scripts in the VO pilot repository
    self.pilotVOScript = paramDict['pilotVOScript']  # filename of the VO pilot script extension
    self.pilotScriptsPath = paramDict['pilotScriptsPath']  # where the find the pilot scripts in the pilot repository
    self.pilotVersion = ''
    self.pilotVOVersion =''

  def sync( self, _eventName, _params ):
    ''' Main synchronizer method.
    '''
    gLogger.notice( '-- Synchronizing the content of the pilot file %s with the content of the CS --' % self.pilotFileName  )

    self._syncFile()

    gLogger.notice( '-- Synchronizing the pilot scripts %s with the content of the repository --' % self.pilotRepo )

    self._syncScripts()

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
      if setup == self.pilotSetup:
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
          return ceList

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
    result = self._upload( pilotDict = pilotDict )
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file: %s" %result['Message'] )
      return result
    return S_OK()

  def _syncScripts(self):
    """Clone the pilot scripts from the repository and upload them to the web server
    """
    gLogger.info( '-- Uploading the pilot scripts --' )
    if os.path.isdir( self.pilotVOLocalRepo ):
      shutil.rmtree( self.pilotVOLocalRepo )
    os.mkdir( self.pilotVOLocalRepo )
    repo_VO = Repo.init( self.pilotVOLocalRepo )
    upstream = repo_VO.create_remote( 'upstream', self.pilotVORepo )
    upstream.fetch()
    upstream.pull( upstream.refs[0].remote_head )
    if repo_VO.tags:
      repo_VO.git.checkout( repo_VO.tags[self.pilotVOVersion], b = 'pilotScripts' )
    else:
      repo_VO.git.checkout( 'master', b = 'pilotVOScripts' )
    result = self._upload( filename = self.pilotVOScript, pilotScript = os.path.join( self.pilotVOLocalRepo, self.projectDir,
                                                                             self.pilotVOScriptPath, self.pilotVOScript ) )
    if not result['OK']:
      gLogger.error( "Error uploading the VO pilot script: %s" % result['Message'] )
      return result
    if os.path.isdir( self.pilotLocalRepo ):
      shutil.rmtree( self.pilotLocalRepo )
    os.mkdir( self.pilotLocalRepo )
    repo = Repo.init( self.pilotLocalRepo )
    releases = repo.create_remote( 'releases', self.pilotRepo )
    releases.fetch()
    releases.pull( releases.refs[0].remote_head )
    if repo.tags:
      with open( os.path.join( self.pilotVOLocalRepo, self.projectDir, 'releases.cfg' ), 'r' ) as releases_file:
        lines = [line.rstrip( '\n' ) for line in releases_file]
        lines = [s.strip() for s in lines]
        if self.pilotVOVersion in lines:
          self.pilotVersion = lines[( lines.index( self.pilotVOVersion ) ) + 3].split( ':' )[1]
      repo.git.checkout( repo.tags[self.pilotVersion], b = 'pilotScripts' )
    else:
      repo.git.checkout( 'master', b = 'pilotVOScripts' )
    try:
      result = self._upload( filename = 'dirac-pilot.py', pilotScript = os.path.join( self.pilotLocalRepo, self.pilotScriptsPath,
                                                                                      'dirac-pilot.py' ) )
      result = self._upload( filename = 'PilotCommands.py', pilotScript = os.path.join( self.pilotLocalRepo, self.pilotScriptsPath,
                                                                                        'PilotCommands.py' ) )
      result = self._upload( filename = 'PilotTools.py', pilotScript = os.path.join( self.pilotLocalRepo, self.pilotScriptsPath,
                                                                                     'PilotTools.py' ) )
    except ValueError:
      gLogger.error( "Error uploading the pilot scripts: %s" % result['Message'] )
      return result
    return S_OK()


  def _upload ( self, pilotDict = None, filename = '', pilotScript = '' ):
    """ Method to upload the pilot json file and the pilot scripts to the server.
    """

    if pilotDict:
      params = urllib.urlencode( {'filename':self.pilotFileName, 'data':json.dumps( pilotDict ) } )
    else:
      with open( pilotScript, "rb" ) as f:
        script = f.read()
      params = urllib.urlencode( {'filename':filename, 'data':script} )
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    con = HTTPDISETConnection( self.pilotFileServer, '443' )
    con.request( "POST", "/DIRAC/upload", params, headers )
    resp = con.getresponse()
    if resp.status != 200:
      return S_ERROR( resp.status )
    else:
      gLogger.info( '-- File and scripts upload done --' )
    return S_OK()
