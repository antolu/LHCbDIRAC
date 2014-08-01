""" LHCb-specific pilot commands
"""

__RCSID__ = "$Id$"

import re
import sys
import subprocess
import distutils.spawn

from pilotCommands import InstallDIRAC, ConfigureDIRAC

class InstallLHCbDIRAC( InstallDIRAC ):
  """ Try SetupProject LHCbDIRAC and fall back to dirac-install when the requested version is not in CVMFS
  """
  
  def __init__( self, pilotParams ):
    """ c'tor
    """
# FIXME: as it is here, this can disappear
    super( InstallLHCbDIRAC, self ).__init__( pilotParams )

  def execute(self):
    """ Standard module executed
    """
# FIXME: has to find a solution to the problem of getting a pilot version beforehand (see mail to Andrei)
# than based on that decide if to make SetupProject (that anyway needs a fallback) or not

    try:
      __retCode__, diracEnv, setupProjectRelease = self._doSetupProject()
      self.log.debug( "diracEnv: %s" % diracEnv )
      if diracEnv:
        self.log.info( "SetupProject DONE, the current release version is: %s" % setupProjectRelease )
      else:
        self.log.warn( "SetupProject NOT DONE: start traditional DIRAC installation" )
        super( InstallLHCbDIRAC, self ).execute()
    except OSError, e:
      print e
      self.log.error( "SetupProject NOT FOUND" )
      sys.exit( -1 )


  def _doSetupProject( self ):  # execute SetupProject
    """ do SetupProject LHCbDIRAC
    """

    self.log.debug( "Executing SetupProject" )

    _p = subprocess.Popen( ". /cvmfs/lhcb.cern.ch/lib/LbLogin.sh && . SetupProject.sh LHCbDirac && printenv ",
                           shell = True, stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE, close_fds = False )

    # Creating the dictionary containing the LHCbDirac environment
    outData = {}
    first = True
    for line in _p.stdout:
      self.log.debug( line )
      if line.find( 'LHCbDirac' ) != -1 and first:
        SetupProjectRelease = re.findall( r'(?<=LHCbDirac)(.*?)(?=ready)', line )
        SetupProjectRelease = SetupProjectRelease[0]
        SetupProjectRelease = SetupProjectRelease.strip()
        first = False
      if line.find( "=" ) != -1:
        line = line.strip()
        parts = line.split( '=' )
        outData[parts[0]] = parts[1]
    returnCode = _p.wait()
    return ( returnCode, outData, SetupProjectRelease )



class ConfigureLHCbDIRAC( ConfigureDIRAC ):
  """ Configure LHCbDIRAC and with dirac-configure
  """
# FIXME: this is broken right now

  def execute( self ):
    """ Standard module executed
    """
    diracScriptsPath = distutils.spawn.find_executable( 'dirac-configure', diracEnv ).replace( "/dirac-configure", "/" )

    rootPath = diracScriptsPath.replace( "/InstallArea/scripts/", "/" )

    self.log.info( 'Using the DIRAC installation in %s' % rootPath )

    configure = diracPilotLast.ConfigureDIRAC( diracScriptsPath, rootPath, diracEnv, True )
    configure.setConfigureOpt()
    release = configure.releaseVersionList.split( ',' )
    if SetupProjectRelease in release:
      diracPilotLast.logINFO ( 'The release version %s is available in cvmfs' % SetupProjectRelease )
      configure.releaseVersion = SetupProjectRelease
      configure.execute()
      return True
    else:
      self.log.info( 'The release version %s is NOT available cvmfs: start DIRAC Installation' % release )
      return False

