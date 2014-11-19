""" LHCb-specific pilot commands
"""

__RCSID__ = "$Id$"

import subprocess
import os
import sys

from pilotCommands import GetPilotVersion, InstallDIRAC, ConfigureBasics, ConfigureCPURequirements, ConfigureSite, ConfigureArchitecture
from pilotTools import CommandBase

class LHCbCommandBase( CommandBase ):
  """ Simple extension, just for LHCb parameters
  """
  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( LHCbCommandBase, self ).__init__( pilotParams )
    pilotParams.pilotCFGFile = 'LHCb-pilot.json'
    pilotParams.localConfigFile = 'pilot.cfg'
    pilotParams.architectureScript = 'dirac-architecture'

class LHCbGetPilotVersion( LHCbCommandBase, GetPilotVersion ):
  pass

class LHCbInstallDIRAC( LHCbCommandBase, InstallDIRAC ):
  """ Try SetupProject LHCbDIRAC and fall back to dirac-install when the requested version is not in CVMFS
  """

  def execute( self ):
    """ Standard module executed
    """
    try:
      self._doSetupProject()
      self.log.info( "SetupProject DONE, for release %s" % self.pp.releaseVersion )
    except OSError, e:
      print "Exception when trying SetupProject:", e
      self.log.warn( "SetupProject NOT DONE: starting traditional DIRAC installation" )
      super( LHCbInstallDIRAC, self ).execute()
      self.pp.installEnv = os.environ


  def _doSetupProject( self ):
    """ do SetupProject LHCbDIRAC of the requested version.
    
        If the version does not exist, or if there is an issue within LbLogin or SetupProject, raise OSError
    """

    environment = os.environ
    if 'LHCb_release_area' not in environment:
      environment['LHCb_release_area'] = '/cvmfs/lhcb.cern.ch/lib/lhcb/'
    # when we reach here we expect to know the release version to install
    for cmd in ['$LHCb_release_area/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.sh',
                'SetupProject.sh LHCbDirac %s' % self.pp.releaseVersion]:
      environment = self.__invokeCmd( cmd, environment )

    # now setting the correct environment to be used by dirac-configure, or whatever follows
    # (by default this is not needed, since with dirac-install works in the local directory)
    self.pp.installEnv = environment


  def __invokeCmd( self, cmd, environment ):
    """ Controlled invoke of command via subprocess.Popen
    """

    self.log.debug( "Executing . %s && printenv > environment%s" % ( cmd, cmd.replace( ' ', '' ).split( '/' )[-1] ) )

    cmdExecution = subprocess.Popen( ". %s && printenv > environment%s" % ( cmd, cmd.replace( ' ', '' ).split( '/' )[-1] ),
                                     shell = True, env = environment,
                                     stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = False )
    if cmdExecution.wait() != 0:
      self.log.warn( "Problem executing %s" % cmd )
      for line in cmdExecution.stderr:
        sys.stdout.write( line )
      raise OSError( "Can't do %s" % cmd )

    for line in cmdExecution.stdout:
      sys.stdout.write( line )

    # getting the produced environment
    environmentProduced = {}
    fp = open( 'environment%s' % cmd.replace( ' ', '' ).split( '/' )[-1], 'r' )
    for line in fp:
      try:
        var = line.split( '=' )[0].strip()
        value = line.split( '=' )[1].strip()
        # FIXME: horrible hack... (there's a function that ends in the next line...)
        if '{' in value:
          value = value + '\n}'
        environmentProduced[var] = value
      except IndexError:
        continue

    return environmentProduced

class LHCbConfigureBasics( LHCbCommandBase, ConfigureBasics ):
  """ Only case here, for now, is if to set or not the CAs and VOMS location, that should be found in CVMFS
  """
  def _getSecurityCFG( self ):
    self.pp.installEnv['X509_CERT_DIR'] = '/cvmfs/grid.cern.ch/etc/grid-security/certificates'
    self.pp.installEnv['X509_VOMS_DIR'] = '/cvmfs/grid.cern.ch/etc/grid-security/vomsdir'
    self.cfg.append( '-DMH' )
    super( LHCbConfigureBasics, self )._getSecurityCFG()


class LHCbConfigureCPURequirements( LHCbCommandBase, ConfigureCPURequirements ):
  pass

class LHCbConfigureSite( LHCbCommandBase, ConfigureSite ):
  pass

class LHCbConfigureArchitecture( LHCbCommandBase, ConfigureArchitecture ):
  """ just fix the script to be used
  """

  def execute( self ):
    """ calls the superclass execute and then sets the CMTCONFIG variable
    """
    localArchitecture = super( LHCbConfigureArchitecture, self ).execute()

    self.log.info( 'Setting variable CMTCONFIG=%s' % localArchitecture )
    os.environ['CMTCONFIG'] = localArchitecture
