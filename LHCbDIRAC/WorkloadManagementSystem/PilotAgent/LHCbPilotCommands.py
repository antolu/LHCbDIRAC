""" LHCb-specific pilot commands
"""

__RCSID__ = "$Id$"

import subprocess
import os.path
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
      # also setting the correct environment to be used by dirac-configure, or whatever follows
      # (by default this is not needed, since with dirac-install works in the local directory)
      self.pp.installEnv = self._doSetupProject()
      self.log.info( "SetupProject DONE, for release %s" % self.pp.releaseVersion )

      # saving also in bashrc file for completeness... this is doing some horrible mangling unfortunately!
      fd = open( 'bashrc', 'w' )
      for var, val in self.pp.installEnv.iteritems():
        if var == '_' or 'SSH' in var or '{' in val or '}' in val:
          continue
        bl = "%s=%s\n" % ( var, val )
        fd.write( bl )
      fd.close()

    except OSError, e:
      print "Exception when trying SetupProject:", e
      self.log.warn( "SetupProject NOT DONE: starting traditional DIRAC installation" )
      super( LHCbInstallDIRAC, self ).execute()

      # saving as installation environment
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

    return environment

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

    self.log.debug( "self.pp.installEnv: %s" % str( self.pp.installEnv ) )

    if 'X509_CERT_DIR' in os.environ:
      self.log.debug( "X509_CERT_DIR is set in the host environment as %s, aligning installEnv to it" % os.environ['X509_CERT_DIR'] )
      self.pp.installEnv['X509_CERT_DIR'] = os.environ['X509_CERT_DIR']
    else:
      self.log.debug( "X509_CERT_DIR is not set in the host environment" )
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/certificates', '$VO_LHCB_SW_DIR/etc/grid-security/certificates']
      for candidate in candidates:
        self.log.debug( "Candidate directory for X509_CERT_DIR is %s" % candidate )
        if os.path.isdir( os.path.expandvars( candidate ) ):
          self.log.debug( "Setting X509_CERT_DIR=%s" % candidate )
          self.pp.installEnv['X509_CERT_DIR'] = candidate
          os.environ['X509_CERT_DIR'] = candidate
          break
        else:
          self.log.debug( "%s not found or not a directory" % candidate )

    if 'X509_CERT_DIR' not in self.pp.installEnv:
      self.log.error( "Could not find/set X509_CERT_DIR" )
      sys.exit( 1 )

    if 'X509_VOMS_DIR' in os.environ:
      self.log.debug( "X509_VOMS_DIR is set in the host environment as %s, aligning installEnv to it" % os.environ['X509_VOMS_DIR'] )
      self.pp.installEnv['X509_VOMS_DIR'] = os.environ['X509_VOMS_DIR']
    else:
      self.log.debug( "X509_VOMS_DIR is not set in the host environment" )
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/certificates', '$VO_LHCB_SW_DIR/etc/grid-security/certificates']
      for candidate in candidates:
        self.log.debug( "Candidate directory for X509_VOMS_DIR is %s" % candidate )
        if os.path.isdir( os.path.expandvars( candidate ) ):
          self.log.debug( "Setting X509_VOMS_DIR=%s" % candidate )
          self.pp.installEnv['X509_VOMS_DIR'] = candidate
          os.environ['X509_VOMS_DIR'] = candidate
          break
        else:
          self.log.debug( "%s not found" % candidate )

    if 'X509_VOMS_DIR' not in self.pp.installEnv:
      self.log.error( "Could not find/set X509_VOMS_DIR" )
      sys.exit( 1 )

    if 'DIRAC_VOMSES' in os.environ:
      self.log.debug( "DIRAC_VOMSES is set in the host environment as %s, aligning installEnv to it" % os.environ['DIRAC_VOMSES'] )
      self.pp.installEnv['DIRAC_VOMSES'] = os.environ['DIRAC_VOMSES']
    else:
      self.log.debug( "DIRAC_VOMSES is not set in the host environment" )
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/certificates', '$VO_LHCB_SW_DIR/etc/grid-security/certificates']
      for candidate in candidates:
        self.log.debug( "Candidate directory for DIRAC_VOMSES is %s" % candidate )
        if os.path.isdir( os.path.expandvars( candidate ) ):
          self.log.debug( "Setting DIRAC_VOMSES=%s" % candidate )
          self.pp.installEnv['DIRAC_VOMSES'] = candidate
          os.environ['DIRAC_VOMSES'] = candidate
          break
        else:
          self.log.debug( "%s not found" % candidate )

    if 'DIRAC_VOMSES' not in self.pp.installEnv:
      self.log.error( "Could not find/set DIRAC_VOMSES" )
      sys.exit( 1 )

    self.log.debug( 'X509_CERT_DIR = %s, %s' % ( self.pp.installEnv['X509_CERT_DIR'], os.environ['X509_CERT_DIR'] ) )
    self.log.debug( 'X509_VOMS_DIR = %s, %s' % ( self.pp.installEnv['X509_VOMS_DIR'], os.environ['X509_VOMS_DIR'] ) )
    self.log.debug( 'DIRAC_VOMSES = %s, %s' % ( self.pp.installEnv['DIRAC_VOMSES'], os.environ['DIRAC_VOMSES'] ) )

    # In any case do not download VOMS and CAs
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
