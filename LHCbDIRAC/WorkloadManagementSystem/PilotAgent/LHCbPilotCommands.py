""" LHCb-specific pilot commands
"""

__RCSID__ = "$Id$"

import subprocess
import os
import sys

from pilotCommands import CommandBase, InstallDIRAC, GetPilotVersion, ConfigureArchitecture

class GetLHCbPilotVersion( GetPilotVersion ):
  """ Only to set the location of the pilot cfg file
  """

  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( GetLHCbPilotVersion, self ).__init__( pilotParams )
    # FIXME: set correct ones
    self.pilotCFGFileLocation = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/'
    self.pilotCFGFile = '%s-pilot.json' % self.pp.releaseProject


class InstallLHCbDIRAC( InstallDIRAC ):
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
      super( InstallLHCbDIRAC, self ).execute()


  def _doSetupProject( self ):
    """ do SetupProject LHCbDIRAC of the requested version.
    
        If the version does not exist, or if there is an issue within LbLogin or SetupProject, raise OSError
    """

    environment = os.environ
    # when we reach here we expect to know the release version to install
    for cmd in [ '/cvmfs/lhcb.cern.ch/lib/LbLogin.sh', 'SetupProject.sh LHCbDirac %s' % self.pp.releaseVersion]:
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

class setServerCertificates( CommandBase ):
  """ To fix the use of server certificate path for IAAS and IAAC types of resources
  """

  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( setServerCertificates, self ).__init__( pilotParams )

  def execute( self ):
    """ Adds the following configuration:

        DIRAC
        {
          Security
          {
            CertFile = %s/etc/grid-security/hostcert.pem
            KeyFile  = %s/etc/grid-security/hostkey.pem
          }
        }
    """

    # this conf can be shared...
    cfg = ['-FHDM']  # force update, skip CA cheks, skip CA download, skip VOMS
    cfg.append( "-O pilot.cfg" )  # output file
    if self.pp.useServerCertificate:
      cfg.append( '--UseServerCertificate' )
    if self.pp.debugFlag:
      cfg.append( '-ddd' )

    # real stuff
    secPath = '/DIRAC/Security'
    gridSec = '%s/etc/grid-security' % self.pp.workingDir
    cfg.append( "-o %s/CertFile=%s/hostcert.pem" % ( secPath, gridSec ) )
    cfg.append( "-o %s/KeyFile=%s/hostkey.pem" % ( secPath, gridSec ) )

    configureCmd = "%s %s" % ( self.pp.configureScript, " ".join( cfg ) )

    retCode, _configureOutData = self.executeAndGetOutput( configureCmd, self.pp.installEnv )

    if retCode:
      self.log.error( "Could not configure the use of server certificates" )
      sys.exit( 1 )

class ConfigureLHCbArchitecture( ConfigureArchitecture ):
  """ just fix the script to be used
  """
  def __init__( self, pilotParams ):
    """ c'tor
    """
    pilotParams.architectureScript = 'dirac-architecture'
    super( ConfigureLHCbArchitecture, self ).__init__( pilotParams )

  def execute( self ):
    """ calls the superclass execute and then sets the CMTCONFIG variable
    """
    localArchitecture = super( ConfigureLHCbArchitecture, self ).execute()
    self.log.info( 'Setting CMTCONFIG=%s' % localArchitecture )
    os.environ['CMTCONFIG'] = localArchitecture

