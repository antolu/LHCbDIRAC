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
  """ Try lb-run LHCbDIRAC and fall back to dirac-install when the requested version is not in CVMFS
  """

  def execute( self ):
    """ Standard module executed
    """
    try:
      # also setting the correct environment to be used by dirac-configure, or whatever follows
      # (by default this is not needed, since with dirac-install works in the local directory)
      self.pp.installEnv = self._do_lb_run()
      self.log.info( "lb-run DONE, for release %s" % self.pp.releaseVersion )

      # saving also in bashrc file for completeness... this is doing some horrible mangling unfortunately!
      fd = open( 'bashrc', 'w' )
      for var, val in self.pp.installEnv.iteritems():
        if var == '_' or 'SSH' in var or '{' in val or '}' in val:
          continue
        bl = "%s=%s\n" % ( var, val )
        fd.write( bl )
      fd.close()

    except OSError, e:
      print "Exception when trying lbrun:", e
      self.log.warn( "lb-run NOT DONE: starting traditional DIRAC installation" )
      super( LHCbInstallDIRAC, self ).execute()

      # saving as installation environment
      self.pp.installEnv = os.environ

  def _do_lb_run( self ):
    """ do lb-run LHCbDIRAC of the requested version. If the version does not exist, raise OSError
    """

    def __parseEnvironmentFile( eFile ):
      """ getting the produced environment saved in the file
      """
      environment = {}
      fp = open( eFile, 'r' )
      for line in fp:
        try:
          var = line.split( '=' )[0].strip()
          value = line.split( '=' )[1].strip()
          # FIXME: horrible hack... (there's a function that ends in the next line...)
          if '{' in value:
            value = value + '\n}'
          environment[var] = value
        except IndexError:
          continue
      return environment


    environment = os.environ
    if 'LHCb_release_area' not in environment:
      environment['LHCb_release_area'] = '/cvmfs/lhcb.cern.ch/lib/lhcb/'
    # when we reach here we expect to know the release version to install

    self.__invokeCmd( '. $LHCb_release_area/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.sh && printenv > environmentLbLogin',
                      environment )
    environment = __parseEnvironmentFile( 'environmentLbLogin' )

    self.__invokeCmd( 'lb-run LHCbDirac %s > environmentLHCbDirac' % self.pp.releaseVersion, environment )
    return __parseEnvironmentFile( 'environmentLHCbDirac' )



  def __invokeCmd( self, cmd, environment ):
    """ Controlled invoke of command via subprocess.Popen
    """

    self.log.debug( "Executing %s" % cmd )

    cmdExecution = subprocess.Popen( cmd, shell = True, env = environment,
                                     stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = False )
    if cmdExecution.wait() != 0:
      self.log.warn( "Problem executing . %s" % cmd )
      for line in cmdExecution.stderr:
        sys.stdout.write( line )
      raise OSError( "Can't do %s" % cmd )

    for line in cmdExecution.stdout:
      sys.stdout.write( line )


class LHCbConfigureBasics( LHCbCommandBase, ConfigureBasics ):
  """ Only case here, for now, is if to set or not the CAs and VOMS location, that should be found in CVMFS
  """

  def _getBasicsCFG( self ):
    super( LHCbConfigureBasics, self )._getBasicsCFG()

    # Adding SharedArea (which should be in CVMFS)
    if os.environ.has_key( 'VO_LHCB_SW_DIR' ):
      sharedArea = os.path.join( os.environ[ 'VO_LHCB_SW_DIR' ], 'lib' )
      self.log.debug( "Using VO_LHCB_SW_DIR at '%s'" % sharedArea )
      if os.environ[ 'VO_LHCB_SW_DIR' ] == '.':
        if not os.path.isdir( 'lib' ):
          os.mkdir( 'lib' )
    else: 
      sharedArea = '/cvmfs/lhcb.cern.ch/lib'
      self.log.warn( "Can't find shared area, forcing it to %s" % sharedArea )

    self.cfg.append( '-o /LocalSite/SharedArea=%s' % sharedArea )


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
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir', '$VO_LHCB_SW_DIR/etc/grid-security/vomsdir']
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
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/vomses', '$VO_LHCB_SW_DIR/etc/grid-security/vomses']
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

class LHCbCleanPilotEnv ( LHCbConfigureBasics ):
  """Delete the pilot.cfg and the pilot.json, needed for VMs.
     Force the use of the CS given by command line. The command avoids the use of the CS server address (lhcb-conf2)
     which would not work for some resources, e.g. BOINC.
  """

  def _getBasicsCFG( self ):
    super( LHCbCleanPilotEnv, self )._getBasicsCFG()
    if os.path.exists( self.pp.localConfigFile ):
      os.remove( self.pp.localConfigFile )
    localPilotCFGFile = self.pp.pilotCFGFile + "-local"
    if os.path.exists( localPilotCFGFile ):
      os.remove( localPilotCFGFile )
    self.cfg.append( " -o /DIRAC/Configuration/Servers=%s" % self.pp.configServer )

class LHCbConfigureCPURequirements( LHCbCommandBase, ConfigureCPURequirements ):

  # FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME
  # FIXME: this is just a copy/paste from DIRAC introducing the line for avoiding too slow CPUs

  def execute( self ):
    """ Get job CPU requirement and queue normalization
    """
    # Determining the CPU normalization factor and updating pilot.cfg with it
    configFileArg = ''
    if self.pp.useServerCertificate:
      configFileArg = '-o /DIRAC/Security/UseServerCertificate=yes'
    if self.pp.localConfigFile:
      configFileArg = '%s -R %s %s' % ( configFileArg, self.pp.localConfigFile, self.pp.localConfigFile )
    retCode, cpuNormalizationFactorOutput = self.executeAndGetOutput( 'dirac-wms-cpu-normalization -U %s' % configFileArg,
                                                                      self.pp.installEnv )
    if retCode:
      self.log.error( "Failed to determine cpu normalization [ERROR %d]" % retCode )
      self.exitWithError( retCode )

    # HS06 benchmark
    # FIXME: this is a hack!
    cpuNormalizationFactor = float( cpuNormalizationFactorOutput.split( '\n' )[0].replace( "Estimated CPU power is ",
                                                                                           '' ).replace( " HS06", '' ) )
    self.log.info( "Current normalized CPU as determined by 'dirac-wms-cpu-normalization' is %f" % cpuNormalizationFactor )
    if cpuNormalizationFactor < 3.0:
      self.log.info( "Current normalized CPU is too slow, exiting" )
      self.exitWithError( 1 )

    configFileArg = ''
    if self.pp.useServerCertificate:
      configFileArg = '-o /DIRAC/Security/UseServerCertificate=yes'
    retCode, cpuTime = self.executeAndGetOutput( 'dirac-wms-get-queue-cpu-time %s %s' % ( configFileArg,
                                                                                          self.pp.localConfigFile ),
                                                 self.pp.installEnv )
    if retCode:
      self.log.error( "Failed to determine cpu time left in the queue [ERROR %d]" % retCode )
      self.exitWithError( retCode )
    self.log.info( "CPUTime left (in seconds) is %s" % cpuTime )

    # HS06s = seconds * HS06
    self.pp.jobCPUReq = float( cpuTime ) * float( cpuNormalizationFactor )
    self.log.info( "Queue length (which is also set as CPUTimeLeft) is %f" % self.pp.jobCPUReq )

    # now setting this value in local file
    cfg = ['-FDMH']
    if self.pp.useServerCertificate:
      cfg.append( '-o  /DIRAC/Security/UseServerCertificate=yes' )
    if self.pp.localConfigFile:
      cfg.append( '-O %s' % self.pp.localConfigFile )  # our target file for pilots
      cfg.append( self.pp.localConfigFile )  # this file is also input
    cfg.append( '-o /LocalSite/CPUTimeLeft=%s' % str( int( self.pp.jobCPUReq ) ) )  # the only real option

    configureCmd = "%s %s" % ( self.pp.configureScript, " ".join( cfg ) )
    retCode, _configureOutData = self.executeAndGetOutput( configureCmd, self.pp.installEnv )
    if retCode:
      self.log.error( "Failed to update CFG file for CPUTimeLeft [ERROR %d]" % retCode )
      self.exitWithError( retCode )

    # FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME# FIXME

class LHCbConfigureSite( LHCbCommandBase, ConfigureSite ):
  pass

class LHCbConfigureArchitecture( LHCbCommandBase, ConfigureArchitecture ):
  """ just sets the CMTCONFIG variable
  """

  def execute( self ):
    """ calls the superclass execute and then sets the CMTCONFIG variable
    """
    localArchitecture = super( LHCbConfigureArchitecture, self ).execute()

    self.log.info( 'Setting variable CMTCONFIG=%s' % localArchitecture )
    os.environ['CMTCONFIG'] = localArchitecture
