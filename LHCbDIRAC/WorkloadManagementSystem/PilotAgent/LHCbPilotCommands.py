""" LHCb-specific pilot commands
"""

__RCSID__ = "$Id$"

# import re
# import sys
import subprocess
import os
import distutils.spawn

from pilotCommands import InstallDIRAC, ConfigureDIRAC, GetPilotVersion

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

    # now setting the correct paths/variables 
    # (by default this is not needed, since with dirac-install we work in the local directory)
    self.pp.installEnv = environment
    self.pp.rootPath = 


  def __invokeCmd( self, cmd, environment ):
    """ Controlled invoke of command via subprocess.Popen
    """

    self.log.debug( "Executing . %s && printenv > environment%s" % ( cmd, cmd.replace( ' ', '' ).split( '/' )[-1] ) )

    cmdExecution = subprocess.Popen( ". %s && printenv > environment%s" % ( cmd, cmd.replace( ' ', '' ).split( '/' )[-1] ),
                                     shell = True, env = environment,
                                     stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = False )
    if cmdExecution.wait() != 0:
      for line in cmdExecution.stderr:
        print line
      raise OSError, "Can't do %s" % cmd

    for line in cmdExecution.stdout:
      self.log.debug(line)

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




# class ConfigureLHCbDIRAC( ConfigureDIRAC ):
#   """ Configure LHCbDIRAC and with dirac-configure
#   """
# # FIXME: this is broken right now
#
#   def execute( self ):
#     """ Standard module executed
#     """
#     diracScriptsPath = distutils.spawn.find_executable( 'dirac-configure', diracEnv ).replace( "/dirac-configure", "/" )
#
#     rootPath = diracScriptsPath.replace( "/InstallArea/scripts/", "/" )
#
#     self.log.info( 'Using the DIRAC installation in %s' % rootPath )
#
#     configure = diracPilotLast.ConfigureDIRAC( diracScriptsPath, rootPath, diracEnv, True )
#     configure.setConfigureOpt()
#     release = configure.releaseVersionList.split( ',' )
#     if SetupProjectRelease in release:
#       diracPilotLast.logINFO ( 'The release version %s is available in cvmfs' % SetupProjectRelease )
#       configure.releaseVersion = SetupProjectRelease
#       configure.execute()
#       return True
#     else:
#       self.log.info( 'The release version %s is NOT available cvmfs: start DIRAC Installation' % release )
#       return False

