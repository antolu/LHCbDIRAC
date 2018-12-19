""" LHCb-specific pilot commands
"""

import os
import subprocess
import sys

from pilotCommands import GetPilotVersion, InstallDIRAC, ConfigureBasics  # pylint: disable=import-error
from pilotCommands import ConfigureCPURequirements, ConfigureSite, ConfigureArchitecture  # pylint: disable=import-error
from pilotTools import CommandBase  # pylint: disable=import-error

__RCSID__ = "$Id$"


# Utilities functions

def invokeCmd(cmd, environment=None):
  """ Controlled invoke of command via subprocess.Popen

  :param env: environment in a dict
  :type env: dict
  """
  print "Executing %s" % cmd
  cmdExecution = subprocess.Popen(cmd, shell=True, env=environment,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=False)
  if cmdExecution.wait() != 0:
    for line in cmdExecution.stderr:
      sys.stdout.write(line)
    raise OSError("Can't do %s" % cmd)

  for line in cmdExecution.stdout:
    sys.stdout.write(line)


def parseEnvironmentFile(eFile):
  """ getting the produced environment saved in the file

  :param eFile: file where to save env
  :type eFile: str

  :return: environment
  :rtype: dict
  """
  environment = {}
  fp = open(eFile, 'r')
  for line in fp:
    try:
      var = line.split('=')[0].strip()
      value = '='.join(line.split("=")[1:]).strip()
      if '{' in value:  # horrible hack... (there's a function that ends in the next line...)
        value = value + '\n}'
      if value:
        environment[var] = value
    except IndexError:
      continue
  fp.close()
  return environment


def saveEnvInFile(env, eFile):
  """ Save environment in file (delete if already present)

  :param env: environment in a dict
  :type env: dict
  :param eFile: file where to save env
  :type eFile: str
  """
  if os.path.isfile(eFile):
    os.remove(eFile)

  fd = open(eFile, 'w')
  for var, val in env.iteritems():
    if var == '_' \
       or var == 'X509_USER_PROXY' \
       or 'SSH' in var \
       or '{' in val \
       or '}' in val:
      continue
    if ' ' in val and val[0] != '"':
      val = '"%s"' % val
    bl = "export %s=%s\n" % (var, val.rstrip(":"))
    fd.write(bl)
  fd.close()


# LHCb pilot commands

class LHCbCommandBase(CommandBase):
  """ Simple extension, just for LHCb parameters
  """

  def __init__(self, pilotParams):
    """ c'tor
    """
    super(LHCbCommandBase, self).__init__(pilotParams)
    pilotParams.pilotCFGFile = 'LHCb-pilot.json'
    pilotParams.localConfigFile = 'pilot.cfg'
    pilotParams.architectureScript = 'dirac-architecture'


class LHCbGetPilotVersion(LHCbCommandBase, GetPilotVersion):
  """ Base is enough (pilotParams.pilotCFGFile = 'LHCb-pilot.json')
  """
  pass


class LHCbInstallDIRAC(LHCbCommandBase, InstallDIRAC):
  """ Try lb-run LHCbDIRAC and fall back to dirac-install when the requested version is not in CVMFS.
      When we reach here we expect to know the release version to install
  """

  def execute(self):
    """ Standard module executed
    """
    try:
      # also setting the correct environment to be used by dirac-configure, or whatever follows
      # (by default this is not needed, since with dirac-install works in the local directory)
      try:
        self.pp.installEnv = self._do_lb_login()
      except OSError as e:
        self.log.error("Invocation of LbLogin NOT successful ===> +++ABORTING+++")
        sys.exit(1)
      self.log.info("LbLogin DONE")
      try:
        self.pp.installEnv = self._do_get_lhcbdiracenv()
      except OSError:
        self.pp.installEnv = self._do_lb_run()
      self.log.info("source lhcbdirac env DONE, for release %s" % self.pp.releaseVersion)

    except OSError as e:
      self.log.error("Exception when trying to source the lhcbdirac environment:", e)
      if 'lbRunOnly' in self.pp.genericOption:
        self.exitWithError(1)
      else:
        self.log.warn("Source of the lhcbdirac environment NOT DONE: starting traditional DIRAC installation")
        super(LHCbInstallDIRAC, self).execute()

    finally:
      # saving also in environmentLHCbDirac file for completeness...
      # this is doing some horrible mangling unfortunately!
      # The content of environmentLHCbDirac will be the same as the content of environmentLbRunDirac
      # if lb-run LHCbDIRAC is successful
      saveEnvInFile(self.pp.installEnv, 'environmentLHCbDirac')

  def _do_lb_login(self):
    """ do LbLogin. If it doesn't work, the invokeCmd will raise OSError
    """
    environment = os.environ.copy()
    if 'LHCb_release_area' not in environment:
      environment['LHCb_release_area'] = '/cvmfs/lhcb.cern.ch/lib/lhcb/'

    # check for need of devLbLogin
    if 'devLbLogin' in self.pp.genericOption:
      invokeCmd('. $LHCb_release_area/LBSCRIPTS/dev/InstallArea/scripts/LbLogin.sh && printenv > environmentLbLogin',
                environment)
    else:
      invokeCmd('. $LHCb_release_area/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.sh && printenv > environmentLbLogin',
                environment)

    return parseEnvironmentFile('environmentLbLogin')

  def _do_get_lhcbdiracenv(self):
    """ get the LHCbDIRAC environment of the requested version. If the version does not exist, raise OSError

        Here, it tries:
        1. sourcing lhcbdirac from /cvmfs/lhcb.cern.ch
        2. if it fails, try sourcing lhcbdirac from /cvmfs/lhcbdev.cern.ch
    """
    directory = 'lib/lhcb/LHCBDIRAC/lhcbdirac'
    try:
      invokeCmd('source /cvmfs/lhcb.cern.ch/%s %s && env > environmentSourceLHCbDirac' % (directory,
                                                                                          self.pp.releaseVersion))
    except OSError:
      invokeCmd('source /cvmfs/lhcbdev.cern.ch/%s %s && env > environmentSourceLHCbDirac' % (directory,
                                                                                             self.pp.releaseVersion))
    return parseEnvironmentFile('environmentSourceLHCbDirac')

  def _do_lb_run(self):
    """ do lb-run -c best LHCbDIRAC of the requested version. If the version does not exist, raise OSError
    """
    invokeCmd('lb-run -c best LHCbDirac/%s > environmentLbRunDirac' % self.pp.releaseVersion,
              self.pp.installEnv)
    return parseEnvironmentFile('environmentLbRunDirac')


class LHCbConfigureBasics(LHCbCommandBase, ConfigureBasics):
  """ Only case here, for now, is if to set or not the CAs and VOMS location, that should be found in CVMFS
  """

  def _getBasicsCFG(self):
    """ Fill in the sharedArea
    """
    super(LHCbConfigureBasics, self)._getBasicsCFG()

    # Adding SharedArea (which should be in CVMFS)
    if 'VO_LHCB_SW_DIR' in os.environ:
      sharedArea = os.path.join(os.environ['VO_LHCB_SW_DIR'], 'lib')
      self.log.debug("Using VO_LHCB_SW_DIR at '%s'" % sharedArea)
      if os.environ['VO_LHCB_SW_DIR'] == '.':
        if not os.path.isdir('lib'):
          os.mkdir('lib')
    else:
      sharedArea = '/cvmfs/lhcb.cern.ch/lib'
      self.log.warn("Can't find shared area, forcing it to %s" % sharedArea)

    self.cfg.append('-o /LocalSite/SharedArea=%s' % sharedArea)

  def _getSecurityCFG(self):
    """ Locate X509_CERT_DIR
    """

    self.log.debug("self.pp.installEnv: %s" % str(self.pp.installEnv))

    if 'X509_CERT_DIR' in os.environ:
      self.log.debug("X509_CERT_DIR is set in the host environment as %s,\
                      aligning installEnv to it" % os.environ['X509_CERT_DIR'])
      self.pp.installEnv['X509_CERT_DIR'] = os.environ['X509_CERT_DIR']
    else:
      self.log.debug("X509_CERT_DIR is not set in the host environment")
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/certificates',
                    '$VO_LHCB_SW_DIR/etc/grid-security/certificates']
      for candidate in candidates:
        self.log.debug("Candidate directory for X509_CERT_DIR is %s" % candidate)
        if os.path.isdir(os.path.expandvars(candidate)):
          self.log.debug("Setting X509_CERT_DIR=%s" % candidate)
          self.pp.installEnv['X509_CERT_DIR'] = candidate
          os.environ['X509_CERT_DIR'] = candidate
          break
        else:
          self.log.debug("%s not found or not a directory" % candidate)

    if 'X509_CERT_DIR' not in self.pp.installEnv:
      self.log.error("Could not find/set X509_CERT_DIR")
      sys.exit(1)

    if 'X509_VOMS_DIR' in os.environ:
      self.log.debug("X509_VOMS_DIR is set in the host environment as %s, \
                      aligning installEnv to it" % os.environ['X509_VOMS_DIR'])
      self.pp.installEnv['X509_VOMS_DIR'] = os.environ['X509_VOMS_DIR']
    else:
      self.log.debug("X509_VOMS_DIR is not set in the host environment")
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir',
                    '$VO_LHCB_SW_DIR/etc/grid-security/vomsdir']
      for candidate in candidates:
        self.log.debug("Candidate directory for X509_VOMS_DIR is %s" % candidate)
        if os.path.isdir(os.path.expandvars(candidate)):
          self.log.debug("Setting X509_VOMS_DIR=%s" % candidate)
          self.pp.installEnv['X509_VOMS_DIR'] = candidate
          os.environ['X509_VOMS_DIR'] = candidate
          break
        else:
          self.log.debug("%s not found" % candidate)

    if 'X509_VOMS_DIR' not in self.pp.installEnv:
      self.log.error("Could not find/set X509_VOMS_DIR")
      sys.exit(1)

    if 'DIRAC_VOMSES' in os.environ:
      self.log.debug("DIRAC_VOMSES is set in the host environment as %s, \
                      aligning installEnv to it" % os.environ['DIRAC_VOMSES'])
      self.pp.installEnv['DIRAC_VOMSES'] = os.environ['DIRAC_VOMSES']
    else:
      self.log.debug("DIRAC_VOMSES is not set in the host environment")
      # try and find it
      candidates = ['/cvmfs/lhcb.cern.ch/etc/grid-security/vomses', '$VO_LHCB_SW_DIR/etc/grid-security/vomses']
      for candidate in candidates:
        self.log.debug("Candidate directory for DIRAC_VOMSES is %s" % candidate)
        if os.path.isdir(os.path.expandvars(candidate)):
          self.log.debug("Setting DIRAC_VOMSES=%s" % candidate)
          self.pp.installEnv['DIRAC_VOMSES'] = candidate
          os.environ['DIRAC_VOMSES'] = candidate
          break
        else:
          self.log.debug("%s not found" % candidate)

    if 'DIRAC_VOMSES' not in self.pp.installEnv:
      self.log.error("Could not find/set DIRAC_VOMSES")
      sys.exit(1)

    self.log.debug('X509_CERT_DIR = %s, %s' % (self.pp.installEnv['X509_CERT_DIR'], os.environ['X509_CERT_DIR']))
    self.log.debug('X509_VOMS_DIR = %s, %s' % (self.pp.installEnv['X509_VOMS_DIR'], os.environ['X509_VOMS_DIR']))
    self.log.debug('DIRAC_VOMSES = %s, %s' % (self.pp.installEnv['DIRAC_VOMSES'], os.environ['DIRAC_VOMSES']))

    # re-saving also in environmentLHCbDirac file for completeness
    # since we may have added/changed the security variables above
    # The content of environmentLHCbDirac will be the same
    # as the content of environmentLbRunDirac if lb-run LHCbDIRAC is successful
    # plus the security-related variables of above
    saveEnvInFile(self.pp.installEnv, 'environmentLHCbDirac')

    # In any case do not download VOMS and CAs
    self.cfg.append('-DMH')

    super(LHCbConfigureBasics, self)._getSecurityCFG()


class LHCbCleanPilotEnv(LHCbConfigureBasics):
  """Delete the pilot.cfg and the pilot.json, needed for VMs.
     Force the use of the CS given by command line. The command avoids the use of the CS server address (lhcb-conf2)
     which would not work for some resources, e.g. BOINC.
  """

  def _getBasicsCFG(self):
    super(LHCbCleanPilotEnv, self)._getBasicsCFG()
    if os.path.exists(self.pp.localConfigFile):
      os.remove(self.pp.localConfigFile)
    localPilotCFGFile = self.pp.pilotCFGFile + "-local"
    if os.path.exists(localPilotCFGFile):
      os.remove(localPilotCFGFile)
    self.cfg.append(" -o /DIRAC/Configuration/Servers=%s" % self.pp.configServer)


class LHCbConfigureCPURequirements(LHCbCommandBase, ConfigureCPURequirements):
  pass


class LHCbConfigureSite(LHCbCommandBase, ConfigureSite):
  pass


class LHCbConfigureArchitecture(LHCbCommandBase, ConfigureArchitecture):
  """ just sets the CMTCONFIG variable
  """

  def execute(self):
    """ calls the superclass execute and then sets the CMTCONFIG variable with the host binary tag
    """
    # This sets the DIRAC architecture
    super(LHCbConfigureArchitecture, self).execute()

    # Now we find the host binary tag
    cfg = ['--BinaryTag']
    if self.pp.useServerCertificate:
      cfg.append('-o  /DIRAC/Security/UseServerCertificate=yes')
    if self.pp.localConfigFile:
      cfg.append(self.pp.localConfigFile)  # this file is as input

    binaryTagCmd = "%s %s" % (self.pp.architectureScript, " ".join(cfg))

    retCode, binaryTag = self.executeAndGetOutput(binaryTagCmd, self.pp.installEnv)
    if retCode:
      self.log.error("There was an error getting the binary tag [ERROR %d]" % retCode)
      self.exitWithError(retCode)
    self.log.info("BinaryTag determined: %s" % binaryTag)

    self.log.info('Setting variable CMTCONFIG=%s' % binaryTag)
    os.environ['CMTCONFIG'] = binaryTag
