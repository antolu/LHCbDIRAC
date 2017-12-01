""" Extension of DIRAC SiteDirector. Simply defines what to send.
"""

import os
import base64
import bz2
import tempfile

from DIRAC import S_OK, S_ERROR, rootPath
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as DIRACSiteDirector

__RCSID__ = "$Id$"

DIRAC_MODULES = [ os.path.join( rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotCommands.py' ),
                  os.path.join( rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotTools.py' ),
                  os.path.join( rootPath, 'LHCbDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'LHCbPilotCommands.py' ) ]


class SiteDirector( DIRACSiteDirector ):
  """ Simple extension of the DIRAC site director to send LHCb specific pilots (with a custom list of commands)
  """

  def beginExecution(self):
    """ just simple redefinition
    """
    res = DIRACSiteDirector.beginExecution( self )
    if not res['OK']:
      return res

    self.extraModules = self.am_getOption( 'ExtraPilotModules', [] ) + DIRAC_MODULES
    self.devLbLogin = self.am_getOption( 'devLbLogin', False )
    self.lbRunOnly = self.am_getOption( 'lbRunOnly', False )

    return S_OK()

  # FIXME: fully here ONLY because of removing the setting of X509_CERT_DIR, maybe better extensibility?
  def _writePilotScript( self, workingDirectory, pilotOptions, proxy = None,
                         httpProxy = '', pilotExecDir = '' ):
    """ Bundle together and write out the pilot executable script, admix the proxy if given
    """

    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy is not None:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) )
        proxyFlag = 'True'
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) )
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) )
      compressedAndEncodedExtra = {}
      for module in self.extraModules:
        moduleName = os.path.basename( module )
        compressedAndEncodedExtra[moduleName] = base64.encodestring( bz2.compress( open( module, "rb" ).read(), 9 ) )
    except:
      self.log.exception( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )
      return S_ERROR( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )

    # Extra modules
    mStringList = []
    for moduleName in compressedAndEncodedExtra:
      mString = """open( '%s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%s\"\"\" ) ) )""" % \
                ( moduleName, compressedAndEncodedExtra[moduleName] )
      mStringList.append( mString )
    extraModuleString = '\n  '.join( mStringList )

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os
import stat
import tempfile
import sys
import shutil
import base64
import bz2
import logging
import time

formatter = logging.Formatter(fmt='%%(asctime)s UTC %%(levelname)-8s %%(message)s', datefmt='%%Y-%%m-%%d %%H:%%M:%%S')
logging.Formatter.converter = time.gmtime
try:
  screen_handler = logging.StreamHandler(stream=sys.stdout)
except TypeError: #python2.6
  screen_handler = logging.StreamHandler(strm=sys.stdout)
screen_handler.setFormatter(formatter)
logger = logging.getLogger('pippoLogger')
logger.setLevel(logging.DEBUG)
logger.addHandler(screen_handler)

try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = os.getcwd()
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = pilotExecDir )
  pilotWorkingDirectory = os.path.realpath( pilotWorkingDirectory )
  os.chdir( pilotWorkingDirectory )
  if %(proxyFlag)s:
    open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedProxy)s\"\"\" ) ) )
    os.chmod("proxy", stat.S_IRUSR | stat.S_IWUSR)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedPilot)s\"\"\" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedInstall)s\"\"\" ) ) )
  os.chmod("%(pilotScript)s", stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR )
  os.chmod("%(installScript)s", stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR )
  %(extraModuleString)s
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  # TODO: structure the output
  print '==========================================================='
  logger.debug('Environment of execution host\\n')
  for key, val in os.environ.iteritems():
    print key + '=' + val
  print '===========================================================\\n'
except Exception as x:
  print >> sys.stderr, x
  shutil.rmtree( pilotWorkingDirectory )
  sys.exit(-1)
cmd = "python %(pilotScript)s %(pilotOptions)s"
logger.info('Executing: %%s' %% cmd)
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( pilotWorkingDirectory )

EOF
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'extraModuleString': extraModuleString,
        'pilotExecDir': pilotExecDir,
        'pilotScript': os.path.basename( self.pilot ),
        'installScript': os.path.basename( self.install ),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir = workingDirectory )
    pilotWrapper = os.fdopen( fd, 'w' )
    pilotWrapper.write( localPilot )
    pilotWrapper.close()
    return name


  def _getPilotOptions( self, queue, pilotsToSubmit ):
    """ Adding LHCb specific options
    """
    pilotOptions, newPilotsToSubmit = DIRACSiteDirector._getPilotOptions( self, queue, pilotsToSubmit )

    lhcbPilotCommands = ['LHCbGetPilotVersion',
                         'CheckWorkerNode',
                         'LHCbInstallDIRAC',
                         'LHCbConfigureBasics',
                         'CheckCECapabilities',
                         'CheckWNCapabilities',
                         'LHCbConfigureSite',
                         'LHCbConfigureArchitecture',
                         'LHCbConfigureCPURequirements',
                         'LaunchAgent']

    pilotOptions.append( '-E LHCbPilot' )
    pilotOptions.append( '-X %s' % ','.join(lhcbPilotCommands) )
    if self.devLbLogin or self.lbRunOnly:
      opt = ''
      if self.devLbLogin:
        opt = 'devLbLogin'
      if self.lbRunOnly:
        opt = '.'.join([opt, 'lbRunOnly'])
      pilotOptions.append( '-o %s' % opt )

    return [pilotOptions, newPilotsToSubmit]
