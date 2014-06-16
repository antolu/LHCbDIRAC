""" Extension of DIRAC SiteDirector
"""

import os, base64, bz2, tempfile

from DIRAC                                import S_ERROR
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as DIRACSiteDirector

class SiteDirector( DIRACSiteDirector ):
  """ just extension
  """

  #####################################################################################
  def _writePilotScript( self, workingDirectory, pilotOptions, proxy = None, httpProxy = '', pilotExecDir = '' ):
    """ Bundle together and write out the pilot executable script, admix the proxy if given
    """
    # use the LHCbPilot, if it exists
    try:
      compressedAndEncodedLHCbPilot = ''
      lhcbFlag = 'False'
      import LHCbDIRAC.WorkloadManagementSystem.PilotAgent.LHCbPilot
      lhcbPilot = os.path.join( LHCbDIRAC.LHCbrootPath, 'LHCbDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'LHCbPilot.py' )
      compressedAndEncodedLHCbPilot = base64.encodestring( bz2.compress( open( lhcbPilot, "rb" ).read(), 9 ) )
      lhcbFlag = 'True'
    except ImportError:
      pass

    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy is not None:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) )
        proxyFlag = 'True'
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) )
      print compressedAndEncodedPilot
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) )
    except:
      self.log.exception( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )
      return S_ERROR( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = None
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = pilotExecDir )
  pilotWorkingDirectory = os.path.realpath( pilotWorkingDirectory )
  os.chdir( pilotWorkingDirectory )
  if %(proxyFlag)s:
    open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedProxy)s\"\"\" ) ) )
    os.chmod("proxy",0600)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  if %(lhcbFlag)s:
    open( '%(LHCbpilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedLHCbPilot)s\"\"\" ) ) )
    os.chmod("%(LHCbpilotScript)s",0700)
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedPilot)s\"\"\" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedInstall)s\"\"\" ) ) )
  os.chmod("%(pilotScript)s",0700)
  os.chmod("%(installScript)s",0700)
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  if "%(httpProxy)s":
    os.environ["HTTP_PROXY"]="%(httpProxy)s"
  os.environ["X509_CERT_DIR"]=os.path.join(pilotWorkingDirectory, 'etc/grid-security/certificates')
  # TODO: structure the output
  print '==========================================================='
  print 'Environment of execution host'
  for key in os.environ.keys():
    print key + '=' + os.environ[key]
  print '==========================================================='
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
  if %(lhcbFlag)s:
  cmd = "python %(LHCbpilotScript)s %(pilotOptions)s"
else
  cmd = "python %(pilotScript)s %(pilotOptions)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )
shutil.rmtree( pilotWorkingDirectory )

EOF
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedLHCbPilot': compressedAndEncodedLHCbPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'httpProxy': httpProxy,
        'pilotExecDir': pilotExecDir,
        'pilotScript': os.path.basename( self.pilot ),
        'installScript': os.path.basename( self.install ),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag,
        'lhcbFlag': lhcbFlag}

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir = workingDirectory )
    pilotWrapper = os.fdopen( fd, 'w' )
    pilotWrapper.write( localPilot )
    pilotWrapper.close()
    return name
