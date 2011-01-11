########################################################################
# $Id$
# File :   LocalSoftwareInstall.py
# Author : Ricardo Graciani
########################################################################

""" The LHCb Local Software Insrtall class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory.  This relies on
    two JDL parameters in LHCb workflows:
    - SoftwareDistModule - expresses the import string
    - SoftwarePackages - for the necessary parameters to install software
    and DIRAC assumes an execute() method will exist during usage.

    In its initial incarnation, this simply uses install_proyect.py to install
    in the local area of the job
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                     import systemCall
from DIRAC                                               import S_OK, S_ERROR, gLogger

import os,sys,shutil

class LocalSoftwareInstall:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    os.environ['CMTCONFIG'] = argumentsDict['Job']['SystemConfig']

    apps = argumentsDict['Job']['SoftwarePackages']
    if type(apps) == type(' '):
      apps = [apps]

    self.installProject = 'install_project.py'
    self.apps = []
    for app in apps:
      gLogger.verbose( 'Requested Package %s' % app )
      app = tuple(app.split('.'))
      self.apps.append(app)

  #############################################################################
  def execute(self):
    """When this module is used by DIRAC components, this method is called.
       Currently this only creates a link to the VO_LHCB_SW_DIR/lib directory
       if available.
    """

    initialDir = os.getcwd()

    os.environ['MYSITEROOT'] = os.path.join(initialDir,'lib')
    if not os.path.exists('lib'):
      os.mkdir('lib')
    elif not os.path.isdir('lib'):
      return S_ERROR( 'Existing lib file' )
    shutil.copy('install_project.py','lib')
    os.chdir('lib')
    for app in self.apps:
      cmd = '%s install_project -b %s %s' % (( sys.executable,)+app)
      ret = systemCall( 3600, cmd.split(), callbackFunction=log )
      if not ret['OK']:
        break
      if ret['Value'][0]:
        ret = S_ERROR( 'Command exits with error %s: "%s"' % ( ret['Value'][0], cmd ) )
        break
      ret = S_OK()
    os.chdir(initialDir)
    shutil.copy( os.path.join('lib','LHCb_config.py'),'LHCb_config.py')
#    from LHCb_config import *
    return ret

def log( n, line ):
  gLogger.info( line )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#