########################################################################
# $Id: SoftwareDistribution.py,v 1.2 2007/12/17 16:27:38 paterson Exp $
# File :   SoftwareDistribution.py
# Author : Stuart Paterson
########################################################################

""" The LHCb Software Distribution class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory.  This relies on
    two JDL parameters in LHCb workflows:
    - SoftwareDistModule - expresses the import string
    - SoftwarePackages - for the necessary parameters to install software
    and DIRAC assumes an execute() method will exist during usage.

    In its initial incarnation, this simply checks for the VO_LHCB_SW_DIR
    environment variable and creates a link.  Eventually this will check the
    availability of packages in the shared area and trigger local installation
    as required.
"""

__RCSID__ = "$Id: SoftwareDistribution.py,v 1.2 2007/12/17 16:27:38 paterson Exp $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR, gLogger

import os,sys,re

class SoftwareDistribution:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.log = gLogger

  #############################################################################
  def __createLink(self,source,target):
    """Simple function to create a local link.  Unix dependent but could use
       a shutil.copy for other platforms.
    """
    cmd = 'ln -s %s %s' %(source,target)
    self.log.debug(cmd)
    outputDict = shellCall(0,cmd)
    return outputDict

  #############################################################################
  def execute(self):
    """When this module is used by DIRAC components, this method is called.
       Currently this only creates a link to the VO_LHCB_SW_DIR/lib directory
       if available.
    """
    params = self.arguments
    #First an example to show the use of the arguments passed from the JobAgent
    #software packages are also available here e.g. ['DaVinci.v19r5']
    if params.has_key('CE'):
      local = params['CE']
      if local.has_key('SharedArea'):
        sharedArea = local['SharedArea']
        self.log.debug('Found LocalSite/SharedArea %s' %(sharedArea))
      else:
        self.log.debug('Site shared area not specified in local configuration')
    else:
      self.log.debug('CE Arguments not available')

    if os.environ.has_key('VO_LHCB_SW_DIR'):
      voDir = os.environ['VO_LHCB_SW_DIR']
      self.log.verbose('Found VO_LHCB_SW_DIR = %s' %(voDir))
      sharedArea = voDir+'/lib'
      if os.path.exists(sharedArea):
        self.log.verbose('Found $VO_LHCB_SW_DIR/lib = %s' %(sharedArea))
        root = os.getcwd()
        self.__createLink(sharedArea,root+'/lib')
        return S_OK('Created link to VO_LHCB_SW_DIR')
      else:
        return S_ERROR('Path to $VO_LHCB_SW_DIR/lib = %s does not exist' %(sharedArea))
    else:
      return S_ERROR('No VO_LHCB_SW_DIR defined')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#