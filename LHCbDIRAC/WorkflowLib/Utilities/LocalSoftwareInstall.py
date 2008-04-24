########################################################################
# $Id: LocalSoftwareInstall.py,v 1.1 2008/04/24 09:11:33 rgracian Exp $
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

__RCSID__ = "$Id: LocalSoftwareInstall.py,v 1.1 2008/04/24 09:11:33 rgracian Exp $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR, gLogger

import os,sys,re

class LocalSoftwareInstall:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.installProject = 'install_project.py'
    self.log = gLogger

  #############################################################################
  def execute(self):
    """When this module is used by DIRAC components, this method is called.
       Currently this only creates a link to the VO_LHCB_SW_DIR/lib directory
       if available.
    """
    gLogge.logINFO( str(argumentsDict) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#