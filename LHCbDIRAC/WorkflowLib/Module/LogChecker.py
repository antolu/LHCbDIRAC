########################################################################
# $Id: LogChecker.py,v 1.14 2008/06/03 15:16:04 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: LogChecker.py,v 1.14 2008/06/03 15:16:04 joel Exp $"

from WorkflowLib.Module.AnalyseLogFile import *
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Module.ModuleBase                       import *
from WorkflowLib.Utilities.Tools                         import *

import commands, os

class LogChecker(ModuleBase):

  def __init__(self):
    self.result = S_ERROR()
    self.logfile = 'None'
    self.log = gLogger.getSubLogger("CheckLogFile")
    self.logChecker = None
    self.appName = None
    self.NUMBER_OF_EVENTS = None
    self.NUMBER_OF_EVENTS_INPUT = None
    self.NUMBER_OF_EVENTS_OUTPUT = None
    self.OUTPUT_MAX = 'None'

  def execute(self):
    if self.appName in ('Boole', 'Gauss','Brunel', 'DaVinci'):
        self.logChecker = AnalyseLogFile()
    else:
        self.log.error('appName is not defined or known %s' %(self.appName))
        return S_ERROR('appName is not defined or known %s' %(self.appName))

    # Copy all attributes from container class into embedded class replacing
    # inheritance mechanism which can not be used in this case
    copyClassAttributes(self, self.logChecker, 'logChecker')

    rc = self.logChecker.execute()
    self.NUMBER_OF_EVENTS_INPUT = self.logChecker.NUMBER_OF_EVENTS_INPUT
    self.NUMBER_OF_EVENTS_OUTPUT = self.logChecker.NUMBER_OF_EVENTS_OUTPUT
    if not rc['OK']:
      self.logChecker.checkApplicationLog(rc['Message'])
      return rc

    return rc

