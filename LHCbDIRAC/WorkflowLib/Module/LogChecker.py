########################################################################
# $Id: LogChecker.py,v 1.15 2008/06/17 09:43:36 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: LogChecker.py,v 1.15 2008/06/17 09:43:36 joel Exp $"

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
    self.sourceData = None
    self.applicationName = None
    self.applicationLog = None
    self.numberOfEvents = None
    self.numberOfEventsInput = None
    self.numberOfEventsOutput = None
    self.OUTPUT_MAX = 'None'

  def execute(self):
    if self.workflow_commons.has_key('sourceData'):
        self.sourceData = self.workflow_commons['sourceData']

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('numberOfEvents'):
       self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key('numberOfEventsInput'):
       self.numberOfEventsInput = self.step_commons['numberOfEventsInput']

    if self.step_commons.has_key('numberOfEventsOutput'):
       self.numberOfEventsOutput = self.step_commons['numberOfEventsOutput']

    if self.applicationName in ('Boole', 'Gauss','Brunel', 'DaVinci'):
        self.logChecker = AnalyseLogFile()
    else:
        self.log.error('applicationName is not defined or known %s' %(self.applicationName))
        return S_ERROR('applicationName is not defined or known %s' %(self.applicationName))

    # Copy all attributes from container class into embedded class replacing
    # inheritance mechanism which can not be used in this case
    copyClassAttributes(self, self.logChecker, 'logChecker')

    rc = self.logChecker.execute()
    self.step_commons['numberOfEventsInput'] = self.logChecker.numberOfEventsInput
    self.step_commons['numberOfEventsOutput'] = self.logChecker.numberOfEventsOutput
    if not rc['OK']:
      self.logChecker.checkApplicationLog(rc['Message'])
      return rc

    return rc

