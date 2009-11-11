########################################################################
# $Id$
########################################################################
""" Script Base Class """

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog

from LHCbDIRAC.Workflow.Modules.AnalyseLogFile                    import AnalyseLogFile
from LHCbDIRAC.Workflow.Modules.ModuleBase                        import ModuleBase

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import commands, os

class LogChecker(ModuleBase):

  def __init__(self):
    self.result = S_ERROR()
    self.logfile = ''
    self.log = gLogger.getSubLogger("LogChecker")
    self.logChecker = None
    self.sourceData = ''
    self.applicationName = ''
    self.applicationLog = ''
    self.numberOfEvents = 0
    self.numberOfEventsInput = 0
    self.numberOfEventsOutput = 0
    self.OUTPUT_MAX = ''

  def resolveInputVariables(self):
    if self.workflow_commons.has_key('InputData'):
       self.InputData = self.workflow_commons['InputData']

    if self.workflow_commons.has_key('sourceData'):
        self.sourceData = self.workflow_commons['sourceData']

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']

    if self.workflow_commons.has_key('poolXMLCatName'):
       self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    if self.step_commons.has_key('inputDataType'):
       self.inputDataType = self.step_commons['inputDataType']

    if self.step_commons.has_key('numberOfEvents'):
       self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key('numberOfEventsInput'):
       self.numberOfEventsInput = self.step_commons['numberOfEventsInput']

    if self.step_commons.has_key('numberOfEventsOutput'):
       self.numberOfEventsOutput = self.step_commons['numberOfEventsOutput']

  def copyClassAttributes(self,from_, to_, except_=[]):
    """copy class attributes from one class to another"""
    for attr in from_.__dict__.keys():
      if attr not in except_:
        to_.__dict__[attr]=from_.__dict__[attr]

  def execute(self):
    self.resolveInputVariables()
    if self.applicationName in ('Boole', 'Gauss','Brunel', 'DaVinci','LHCb','Moore'):
        self.logChecker = AnalyseLogFile()
    else:
        self.log.error('applicationName is not defined or known %s' %(self.applicationName))
        return S_ERROR('applicationName is not defined or known %s' %(self.applicationName))

    # Copy all attributes from container class into embedded class replacing
    # inheritance mechanism which can not be used in this case
    self.copyClassAttributes(self.logChecker, 'logChecker')

    rc = self.logChecker.execute()
    self.step_commons['numberOfEventsInput'] = self.logChecker.numberOfEventsInput
    self.step_commons['numberOfEventsOutput'] = self.logChecker.numberOfEventsOutput
    if not rc['OK']:
      self.logChecker.checkApplicationLog(rc['Message'])
      return rc

    return rc

