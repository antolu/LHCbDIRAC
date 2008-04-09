########################################################################
# $Id: LogChecker.py,v 1.7 2008/04/09 16:13:26 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: LogChecker.py,v 1.7 2008/04/09 16:13:26 joel Exp $"

from WorkflowLib.Module.BooleCheckLogFile import *
from WorkflowLib.Module.BrunelCheckLogFile import *
from WorkflowLib.Module.AnalyseLogFile import *
#from WorkflowLib.Module.GaussCheckLogFile import *
from WorkflowLib.Module.DaVinciCheckLogFile import *
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Utilities.Tools                         import *

import commands, os

class LogChecker(object):

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

    if self.appName == 'Boole':
        self.logChecker = BooleCheckLogFile()
    elif  self.appName == 'Brunel':
#        self.logChecker = BrunelCheckLogFile()
        self.logChecker = AnalyseLogFile()
    elif  self.appName == 'Gauss':
        self.logChecker = GaussCheckLogFile()
    elif  self.appName == 'DaVinci':
#        self.logChecker = DaVinciCheckLogFile()
        self.logChecker = AnalyseLogFile()
    else:
        self.log.error('appName is not defined or known %s' %(self.appName))
        return S_ERROR('appName is not defined or known %s' %(self.appName))

    #self.logChecker.logfile = self.appLog
    #self.logChecker.appName = self.appName
    #self.logChecker.appVersion = self.appVersion
    #self.logChecker.job_id = self.JOB_ID
    #self.logChecker.prod_id = self.PRODUCTION_ID
    #self.logChecker.inputData = self.inputData
    #self.logChecker.EMAIL = self.EMAIL
    #self.logChecker.OUTPUT_MAX = self.OUTPUT_MAX

    # Copy all attributes from container class into embedded class replacing
    # inheritance mechanism which can not be used in this case
    copyClassAttributes(self, self.logChecker, 'logChecker')

    rc = self.logChecker.execute()
    self.NUMBER_OF_EVENTS_INPUT = self.logChecker.NUMBER_OF_EVENTS_INPUT
    self.NUMBER_OF_EVENTS_OUTPUT = self.logChecker.NUMBER_OF_EVENTS_OUTPUT
    if not rc['OK']:
      self.logChecker.checkApplicationLog(rc['Message'])

