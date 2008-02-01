########################################################################
# $Id: LogChecker.py,v 1.3 2008/02/01 15:00:10 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: LogChecker.py,v 1.3 2008/02/01 15:00:10 joel Exp $"

from WorkflowLib.Module.BooleCheckLogFile import *
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import commands, os

class LogChecker(object):

  def __init__(self):
    self.result = S_ERROR()
    self.logfile = 'None'
    self.log = gLogger.getSubLogger("CheckLogFile")
    self.logChecker = None
    self.appName = None
    self.OUTPUT_MAX = 'None'

  def execute(self):

    if self.appName == 'Boole':
        self.logChecker = BooleCheckLogFile()
    elif  self.appName == 'Brunel':
        self.logChecker = BrunelCheckLogFile()
    elif  self.appName == 'Gauss':
        self.logChecker = GaussCheckLogFile()
    elif  self.appName == 'DaVinci':
        self.logChecker = DaVinciCheckLogFile()
    else:
        self.log.error('appName is not defined or known %s' %(self.appName))
        return S_ERROR('appName is not defined or known %s' %(self.appName))

    self.logChecker.logfile = self.logfile
    self.logChecker.appName = self.appName
    self.logChecker.appVersion = self.appVersion
    self.logChecker.job_id = self.JOB_ID
    self.logChecker.prod_id = self.PRODUCTION_ID
    self.logChecker.inputData = self.inputData
    self.logChecker.EMAIL = self.EMAIL
    self.logChecker.OUTPUT_MAX = self.OUTPUT_MAX
    rc = self.logChecker.execute()
    if not rc['OK']:
      self.logChecker.checkApplicationLog(rc['Message'])

