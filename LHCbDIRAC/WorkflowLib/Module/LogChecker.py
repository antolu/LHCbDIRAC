########################################################################
# $Id: LogChecker.py,v 1.1 2008/02/01 08:10:19 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: LogChecker.py,v 1.1 2008/02/01 08:10:19 joel Exp $"

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *
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

  def execute(self):

    self.site = gConfig.getValue('/LocalSite/Site','Site')
#    print self.JOBID
    print self.appName,  self.logfile

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
    self.logChecker.job_id = self.job_id
    self.logChecker.prod_id = self.prod_id
    self.logChecker.inputData = self.inputData
    self.logChecker.EMAIL = self.EMAIL
    self.logChecker.execute()
    self.logChecker.checkApplicationLog("my error")
