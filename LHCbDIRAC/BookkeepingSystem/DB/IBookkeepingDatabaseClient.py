# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                     import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class IBookkeepingDatabaseClient(object):
    
  #############################################################################
  def __init__(self, DatabaseManager = IBookkeepingDB()):
    self.databaseManager_ = DatabaseManager
    
  #############################################################################
  def getManager(self):
    return self.databaseManager_
  
  #############################################################################
  def getAvailableSteps(self, dict={}):
    return self.getManager().getAvailableSteps(dict)
  
  #############################################################################
  def getAvailableFileTypes(self):
    return self.getManager().getAvailableFileTypes()
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    return self.getManager().insertFileTypes(ftype, desc)
  
  #############################################################################
  def insertStep(self, dict):
    return self.getManager().insertStep(dict)
  
  #############################################################################
  def deleteStep(self, stepid):
    return self.getManager().deleteStep(stepid)
  
  #############################################################################
  def updateStep(self, dict):
    return self.getManager().updateStep(dict)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    return self.getManager().getStepInputFiles(StepId)
  
  #############################################################################
  def setStepInputFiles(self, stepid, files):
    return self.getManager().setStepInputFiles(stepid, files)
  
  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    return self.getManager().setStepOutputFiles(stepid, files)
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    return self.getManager().getStepOutputFiles(StepId)
  
  #############################################################################
  def getAvailableConfigNames(self):
    return self.getManager().getAvailableConfigNames()
  
  #############################################################################
  def getConfigVersions(self, configname):
    return self.getManager().getConfigVersions(configname)
  #############################################################################
  def getConditions(self, configName, configVersion):
    return self.getManager().getConditions(configName, configVersion)
  
  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, path):
    return self.getManager().getProcessingPass(configName, configVersion, conddescription, path)