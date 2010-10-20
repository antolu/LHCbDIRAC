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
  def getStepOutputFiles(self, StepId):
    return self.getManager().getStepOutputFiles(StepId)