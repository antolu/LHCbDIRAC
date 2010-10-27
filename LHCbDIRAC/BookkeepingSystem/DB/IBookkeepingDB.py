########################################################################
# $Id$
########################################################################

"""

"""

__RCSID__ = "$Id$"

from DIRAC                                      import gLogger, S_OK, S_ERROR

class IBookkeepingDB(object):
  
  #############################################################################
  def __init__(self):
    pass
  
  #############################################################################
  def getAvailableSteps(self, dict):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getAvailableFileTypes(self):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def insertStep(self, dict):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
  	gLogger.error('This method is not implemented!')
  def deleteStep(self, stepid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def updateStep(self, dict):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getAvailableConfigNames(self):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getConfigVersions(self, configname):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, path):
    gLogger.error('This method is not implemented!')