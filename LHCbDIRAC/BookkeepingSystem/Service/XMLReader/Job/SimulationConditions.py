########################################################################
# $Id$
########################################################################

"""

"""

from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"


class SimulationConditions:
  
  #############################################################################
  def __init__(self):
    self.parameters_ = {}
  
  #############################################################################
  def addParam(self, name, value):
    self.parameters_[name]= value
  
  #############################################################################
  def getParams(self):
    return self.parameters_
  
  #############################################################################
  def writeToXML(self):
    gLogger.info("Write Simulation conditions to XML!!")
    result = '<SimulationCondition>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="'+ name +'"   Value="'+value+'"/>\n'
    result += '</SimulationCondition>\n'
    
    return result
  