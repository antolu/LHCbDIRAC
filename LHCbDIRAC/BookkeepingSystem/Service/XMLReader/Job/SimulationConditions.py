"""
stores the simulation condition
"""
########################################################################
# $Id: SimulationConditions.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################


from DIRAC                                                                  import gLogger

__RCSID__ = "$Id: SimulationConditions.py 54098 2012-07-02 16:43:53Z zmathe $"


class SimulationConditions:
  """
  SimulationConditions class
  """
  #############################################################################
  def __init__(self):
    """initialize the class member"""
    self.parameters_ = {}

  #############################################################################
  def addParam(self, name, value):
    """adds a parameter into the dictionary"""
    self.parameters_[name] = value

  #############################################################################
  def getParams(self):
    """returns the parameters"""
    return self.parameters_

  #############################################################################
  def writeToXML(self):
    """creates the xml string"""
    gLogger.info("Write Simulation conditions to XML!!")
    result = '<SimulationCondition>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="' + name + '"   Value="' + value + '"/>\n'
    result += '</SimulationCondition>\n'

    return result

