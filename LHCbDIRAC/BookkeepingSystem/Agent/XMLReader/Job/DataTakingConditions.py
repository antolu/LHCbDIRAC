########################################################################
# $Id: DataTakingConditions.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

"""

"""

from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: DataTakingConditions.py 18161 2009-11-11 12:07:09Z acasajus $"


class DataTakingConditions:
  
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
    gLogger.info("Write DataTaking conditions to XML!!")
    result = '<DataTakingConditions>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="'+ name +'"   Value="'+value+'"/>\n'
    result += '</DataTakingConditions>\n'
    
    return result