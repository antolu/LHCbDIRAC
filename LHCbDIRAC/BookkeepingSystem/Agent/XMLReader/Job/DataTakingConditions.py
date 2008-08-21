########################################################################
# $Id: DataTakingConditions.py,v 1.1 2008/08/21 14:11:19 zmathe Exp $
########################################################################

"""

"""

from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: DataTakingConditions.py,v 1.1 2008/08/21 14:11:19 zmathe Exp $"


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