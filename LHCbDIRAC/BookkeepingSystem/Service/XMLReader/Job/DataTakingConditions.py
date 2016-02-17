########################################################################
# $Id: DataTakingConditions.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores the data taking conditions

"""

from DIRAC                                                                  import gLogger

__RCSID__ = "$Id$"


class DataTakingConditions:
  """
  DataTakingConditions class
  """
  #############################################################################
  def __init__(self):
    """initialize the class memeber"""
    self.parameters_ = {}

  #############################################################################
  def addParam(self, name, value):
    """adds parameter"""
    self.parameters_[name] = value

  #############################################################################
  def getParams(self):
    """returns the parameters (data taking conditions)"""
    return self.parameters_

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    gLogger.info("Write DataTaking conditions to XML!!")
    result = '<DataTakingConditions>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="' + name + '"   Value="' + value + '"/>\n'
    result += '</DataTakingConditions>\n'

    return result

