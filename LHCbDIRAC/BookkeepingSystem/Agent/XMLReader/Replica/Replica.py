########################################################################
# $Id: Replica.py,v 1.4 2008/07/01 10:54:26 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica import ReplicaParam
from DIRAC                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: Replica.py,v 1.4 2008/07/01 10:54:26 zmathe Exp $"

class Replica:


  #############################################################################
  def __init__(self):
      self.params_ = []
      self.fileName_ = ""

  #############################################################################
  def addParam(self, param):
      self.params_ += [param]

  #############################################################################
  def getaprams(self):
    return self.params_

  #############################################################################
  def getFileName(self):
    return self.fileName_

  #############################################################################
  def setFileName(self, name):
    self.fileName_ = name

  #############################################################################
  def __repr__(self):
    result = "\nReplica: "
    result += self.fileName_ + "\n"
    for param in self.params_:
        result += str(param)

    return result
  
  #############################################################################
  def writeToXML(self):
    gLogger.info("Replica XML writing!!!")
    for param in self.getaprams():
      param.writeToXML(False)
  #############################################################################
