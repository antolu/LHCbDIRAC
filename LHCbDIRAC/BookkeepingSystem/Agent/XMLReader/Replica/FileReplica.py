#######################################################################
# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica    import Replica
from DIRAC                                                      import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class FileReplica(Replica):
  
  def writeToXML(self):
    gLogger.info("Job Replica XML writing!!!")
    result = ''
    for param in self.getaprams():
      result += param.writeToXML()
    
    return result
    