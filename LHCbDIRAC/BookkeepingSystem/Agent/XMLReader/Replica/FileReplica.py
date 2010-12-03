#######################################################################
# $Id: FileReplica.py 20219 2010-01-22 10:27:30Z atsareg $
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica    import Replica
from DIRAC                                                      import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: FileReplica.py 20219 2010-01-22 10:27:30Z atsareg $"

class FileReplica(Replica):
  
  def writeToXML(self):
    gLogger.info("Job Replica XML writing!!!")
    result = ''
    for param in self.getaprams():
      result += param.writeToXML()
    
    return result
    