#######################################################################
# $Id: FileReplica.py,v 1.1 2008/07/01 10:54:26 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica    import Replica
from DIRAC                                                      import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: FileReplica.py,v 1.1 2008/07/01 10:54:26 zmathe Exp $"

class FileReplica(Replica):
  
  def writeToXML(self):
    gLogger.info("Job Replica XML writing!!!")
    result = ''
    for param in self.getaprams():
      result += param.writeToXML()
    
    return result
    