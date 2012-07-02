#######################################################################
# $Id$
########################################################################

"""
stores the file parameters
"""

from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Replica.Replica    import Replica
from DIRAC                                                               import gLogger

__RCSID__ = "$Id$"

class FileReplica(Replica):
  """
  FileReplica class
  """
  def writeToXML(self):
    """creates an xml string"""
    gLogger.info("Job Replica XML writing!!!")
    result = ''
    for param in self.getaprams():
      result += param.writeToXML()

    return result

