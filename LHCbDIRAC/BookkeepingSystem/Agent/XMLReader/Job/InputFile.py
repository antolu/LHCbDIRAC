########################################################################
# $Id: InputFile.py,v 1.1 2008/07/01 10:54:26 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.File                       import File

__RCSID__ = "$Id: InputFile.py,v 1.1 2008/07/01 10:54:26 zmathe Exp $"

#############################################################################  
class InputFile(File):
  
  #############################################################################  
  def writeToXML(self):
    result = '  <InputFile    Name="'+self.getFileName()+'"/>\n'
    return result