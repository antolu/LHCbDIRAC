########################################################################
# $Id$
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.File                       import File

__RCSID__ = "$Id$"

#############################################################################  
class InputFile(File):
  
  #############################################################################  
  def writeToXML(self):
    result = '  <InputFile    Name="'+self.getFileName()+'"/>\n'
    return result