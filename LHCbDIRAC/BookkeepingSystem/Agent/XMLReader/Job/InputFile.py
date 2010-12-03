########################################################################
# $Id: InputFile.py 20219 2010-01-22 10:27:30Z atsareg $
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.File                       import File

__RCSID__ = "$Id: InputFile.py 20219 2010-01-22 10:27:30Z atsareg $"

#############################################################################  
class InputFile(File):
  
  #############################################################################  
  def writeToXML(self):
    result = '  <InputFile    Name="'+self.getFileName()+'"/>\n'
    return result