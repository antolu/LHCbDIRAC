"""
stores the input files
"""

from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Job.File                       import File

__RCSID__ = "$Id$"

#############################################################################
class InputFile(File):
  """
  InputFile class
  """
  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    result = '  <InputFile    Name="'+self.getFileName()+'"/>\n'
    return result