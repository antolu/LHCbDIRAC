########################################################################
# $Id: InputFile.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores the input files
"""

from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Job.File                       import File

__RCSID__ = "$Id: InputFile.py 54098 2012-07-02 16:43:53Z zmathe $"

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